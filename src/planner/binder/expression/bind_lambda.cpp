#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

static void ExtractParameter(const ParsedExpression &expr, vector<Identifier> &column_names,
                             vector<string> &column_aliases) {
	auto &column_ref = expr.Cast<ColumnRefExpression>();
	if (column_ref.IsQualified()) {
		throw BinderException(LambdaExpression::InvalidParametersErrorMessage());
	}

	column_names.emplace_back(column_ref.GetName());
	column_aliases.push_back(column_ref.ToString());
}

static void ExtractParameters(LambdaExpression &expr, vector<Identifier> &column_names,
                              vector<string> &column_aliases) {
	// extract the lambda parameters, which are a single column
	// reference, or a list of column references (ROW function)
	string error_message;
	auto column_refs = expr.ExtractColumnRefExpressions(error_message);
	if (!error_message.empty()) {
		throw BinderException(error_message);
	}

	for (const auto &column_ref : column_refs) {
		ExtractParameter(column_ref.get(), column_names, column_aliases);
	}
	D_ASSERT(!column_names.empty());
}

static bool IsDoubleArrowRHS(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<FunctionExpression>();
	return func.IsOperator() && func.FunctionName() == "->>" && func.GetArguments().size() == 2;
}

static unique_ptr<ParsedExpression> RestructureArrowChain(LambdaExpression &expr) {
	auto &rhs_func = expr.RightMutable()->Cast<FunctionExpression>();
	auto inner_lambda = make_uniq<LambdaExpression>(
	    std::move(expr.LeftMutable()), std::move(rhs_func.GetArgumentsMutable()[0].GetExpressionMutable()));
	inner_lambda->GetLambdaSyntaxTypeMutable() = expr.GetLambdaSyntaxType();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(inner_lambda));
	children.push_back(std::move(rhs_func.GetArgumentsMutable()[1].GetExpressionMutable()));
	auto restructured = make_uniq<FunctionExpression>("->>", std::move(children));
	restructured->IsOperatorMutable() = true;
	return std::move(restructured);
}

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth) {
	// a lambda is bound by the function it is passed to - anywhere else, it is the JSON arrow operator
	if (expr.GetLambdaSyntaxType() == LambdaSyntaxType::LAMBDA_KEYWORD) {
		return BindResult("invalid lambda expression");
	}
	// The PEG parser produces A -> (B ->> C) where the standard parser left-associates to (A -> B) ->> C.
	// Restructure to match standard behavior before binding.
	if (IsDoubleArrowRHS(expr.Right())) {
		unique_ptr<ParsedExpression> restructured = RestructureArrowChain(expr);
		return BindExpression(restructured, depth);
	}

	// This is not a lambda expression, but the JSON arrow operator.
	// Remember the original expression in case of a binding error.
	if (!expr.CopiedExprMutable()) {
		expr.CopiedExprMutable() = expr.Right().Copy();
	}
	OperatorExpression arrow_expr(ExpressionType::ARROW, std::move(expr.LeftMutable()), std::move(expr.RightMutable()));
	auto bind_result = BindExpression(arrow_expr, depth);

	// The arrow_expr now might contain bound nodes.
	// Restore the original expression.
	if (bind_result.HasError()) {
		D_ASSERT(arrow_expr.GetChildrenMutable().size() == 2);
		expr.LeftMutable() = std::move(arrow_expr.GetChildrenMutable()[0]);
		expr.RightMutable() = std::move(arrow_expr.GetChildrenMutable()[1]);
	}
	return bind_result;
}

idx_t ExpressionBinder::GetLambdaParameterCount(LambdaExpression &expr) {
	vector<Identifier> column_names;
	vector<string> column_aliases;
	ExtractParameters(expr, column_names, column_aliases);
	return column_names.size();
}

BindResult ExpressionBinder::BindLambda(LambdaExpression &expr, idx_t depth, vector<LogicalType> column_types) {
	// extract and verify lambda parameters to create dummy columns
	vector<Identifier> column_names;
	vector<string> column_aliases;
	ExtractParameters(expr, column_names, column_aliases);
	if (column_names.size() != column_types.size()) {
		throw BinderException("The lambda expects %llu parameters, but it was given %llu", column_types.size(),
		                      column_names.size());
	}

	// base table alias
	auto table_alias = StringUtil::Join(column_aliases, ", ");
	if (column_aliases.size() > 1) {
		table_alias = "(" + table_alias + ")";
	}

	// create a lambda binding and push it to the lambda bindings vector
	vector<DummyBinding> local_bindings;
	if (!lambda_bindings) {
		lambda_bindings = &local_bindings;
	}
	DummyBinding new_lambda_binding(column_types, column_names, table_alias);
	lambda_bindings->push_back(new_lambda_binding);

	if (expr.CopiedExprMutable()) {
		expr.RightMutable() = std::move(expr.CopiedExprMutable());
	}
	auto result = BindExpression(expr.RightMutable(), depth, false);
	lambda_bindings->pop_back();

	// successfully bound a subtree of nested lambdas, set this to nullptr in case other parts of the
	// query also contain lambdas
	if (lambda_bindings->empty()) {
		lambda_bindings = nullptr;
	}

	if (result.HasError()) {
		result.error.Throw();
	}

	return BindResult(make_uniq<BoundLambdaExpression>(std::move(column_types), std::move(result.expression),
	                                                   std::move(column_names)));
}

void ExpressionBinder::TransformCapturedLambdaColumn(unique_ptr<Expression> &original,
                                                     unique_ptr<Expression> &replacement,
                                                     BoundLambdaExpression &bound_lambda_expr) {
	// a parameter of this lambda: the parameters come first in the input chunk of the body, in reverse order
	if (original->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA_REF) {
		auto &bound_lambda_ref = original->Cast<BoundLambdaRefExpression>();
		const bool is_own_parameter = !lambda_bindings || bound_lambda_ref.LambdaIndex() == lambda_bindings->size();
		if (is_own_parameter) {
			D_ASSERT(bound_lambda_expr.HasSignature());
			auto column_index = bound_lambda_ref.Binding().column_index;
			auto &logical_type = bound_lambda_expr.ParameterTypes()[column_index];
			auto index = bound_lambda_expr.ParameterCount() - column_index - 1;
			replacement = make_uniq<BoundReferenceExpression>(bound_lambda_ref.GetAlias(), logical_type, index);
			return;
		}
	}

	// anything else - including a parameter of an enclosing lambda - is captured: it is evaluated where the lambda
	// is passed, and follows the parameters in the input chunk of the body
	auto &captures = bound_lambda_expr.CapturesMutable();
	idx_t capture_idx = captures.size();
	for (idx_t i = 0; i < captures.size(); i++) {
		if (captures[i]->Equals(*original)) {
			capture_idx = i;
			break;
		}
	}
	auto index = bound_lambda_expr.ParameterCount() + capture_idx;
	replacement = make_uniq<BoundReferenceExpression>(original->GetAlias(), original->GetReturnType(), index);
	if (capture_idx == captures.size()) {
		captures.push_back(std::move(original));
	}
}

void ExpressionBinder::CaptureLambdaColumns(BoundLambdaExpression &bound_lambda_expr, unique_ptr<Expression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		throw BinderException("subqueries in lambda expressions are not supported");
	}

	// lambdas are bound depth-first, so an inner lambda has already captured its columns - those are its
	// children, which belong to the scope of this lambda and are visited below

	// we do not need to replace anything, as these will be constant in the lambda expression
	// when executed by the expression executor
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return;
	}

	// these expression classes do not have children, transform them
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->GetExpressionClass() == ExpressionClass::BOUND_PARAMETER ||
	    expr->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA_REF) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			// Search for UNNEST.
			auto &column_binding = expr->Cast<BoundColumnRefExpression>().Binding();
			ThrowIfUnnestInLambda(column_binding);
		}

		// move the expr because we are going to replace it
		auto original = std::move(expr);
		unique_ptr<Expression> replacement;

		TransformCapturedLambdaColumn(original, replacement, bound_lambda_expr);

		// replace the expression
		expr = std::move(replacement);

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<Expression> &child) { CaptureLambdaColumns(bound_lambda_expr, child); });
	}

	expr->Verify();
}

} // namespace duckdb
