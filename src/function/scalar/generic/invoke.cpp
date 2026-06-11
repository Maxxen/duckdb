#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

struct LambdaInvokeData final : public LambdaFunctionData {
	unique_ptr<Expression> lambda_expr;

	explicit LambdaInvokeData(unique_ptr<Expression> lambda_expr_p) : lambda_expr(std::move(lambda_expr_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto lambda_expr_copy = lambda_expr ? lambda_expr->Copy() : nullptr;
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr_copy));
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LambdaInvokeData>();
		return Expression::Equals(lambda_expr, other.lambda_expr);
	}

	//! Serializes a lambda function's bind data
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const BoundScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<LambdaInvokeData>();
		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	}

	//! Deserializes a lambda function's bind data
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, BoundScalarFunction &) {
		auto lambda_expr = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Expression>>(
		    101, "lambda_expr", unique_ptr<Expression>());
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr));
	}

	optional_ptr<const Expression> GetLambdaExpression() const override {
		if (!lambda_expr) {
			return nullptr;
		}
		auto &bound_lambda_expr = lambda_expr->Cast<BoundLambdaExpression>();
		return bound_lambda_expr.LambdaExpr().get();
	}
};

struct LambdaInvokeState final : public FunctionLocalState {
	unique_ptr<ExpressionExecutor> executor;
	DataChunk input_chunk;
	idx_t parameter_count;

	LambdaInvokeState(unique_ptr<ExpressionExecutor> executor_p, const vector<LogicalType> &input_types,
	                  const idx_t parameter_count_p)
	    : executor(std::move(executor_p)), parameter_count(parameter_count_p) {
		input_chunk.InitializeEmpty(input_types);
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		auto &bdata = bind_data->Cast<LambdaInvokeData>();
		if (!bdata.lambda_expr) {
			throw InternalException("Invoke function is missing its bound lambda expression");
		}
		auto &bound_lambda_expr = bdata.lambda_expr->Cast<BoundLambdaExpression>();
		const auto parameter_count = bound_lambda_expr.ParameterCount();
		D_ASSERT(parameter_count <= expr.GetChildren().size());

		vector<LogicalType> input_types;
		input_types.reserve(expr.GetChildren().size());
		for (idx_t i = 0; i < parameter_count; i++) {
			input_types.push_back(expr.GetChildren()[parameter_count - i - 1]->GetReturnType());
		}
		for (idx_t i = parameter_count; i < expr.GetChildren().size(); i++) {
			input_types.push_back(expr.GetChildren()[i]->GetReturnType());
		}

		auto executor = make_uniq<ExpressionExecutor>(state.GetContext(), *bound_lambda_expr.LambdaExpr());
		return make_uniq<LambdaInvokeState>(std::move(executor), input_types, parameter_count);
	}
};

void LambdaInvokeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LambdaInvokeState>();
	for (idx_t i = 0; i < lstate.parameter_count; i++) {
		lstate.input_chunk.data[i].Reference(args.data[lstate.parameter_count - i - 1]);
	}
	for (idx_t i = lstate.parameter_count; i < args.ColumnCount(); i++) {
		lstate.input_chunk.data[i].Reference(args.data[i]);
	}
	lstate.input_chunk.SetChildCardinality(args.size());
	lstate.executor->ExecuteExpression(lstate.input_chunk, result);
}

unique_ptr<FunctionData> LambdaInvokeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	// the list column and the bound lambda expression
	if (arguments[0]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression passed to 'invoke' function.");
	}

	auto &bound_lambda_expr = arguments[0]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.ParameterCount() != arguments.size() - 1) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'invoke' function, expected %d, got %d.",
		                      bound_lambda_expr.ParameterCount(), arguments.size() - 1);
	}

	bound_function.SetReturnType(bound_lambda_expr.LambdaExpr()->GetReturnType());

	return make_uniq<LambdaInvokeData>(bound_lambda_expr.Copy());
}

LogicalType LambdaInvokeBindParameters(ClientContext &context, const vector<LogicalType> &function_child_types,
                                       const idx_t parameter_idx, optional_ptr<BindLambdaContext> bind_lambda_context) {
	// The first parameter is always the lambda
	auto child_idx = parameter_idx + 1;
	if (child_idx >= function_child_types.size()) {
		throw BinderException("The number of lambda parameters does not match the number of arguments passed to the "
		                      "'invoke' function, expected at least %d, got %d.",
		                      parameter_idx + 1, function_child_types.size() - 1);
	}
	return function_child_types[child_idx];
}

//! Returns true if all input references (lambda parameters and captures) in the lambda body are within range
bool LambdaReferencesInRange(const Expression &expr, const idx_t input_count) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		return expr.Cast<BoundReferenceExpression>().Index() < input_count;
	}
	bool in_range = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { in_range = in_range && LambdaReferencesInRange(child, input_count); });
	return in_range;
}

//! Pushes all outer column references in the expression down into the subquery, by incrementing their depth and
//! registering them as correlated columns of the subquery binder
void MarkCorrelatedColumns(Binder &subquery_binder, Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		colref.DepthMutable()++;
		subquery_binder.AddCorrelatedColumn(CorrelatedColumnInfo(colref));
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { MarkCorrelatedColumns(subquery_binder, *child); });
}

//! Replaces the lambda input references in the body with column references into the CTE projection
void ReplaceLambdaReferences(unique_ptr<Expression> &expr, const TableIndex cte_index) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr->Cast<BoundReferenceExpression>();
		expr = make_uniq<BoundColumnRefExpression>(ref.GetAlias(), ref.GetReturnType(),
		                                           ColumnBinding(cte_index, ProjectionIndex(ref.Index())));
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceLambdaReferences(child, cte_index); });
}

unique_ptr<Expression> LambdaInvokeExpr(FunctionBindExpressionInput &input) {
	// Rewrite "invoke(lambda, arg_0, ..., arg_n)" into the scalar subquery
	//   (WITH cte(p_0, ..., p_n) AS (SELECT arg_0, ..., arg_n) SELECT <lambda body> FROM cte)
	// i.e. a projection of the lambda body over a projection that evaluates every argument (and capture) exactly
	// once. This exposes the lambda body to the optimizer instead of executing it through a nested expression
	// executor. Whenever we cannot rewrite we return nullptr to fall back to the regular execution path.
	if (!input.binder) {
		// no binder available (e.g. when re-binding during deserialization)
		return nullptr;
	}
	if (input.inside_lambda) {
		// subqueries cannot be executed inside the body of an enclosing lambda
		return nullptr;
	}
	auto &children = input.children;
	if (children.empty() || children[0]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		return nullptr;
	}

	auto &bound_lambda = children[0]->Cast<BoundLambdaExpression>();
	auto &lambda_body = bound_lambda.LambdaExprMutable();
	auto &captures = bound_lambda.CapturesMutable();
	const auto param_count = bound_lambda.ParameterCount();
	D_ASSERT(children.size() == param_count + 1);

	for (idx_t i = 1; i < children.size(); i++) {
		if (children[i]->HasSubquery()) {
			// we cannot embed an unplanned subquery into the plan we build here
			return nullptr;
		}
		if (children[i]->IsVolatile()) {
			// a volatile argument would be evaluated once per query in an uncorrelated subquery,
			// instead of once per row
			return nullptr;
		}
	}
	if (lambda_body->IsVolatile()) {
		return nullptr;
	}

	// the lambda body references its inputs by index: the parameters (in reverse argument order) followed by the
	// captures - out-of-range references belong to an enclosing lambda
	const idx_t input_count = param_count + captures.size();
	if (!LambdaReferencesInRange(*lambda_body, input_count)) {
		return nullptr;
	}

	// build the projection list of the CTE so that column i lines up with input reference i of the lambda body
	vector<unique_ptr<Expression>> cte_exprs;
	cte_exprs.reserve(input_count);
	for (idx_t i = 0; i < param_count; i++) {
		cte_exprs.push_back(std::move(children[param_count - i]));
	}
	for (auto &capture : captures) {
		cte_exprs.push_back(std::move(capture));
	}
	captures.clear();

	// create the binder for the subquery, and turn all outer column references in the argument expressions into
	// correlated columns of the subquery
	auto subquery_binder = Binder::CreateBinder(input.context, input.binder);
	subquery_binder->SetCanContainNulls(true);
	subquery_binder->SetInsideSubquery();
	for (auto &cte_expr : cte_exprs) {
		MarkCorrelatedColumns(*subquery_binder, *cte_expr);
	}

	// the CTE: a projection evaluating the arguments and captures over a single-row dummy scan
	auto cte_index = subquery_binder->GenerateTableIndex();
	auto cte_projection = make_uniq<LogicalProjection>(cte_index, std::move(cte_exprs));
	cte_projection->AddChild(make_uniq<LogicalDummyScan>(subquery_binder->GenerateTableIndex()));

	// the body of the subquery: the lambda expression with its input references rewritten to refer to the CTE
	auto body = std::move(lambda_body);
	ReplaceLambdaReferences(body, cte_index);
	auto return_type = body->GetReturnType();

	vector<unique_ptr<Expression>> select_list;
	select_list.push_back(std::move(body));
	auto projection = make_uniq<LogicalProjection>(subquery_binder->GenerateTableIndex(), std::move(select_list));
	projection->AddChild(std::move(cte_projection));

	auto result = make_uniq<BoundSubqueryExpression>(return_type);
	result->SetAlias("invoke");
	result->SubqueryTypeMutable() = SubqueryType::SCALAR;
	result->SubqueryMutable().plan = std::move(projection);
	result->SubqueryMutable().types.push_back(std::move(return_type));
	result->SubqueryMutable().names.emplace_back("invoke");
	result->GetBinderMutable() = std::move(subquery_binder);
	return std::move(result);
}

} // namespace

ScalarFunction InvokeFun::GetFunction() {
	ScalarFunction fun("invoke", {LogicalType::LAMBDA, LogicalType::ANY}, LogicalType::ANY, LambdaInvokeFunction);
	fun.SetBindExpressionCallback(LambdaInvokeExpr);
	fun.SetBindCallback(LambdaInvokeBind);
	fun.SetVarArgs(LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindLambdaCallback(LambdaInvokeBindParameters);
	fun.SetInitStateCallback(LambdaInvokeState::Init);
	fun.SetSerializeCallback(LambdaInvokeData::Serialize);
	fun.SetDeserializeCallback(LambdaInvokeData::Deserialize);
	return fun;
}

} // namespace duckdb
