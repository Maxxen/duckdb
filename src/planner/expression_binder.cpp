#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/scope_chain.hpp"
#include "duckdb/planner/scope_resolver.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void ExpressionBinder::SetCatalogLookupCallback(catalog_entry_callback_t callback) {
	binder.SetCatalogLookupCallback(std::move(callback));
}

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context) : binder(binder), context(context) {
	InitializeStackCheck();
}

ExpressionBinder::~ExpressionBinder() {
}

BoundExpressionMap &ExpressionBinder::GetBoundExpressions() const {
	return binder.GetBoundExpressions();
}

void ExpressionBinder::InitializeStackCheck() {
	static constexpr idx_t INITIAL_DEPTH = 5;
	if (binder.HasActiveBinder()) {
		stack_depth = binder.GetActiveBinder().stack_depth + INITIAL_DEPTH;
	} else {
		stack_depth = INITIAL_DEPTH;
	}
}

StackChecker<ExpressionBinder> ExpressionBinder::StackCheck(const ParsedExpression &expr, idx_t extra_stack) {
	D_ASSERT(stack_depth != DConstants::INVALID_INDEX);
	auto max_expression_depth = Settings::Get<MaxExpressionDepthSetting>(context);
	if (stack_depth + extra_stack >= max_expression_depth) {
		throw BinderException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      max_expression_depth);
	}
	return StackChecker<ExpressionBinder>(*this, extra_stack);
}

BindResult ExpressionBinder::BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	auto stack_checker = StackCheck(*expr);

	auto &expr_ref = *expr;
	switch (expr_ref.GetExpressionClass()) {
	case ExpressionClass::BETWEEN:
		return BindExpression(expr_ref.Cast<BetweenExpression>(), depth);
	case ExpressionClass::CASE:
		return BindExpression(expr_ref.Cast<CaseExpression>(), depth);
	case ExpressionClass::CAST:
		return BindExpression(expr_ref.Cast<CastExpression>(), depth);
	case ExpressionClass::COLLATE:
		return BindExpression(expr_ref.Cast<CollateExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression(expr_ref.Cast<ColumnRefExpression>(), depth, root_expression, expr);
	case ExpressionClass::LAMBDA_REF:
		return BindExpression(expr_ref.Cast<LambdaRefExpression>(), depth);
	case ExpressionClass::COMPARISON:
		return BindExpression(expr_ref.Cast<ComparisonExpression>(), depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression(expr_ref.Cast<ConjunctionExpression>(), depth);
	case ExpressionClass::CONSTANT:
		return BindExpression(expr_ref.Cast<ConstantExpression>(), depth);
	case ExpressionClass::TYPE:
		return BindExpression(expr_ref.Cast<TypeExpression>(), depth);
	case ExpressionClass::FUNCTION: {
		auto &function = expr_ref.Cast<FunctionExpression>();
		if (IsUnnestFunction(function.FunctionName())) {
			// special case, not in catalog
			return BindUnnest(function, depth, root_expression);
		}
		// binding a function expression requires an extra parameter for macros
		return BindExpression(function, depth, expr);
	}
	case ExpressionClass::LAMBDA: {
		const vector<LogicalType> function_child_types;
		return BindExpression(expr_ref.Cast<LambdaExpression>(), depth, function_child_types, nullptr, nullptr);
	}
	case ExpressionClass::OPERATOR:
		return BindExpression(expr_ref.Cast<OperatorExpression>(), depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression(expr_ref.Cast<SubqueryExpression>(), depth);
	case ExpressionClass::PARAMETER:
		return BindExpression(expr_ref.Cast<ParameterExpression>(), depth);
	case ExpressionClass::POSITIONAL_REFERENCE:
		return BindPositionalReference(expr, depth, root_expression);
	case ExpressionClass::STAR:
		return BindResult(BinderException::Unsupported(expr_ref, "STAR expression is not supported here"));
	default:
		return BindResult(
		    NotImplementedException("Unimplemented expression class in ExpressionBinder::BindExpression: %s",
		                            EnumUtil::ToString(expr_ref.GetExpressionClass())));
	}
}

#ifdef DEBUG
//! Collect the column references of the expression that have not been bound by an earlier attempt.
//! Subqueries are skipped: they are bound by a binder of their own and resolve in their own chain.
static void CollectUnboundColumns(ParsedExpression &expr, const BoundExpressionMap &bound_expressions,
                                  vector<reference<ColumnRefExpression>> &result) {
	if (bound_expressions.IsBound(expr)) {
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		result.push_back(expr.Cast<ColumnRefExpression>());
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { CollectUnboundColumns(child, bound_expressions, result); });
}

//! A lower bound on the depth the retry loop settles on: a column that resolves at depth k is reached
//! by the k'th attempt. It is only a lower bound because a scope can resolve a name and still fail to
//! bind it - "s1.s2" qualifies as a struct extract of a column named s1, and only once that fails to
//! bind does the next scope get to read s1 as a table alias.
static optional_idx PredictSettledDepth(const ScopeChain &chain, ParsedExpression &expr,
                                        const BoundExpressionMap &bound_expressions) {
	vector<reference<ColumnRefExpression>> unbound;
	CollectUnboundColumns(expr, bound_expressions, unbound);
	if (unbound.empty()) {
		return optional_idx();
	}
	idx_t predicted = 0;
	for (auto &colref : unbound) {
		auto resolution = ScopeResolver::ResolveColumn(chain, colref.get(), 0);
		if (!resolution.found) {
			// the column resolves through something other than a scope in the chain (an alias, a lambda
			// parameter, a value function): the loop's outcome is not predicted by column resolution alone
			return optional_idx();
		}
		predicted = MaxValue(predicted, resolution.depth);
	}
	return predicted;
}
#endif

BindResult ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr, ErrorData error_message) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto chain = ScopeChain::FromBinder(*this);
#ifdef DEBUG
	auto predicted_depth = PredictSettledDepth(chain, *expr, binder.GetBoundExpressions());
#endif
	// make a copy of the enclosing scopes, so we can restore them later
	auto saved_scopes = binder.GetEnclosingScopes();
	auto bind_error = std::move(error_message);
	idx_t settled_depth = 0;
	// walk outward: the index within the chain is the depth the expression binds at
	for (idx_t depth = 1; depth < chain.Size(); depth++) {
		auto &next_binder = chain.At(depth);
		ExpressionBinder::QualifyColumnNames(next_binder.binder, expr);
		auto next_error = next_binder.Bind(expr, depth);
		if (!next_error.HasError()) {
			bind_error = std::move(next_error);
			settled_depth = depth;
			break;
		}
		ScopeResolver::CombineErrors(bind_error, std::move(next_error));
		// the scope we just tried is no longer reachable while we look further outward
		binder.PopScope();
	}
	binder.SetScopes(std::move(saved_scopes));
#ifdef DEBUG
	// the resolver must never reach a name in a scope inside the one the loop arrives at by trial:
	// that would capture the reference in the wrong scope
	if (!bind_error.HasError() && predicted_depth.IsValid()) {
		D_ASSERT(predicted_depth.GetIndex() <= settled_depth);
	}
#endif
	return BindResult(bind_error);
}

void ExpressionBinder::BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, ErrorData &error) {
	if (expr) {
		ErrorData bind_error = Bind(expr, depth);
		if (!error.HasError()) {
			error = std::move(bind_error);
		}
	}
}

void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.Depth() > 0) {
			binder.AddCorrelatedColumn(CorrelatedColumnInfo(bound_colref));
		}
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ExtractCorrelatedExpressions(binder, child); });
}

bool ExpressionBinder::ContainsType(const LogicalType &type, LogicalTypeId target) {
	if (type.id() == target) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		auto child_count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < child_count; i++) {
			if (ContainsType(StructType::GetChildType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < member_count; i++) {
			if (ContainsType(UnionType::GetMemberType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return ContainsType(ListType::GetChildType(type), target);
	case LogicalTypeId::ARRAY:
		return ContainsType(ArrayType::GetChildType(type), target);
	default:
		return false;
	}
}

LogicalType ExpressionBinder::ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type) {
	if (type.id() == target) {
		return new_type;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		// we make a copy of the child types of the struct here
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			child_type.second = ExchangeType(child_type.second, target, new_type);
		}
		return type.id() == LogicalTypeId::TUPLE ? LogicalType::TUPLE(std::move(child_types))
		                                         : LogicalType::STRUCT(std::move(child_types));
	}
	case LogicalTypeId::UNION: {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			member_type.second = ExchangeType(member_type.second, target, new_type);
		}
		return LogicalType::UNION(std::move(member_types));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ExchangeType(ListType::GetChildType(type), target, new_type));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(ExchangeType(ListType::GetChildType(type), target, new_type));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(ExchangeType(ArrayType::GetChildType(type), target, new_type),
		                          ArrayType::GetSize(type));
	default:
		return type;
	}
}

bool ExpressionBinder::ContainsNullType(const LogicalType &type) {
	return ContainsType(type, LogicalTypeId::SQLNULL);
}

LogicalType ExpressionBinder::ExchangeNullType(const LogicalType &type) {
	return ExchangeType(type, LogicalTypeId::SQLNULL, LogicalType::INTEGER);
}

unique_ptr<Expression> ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, optional_ptr<LogicalType> result_type,
                                              bool root_expression) {
	// open a scope for this bind cycle: entries left behind by failed bind attempts are erased on exit
	BoundExpressionScope scope(GetBoundExpressions());
	// bind the main expression
	auto error_msg = Bind(expr, 0, root_expression);
	if (error_msg.HasError()) {
		// Try binding the correlated column. If binding the correlated column
		// has error messages, those should be propagated up. So for the test case
		// having subquery failed to bind:14 the real error message should be something like
		// aggregate with constant input must be bound to a root node.
		auto result = BindCorrelatedColumns(expr, error_msg);
		if (result.HasError()) {
			ScopeResolver::CombineErrors(error_msg, std::move(result.error));
			error_msg.Throw();
		}
		ExtractCorrelatedExpressions(binder, GetBoundExpressions().Get(*expr));
	}
	unique_ptr<Expression> result = GetBoundExpressions().Consume(*expr);
	if (target_type.id() != LogicalTypeId::INVALID) {
		// the binder has a specific target type: add a cast to that type
		result = BoundCastExpression::AddCastToType(context, std::move(result), target_type);
	} else {
		if (!binder.CanContainNulls()) {
			// SQL NULL type is only used internally in the binder
			// cast to INTEGER if we encounter it outside of the binder
			if (ContainsNullType(result->GetReturnType())) {
				auto exchanged_type = ExchangeNullType(result->GetReturnType());
				result = BoundCastExpression::AddCastToType(context, std::move(result), exchanged_type);
			}
		}
		if (result->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	if (result_type) {
		*result_type = result->GetReturnType();
	}
	return result;
}

ErrorData ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto query_location = expr->GetQueryLocation();
	auto &expression = *expr;
	auto alias = expression.GetAlias();
	if (GetBoundExpressions().IsBound(expression)) {
		// already bound, don't bind it again
		return ErrorData();
	}
	if (expression.GetExpressionClass() == ExpressionClass::WINDOW) {
		auto &w = expression.Cast<WindowExpression>();
		if (WindowHasBoundedParts(w)) {
			BindResult result =
			    BindResult(BinderException::Unsupported(*expr, "window expression is not supported here"));
			if (result.HasError()) {
				return std::move(result.error);
			}
		}
	}
	// bind the expression
	BindResult result = BindExpression(expr, depth, root_expression);
	if (result.HasError()) {
		return std::move(result.error);
	}
	// successfully bound: store the bound expression in the map
	result.expression->SetQueryLocation(query_location);
	if (!alias.empty()) {
		result.expression->SetAlias(alias);
	}
	GetBoundExpressions().Insert(*expr, std::move(result.expression));
	return ErrorData();
}

bool ExpressionBinder::WindowHasBoundedParts(const WindowExpression &window) const {
	auto &bound_expressions = GetBoundExpressions();
	if (bound_expressions.Empty()) {
		return false;
	}
	for (auto &child : window.GetArguments()) {
		if (bound_expressions.IsBound(child.GetExpression())) {
			return true;
		}
	}
	for (auto &child : window.Partitions()) {
		if (bound_expressions.IsBound(*child)) {
			return true;
		}
	}
	for (auto &order : window.OrderBy()) {
		if (bound_expressions.IsBound(*order.expression)) {
			return true;
		}
	}
	for (auto &order : window.ArgOrders()) {
		if (bound_expressions.IsBound(*order.expression)) {
			return true;
		}
	}
	return false;
}

BindResult ExpressionBinder::BindUnsupportedExpression(ParsedExpression &expr, idx_t depth, const string &message) {
	// we always prefer to throw an error if it occurs in a child expression
	// since that error might be more descriptive
	// bind all children
	ErrorData result;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { BindChild(child, depth, result); });
	if (result.HasError()) {
		return BindResult(std::move(result));
	}
	return BindResult(BinderException::Unsupported(expr, message));
}

bool ExpressionBinder::IsUnnestFunction(const Identifier &function_name) {
	return function_name == "unnest" || function_name == "unlist";
}

bool ExpressionBinder::IsPotentialAlias(const ColumnRefExpression &colref) {
	// traditional alias (unqualified), or qualified with table name "alias"
	if (!colref.IsQualified()) {
		return true;
	}
	if (colref.ColumnNames().size() == 2) {
		return colref.ColumnNames()[0] == "alias";
	}
	return false;
}

} // namespace duckdb
