#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include <algorithm>

namespace duckdb {

TableIndex GetMaxTableIndex(LogicalOperator &op) {
	TableIndex result(0);
	for (auto &child : op.children) {
		auto max_child_index = GetMaxTableIndex(*child);
		result = MaxValue<TableIndex>(result, max_child_index);
	}
	auto indexes = op.GetTableIndex();
	for (auto &index : indexes) {
		result = MaxValue<TableIndex>(result, index);
	}
	return result;
}

//! When deserializing a plan, if we realize that a function expression has changed its return type (i.e. our version
//! of the function in our local catalog returns something else than what the plan serialized as the return type) the
//! deserializer will insert a cast, wrapping the result of the function expression to the expected type. However this
//! breaks aggregate and window operators, which only expect aggregates and window expression, so such casts are 
//! hoisted into a projection above the operator
static void HoistResultCasts(Binder &binder, unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		HoistResultCasts(binder, root, child);
	}
	ExpressionClass result_class;
	TableIndex result_index;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		result_class = ExpressionClass::BOUND_AGGREGATE;
		result_index = op->Cast<LogicalAggregate>().aggregate_index;
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result_class = ExpressionClass::BOUND_WINDOW;
		result_index = op->Cast<LogicalWindow>().window_index;
		break;
	default:
		return;
	}

	// unwrap the casts, keeping the type each result is cast to
	vector<LogicalType> cast_types(op->expressions.size(), LogicalType::INVALID);
	bool has_cast = false;
	for (idx_t expr_idx = 0; expr_idx < op->expressions.size(); expr_idx++) {
		auto &expr = op->expressions[expr_idx];
		if (!BoundCastExpression::IsCast(*expr)) {
			continue;
		}
		auto &child = BoundCastExpression::ChildMutable(expr->Cast<BoundFunctionExpression>());
		if (child->GetExpressionClass() != result_class) {
			continue;
		}
		cast_types[expr_idx] = expr->GetReturnType();
		auto result = std::move(child);
		expr = std::move(result);
		has_cast = true;
	}
	if (!has_cast) {
		return;
	}

	// project every column of the operator, casting the unwrapped results
	op->ResolveOperatorTypes();
	const auto bindings = op->GetColumnBindings();
	const auto projection_index = binder.GenerateTableIndex();
	ColumnBindingReplacer replacer;
	vector<unique_ptr<Expression>> projections;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		auto &binding = bindings[col_idx];
		unique_ptr<Expression> column = make_uniq<BoundColumnRefExpression>(op->types[col_idx], binding);
		if (binding.table_index == result_index) {
			auto &cast_type = cast_types[binding.column_index.GetIndex()];
			if (cast_type.id() != LogicalTypeId::INVALID) {
				column = BoundCastExpression::AddCastToType(binder.context, std::move(column), cast_type);
			}
		}
		projections.push_back(std::move(column));
		replacer.replacement_bindings.emplace_back(binding, ColumnBinding(projection_index, ProjectionIndex(col_idx)));
	}
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projections));
	if (op->has_estimated_cardinality) {
		projection->SetEstimatedCardinality(op->estimated_cardinality);
	}
	projection->children.push_back(std::move(op));
	projection->ResolveOperatorTypes();
	op = std::move(projection);

	// the operators above read the projection instead
	replacer.stop_operator = op.get();
	replacer.VisitOperator(*root);
}

BoundStatement Binder::Bind(LogicalPlanStatement &stmt) {
	BoundStatement result;
	result.types = stmt.plan->types;
	for (idx_t i = 0; i < result.types.size(); i++) {
		result.names.emplace_back(StringUtil::Format("col%d", i));
	}
	result.plan = std::move(stmt.plan);

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::QUERY_RESULT; // TODO could also be something else

	if (parent) {
		throw InternalException("LogicalPlanStatement should be bound in root binder");
	}
	global_binder_state->bound_tables = GetMaxTableIndex(*result.plan).index + 1;
	HoistResultCasts(*this, result.plan, result.plan);
	return result;
}

} // namespace duckdb
