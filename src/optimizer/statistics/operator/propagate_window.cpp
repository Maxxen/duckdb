#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalWindow &window,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(window.children[0]);

	// then propagate to each of the order expressions
	for (auto &window_expr : window.expressions) {
		auto &over_expr = window_expr->Cast<BoundWindowExpression>();
		vector<unique_ptr<BaseStatistics>> partitions_stats;
		for (auto &expr : over_expr.PartitionsMutable()) {
			partitions_stats.push_back(PropagateExpression(expr));
		}
		over_expr.PartitionsStatsMutable() = std::move(partitions_stats);
		for (auto &bound_order : over_expr.OrderByMutable()) {
			bound_order.stats = PropagateExpression(bound_order.expression);
		}

		vector<unique_ptr<BaseStatistics>> expr_stats;
		if (over_expr.StartExpr()) {
			expr_stats.push_back(PropagateExpression(over_expr.StartExprMutable()));
		} else {
			expr_stats.push_back(nullptr);
		}

		if (over_expr.EndExpr()) {
			expr_stats.push_back(PropagateExpression(over_expr.EndExprMutable()));
		} else {
			expr_stats.push_back(nullptr);
		}
		over_expr.ExprStatsMutable() = std::move(expr_stats);

		for (auto &bound_order : over_expr.ArgOrdersMutable()) {
			bound_order.stats = PropagateExpression(bound_order.expression);
		}
	}
	return std::move(node_stats);
}

} // namespace duckdb
