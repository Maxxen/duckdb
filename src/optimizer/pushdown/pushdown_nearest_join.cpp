#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_nearest_join.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownNearestJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_NEAREST_JOIN);
	unordered_set<TableIndex> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	// every left row is ranked independently, so filters on the left side can be pushed down
	// filters on the right side change which rows rank first, so they have to stay above the join
	FilterPushdown left_pushdown(optimizer, convert_mark_joins, projection_mode);
	for (idx_t i = 0; i < filters.size(); i++) {
		if (JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings) == JoinSide::LEFT) {
			left_pushdown.filters.push_back(std::move(filters[i]));
			filters.erase_at(i);
			i--;
		}
	}
	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	FilterPushdown right_pushdown(optimizer, convert_mark_joins, projection_mode);
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
