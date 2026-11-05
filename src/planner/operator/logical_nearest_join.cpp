#include "duckdb/planner/operator/logical_nearest_join.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

LogicalNearestJoin::LogicalNearestJoin(JoinType join_type)
    : LogicalJoin(join_type, LogicalOperatorType::LOGICAL_NEAREST_JOIN) {
}

LogicalNearestJoin::LogicalNearestJoin(JoinType join_type, unique_ptr<Expression> ranking, idx_t nearest_count,
                                       OrderType order_type, bool approx)
    : LogicalJoin(join_type, LogicalOperatorType::LOGICAL_NEAREST_JOIN), nearest_count(nearest_count),
      order_type(order_type), approx(approx) {
	D_ASSERT(join_type == JoinType::INNER || join_type == JoinType::LEFT);
	expressions.push_back(std::move(ranking));
}

const Expression &LogicalNearestJoin::GetRanking() const {
	D_ASSERT(expressions.size() == 1);
	return *expressions[0];
}

unique_ptr<Expression> &LogicalNearestJoin::GetRankingMutable() {
	D_ASSERT(expressions.size() == 1);
	return expressions[0];
}

InsertionOrderPreservingMap<string> LogicalNearestJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);
	result["Ranking"] = StringUtil::Format(
	    "%s %s", order_type == OrderType::DESCENDING ? "BY SIMILARITY" : "BY DISTANCE", GetRanking().ToString());
	result["Count"] = to_string(nearest_count);
	result["Mode"] = approx ? "APPROX" : "EXACT";
	SetParamsEstimatedCardinality(result);
	return result;
}

optional_ptr<BoundFunctionExpression> LogicalNearestJoin::TrySplitRanking(idx_t &left_argument, idx_t &right_argument) {
	auto &ranking = *GetRankingMutable();
	if (ranking.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function = ranking.Cast<BoundFunctionExpression>();
	unordered_set<TableIndex> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*children[0], left_bindings);
	LogicalJoin::GetTableReferences(*children[1], right_bindings);
	optional_idx left_index, right_index;
	auto &arguments = function.GetChildren();
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto side = JoinSide::GetJoinSide(*arguments[i], left_bindings, right_bindings);
		if (side == JoinSide::LEFT && !left_index.IsValid()) {
			left_index = i;
		} else if (side == JoinSide::RIGHT && !right_index.IsValid()) {
			right_index = i;
		} else if (side != JoinSide::NONE || !arguments[i]->IsFoldable()) {
			return nullptr;
		}
	}
	if (!left_index.IsValid() || !right_index.IsValid()) {
		return nullptr;
	}
	left_argument = left_index.GetIndex();
	right_argument = right_index.GetIndex();
	return function;
}

idx_t LogicalNearestJoin::GetCardinality(idx_t left_cardinality, idx_t right_cardinality) const {
	auto matches = MinValue<idx_t>(nearest_count, right_cardinality);
	if (join_type == JoinType::LEFT) {
		matches = MaxValue<idx_t>(matches, 1);
	}
	idx_t result;
	if (!TryMultiplyOperator::Operation(left_cardinality, matches, result)) {
		return NumericLimits<idx_t>::Maximum();
	}
	return result;
}

idx_t LogicalNearestJoin::EstimateCardinality(ClientContext &context) {
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	auto left_cardinality = children[0]->EstimateCardinality(context);
	auto right_cardinality = children[1]->EstimateCardinality(context);
	has_estimated_cardinality = true;
	estimated_cardinality = GetCardinality(left_cardinality, right_cardinality);
	return estimated_cardinality;
}

} // namespace duckdb
