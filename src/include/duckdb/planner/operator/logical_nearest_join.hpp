//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_nearest_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

class BoundFunctionExpression;

//! LogicalNearestJoin joins every row of the left child with the `nearest_count` rows of the right child that rank
//! first according to the ranking expression. Right rows whose ranking is NULL never match, unless
//! include_null_rankings is set. For a LEFT join, left rows without any match are emitted once with NULLs for the
//! right columns. Ties are broken arbitrarily.
//! Extensions can replace this operator with a specialized implementation (e.g. an index-based KNN join), otherwise
//! it is planned as a brute-force top-k per left row.
class LogicalNearestJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_NEAREST_JOIN;

public:
	explicit LogicalNearestJoin(JoinType join_type);
	LogicalNearestJoin(JoinType join_type, unique_ptr<Expression> ranking, idx_t nearest_count, OrderType order_type,
	                   bool approx);

	//! How many rows of the right child to return per left row
	idx_t nearest_count = 1;
	//! ASCENDING returns the lowest ranking rows (BY DISTANCE), DESCENDING the highest ones (BY SIMILARITY)
	OrderType order_type = OrderType::ASCENDING;
	//! Whether approximate results are acceptable
	bool approx = false;
	//! Whether right rows with a NULL ranking can still match, ranked after all other rows. Set for lateral top-k
	//! subqueries that do not filter out NULL rankings, cleared once the ranking is known to never be NULL
	bool include_null_rankings = false;
	//! The table index of the ranking output column (after the right columns), invalid if the ranking is not output.
	//! The ranking output is NULL for left rows without a match.
	TableIndex ranking_index;

public:
	//! The ranking expression, stored in expressions[0], referencing columns of both children
	const Expression &GetRanking() const;
	unique_ptr<Expression> &GetRankingMutable();

	//! If the ranking is a function call with exactly one argument referencing the left child, one argument
	//! referencing the right child and only constant other arguments, returns it and the indexes of those arguments.
	//! Works both before and after the column bindings have been resolved.
	optional_ptr<BoundFunctionExpression> TrySplitRanking(idx_t &left_argument, idx_t &right_argument);

	bool OutputsRanking() const {
		return ranking_index.IsValid();
	}
	ColumnBinding GetRankingBinding() const;

	//! The output cardinality for the given child cardinalities, saturating on overflow
	idx_t GetCardinality(idx_t left_cardinality, idx_t right_cardinality) const;

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	idx_t EstimateCardinality(ClientContext &context) override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
