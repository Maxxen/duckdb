//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/nearest_join_ranking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Optimizer;
class LogicalNearestJoin;

//! Replaces expressions above a LogicalNearestJoin that recompute its ranking with the ranking output of the join
class NearestJoinRanking {
public:
	explicit NearestJoinRanking(Optimizer &optimizer);

	void Optimize(LogicalOperator &op);

private:
	vector<reference<LogicalNearestJoin>> VisitOperator(LogicalOperator &op);
	void ReplaceRankings(unique_ptr<Expression> &expr, const vector<reference<LogicalNearestJoin>> &joins);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
