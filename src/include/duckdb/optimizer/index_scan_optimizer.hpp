//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/index_scan_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class Optimizer;
class LogicalOperator;
class LogicalGet;
class ClientContext;
class Expression;
class BoundIndex;
class LogicalFilter;
struct IndexMatch;

//! Generic, index-type-agnostic optimizer for index-based scans. Owns the plan-shape matching,
//! index enumeration, rewrite, and residual-filter handling that each index extension used to
//! re-implement. Index types only implement the BoundIndex::TryBind*/InitializeScan/Scan callbacks.
class IndexScanOptimizer {
public:
	explicit IndexScanOptimizer(Optimizer &optimizer);

	//! Rewrite predicate-based scans: a LOGICAL_FILTER over a seq_scan LogicalGet, or a seq_scan
	//! LogicalGet carrying pushed-down expression filters, into a generic index scan.
	void OptimizePredicateScan(unique_ptr<LogicalOperator> &plan);

	//! Rewrite ORDER BY f(col, const) LIMIT k (a LogicalTopN over a projection over a seq_scan get)
	//! into an ordered generic index scan, deleting the LogicalTopN when the index answer is exact.
	void OptimizeOrderLimitScan(unique_ptr<LogicalOperator> &plan);

	//! Rewrite a lateral per-outer-row KNN (planned as a delim-join around a row_number() window
	//! over a cross product) into a core LogicalIndexJoin driven by an index probe (e.g. HNSW).
	void OptimizeIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);

	//! Rewrite equi-joins with an indexed inner into index nested-loop joins (ART). Runs earlier
	//! than OptimizeIndexJoin (before join-filter pushdown) so the comparison join is still clean.
	void OptimizeEquiIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);

private:
	bool TryOptimizePredicateScan(unique_ptr<LogicalOperator> &plan);
	bool TryOptimizeGet(unique_ptr<LogicalOperator> &get_ptr, optional_ptr<LogicalFilter> filter,
	                    const Expression &predicate);
	//! Enumerate the table's indexes and ask each capable one whether it can serve `predicate`.
	bool TryBindPredicate(LogicalGet &get, const Expression &predicate, optional_ptr<BoundIndex> &out_index,
	                      unique_ptr<IndexMatch> &out_match);
	//! Swap the get to a generic index scan bound to `index`/`match`, pulling residual table filters up.
	void RewriteToIndexScan(unique_ptr<LogicalOperator> &get_ptr, optional_ptr<LogicalFilter> filter,
	                        BoundIndex &index, IndexMatch &match);

	void OptimizeOrderLimitRecursive(unique_ptr<LogicalOperator> &plan);
	bool TryOptimizeOrderLimitScan(unique_ptr<LogicalOperator> &plan);

	bool TryOptimizeIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);
	//! Rewrite an equi-join whose inner side is a seq_scan on a table with a matching index into an
	//! index nested-loop join (probe the index per outer key). Cost-gated: only when the outer side
	//! is much smaller than the indexed inner, so it does not regress hash joins.
	bool TryOptimizeIndexEquiJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);
	bool TryBindEquiJoin(LogicalGet &inner_get, const Expression &inner_key, const Expression &outer_key,
	                     optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match);
	//! Rewrite a range/band inner join (inner_col >/>=/</<= / BETWEEN outer) into an index nested-loop
	//! join that probes the ordered index per outer row. Beats piecewise-merge / IEJoin when the outer
	//! is small and the per-row range is selective. Runs with the equi pass.
	bool TryOptimizeIndexRangeJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);
	bool TryBindRangeJoin(LogicalGet &inner_get, const Expression &inner_key, optional_ptr<const Expression> lo_key,
	                      bool lo_inclusive, optional_ptr<const Expression> hi_key, bool hi_inclusive,
	                      optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match);
	//! Shared rewrite: replace an inner join with a LogicalIndexJoin over `outer_child` (probing the
	//! inner index), rewiring inner-get references and pulling up any residual inner table filters.
	void RewriteInnerJoinToIndexJoin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan,
	                                 LogicalGet &inner_get, unique_ptr<LogicalOperator> &outer_child, BoundIndex &index,
	                                 IndexMatch &match);
	//! DuckDB >= 1.5 rewrites the lateral KNN into an arg_min/min_by top-k aggregate. These match
	//! the delim-join shape and the delim-join-as-CTE shape respectively.
	bool TryOptimizeIndexJoinArgMin(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);
	bool TryOptimizeIndexJoinArgMinCte(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &plan);
	//! Enumerate the inner table's indexes and ask each capable one whether it can serve the probe.
	bool TryBindIndexJoin(LogicalGet &inner_get, const Expression &distance_expr, idx_t k,
	                      optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match);
	//! Enumerate the table's indexes and ask each capable one whether it can serve the ordered scan.
	bool TryBindOrderLimit(LogicalGet &get, const Expression &order_expr, idx_t limit,
	                       optional_ptr<BoundIndex> &out_index, unique_ptr<IndexMatch> &out_match);
	//! Prune now-dead projection expressions stacked above a generic index scan.
	void MergeProjections(unique_ptr<LogicalOperator> &plan);

private:
	Optimizer &optimizer;
	ClientContext &context;
};

} // namespace duckdb
