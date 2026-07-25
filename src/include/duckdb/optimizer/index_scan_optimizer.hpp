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
