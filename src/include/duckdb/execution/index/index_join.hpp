//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/index/index_scan_info.hpp"

namespace duckdb {

class DuckTableEntry;
class BoundIndex;

//! Generic index-nested-loop join: for each outer row, probe an index for matching inner row-ids
//! (BoundIndex::ProbeRow), fetch the inner columns by row-id, and join to the outer row. Emits an
//! optional 1-based per-outer-row rank column (used by OrderLimit / KNN joins). Generalizes the
//! per-extension custom join operators (e.g. HNSW knn-join) into core.
class LogicalIndexJoin final : public LogicalExtensionOperator {
public:
	LogicalIndexJoin(TableIndex table_index, DuckTableEntry &table, BoundIndex &index,
	                 unique_ptr<IndexScanInfo> probe_info, bool emit_rank);

	string GetName() const override;
	string GetExtensionName() const override;
	void ResolveTypes() override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<ColumnBinding> GetLeftBindings();
	vector<ColumnBinding> GetRightBindings();
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	idx_t EstimateCardinality(ClientContext &context) override;

public:
	//! The (left) output columns are the inner table columns, produced by this operator's table index.
	TableIndex table_index;
	DuckTableEntry &table;
	BoundIndex &index;
	//! Index-specific probe parameters (carries the outer key column and, for OrderLimit, k=limit).
	unique_ptr<IndexScanInfo> probe_info;
	//! Whether to emit a per-outer-row rank column after the inner columns (OrderLimit joins).
	bool emit_rank;

	//! The inner (indexed) table's scanned columns / projection.
	vector<column_t> inner_column_ids;
	vector<ProjectionIndex> inner_projection_ids;
	vector<LogicalType> inner_returned_types;
};

} // namespace duckdb
