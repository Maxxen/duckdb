//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/index_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/index/index_scan_info.hpp"

namespace duckdb {

class DuckTableEntry;
class BoundIndex;

//! Bind data for the generic index scan. Produced by the core index optimizer passes when an
//! index can serve a scan; carries the index and its opaque query parameters. The runtime asks
//! the index for row-ids (BoundIndex::InitializeScan / Scan) and fetches table columns by row-id.
struct IndexScanBindData final : public TableFunctionData {
	IndexScanBindData(DuckTableEntry &table, BoundIndex &index, unique_ptr<IndexScanInfo> scan_info,
	                  idx_t estimated_cardinality)
	    : table(table), index(index), scan_info(std::move(scan_info)), estimated_cardinality(estimated_cardinality) {
	}

	//! The table to scan.
	DuckTableEntry &table;
	//! The index producing the row-ids.
	BoundIndex &index;
	//! The index-specific query parameters.
	unique_ptr<IndexScanInfo> scan_info;
	//! The estimated number of produced rows.
	idx_t estimated_cardinality;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexScanBindData>();
		return &other.table == &table && &other.index == &index;
	}
};

struct IndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
