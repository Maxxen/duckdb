//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/deferred_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/index_storage_info.hpp"

namespace duckdb {
class MetadataManager;

//! Flip to true to defer reading persistent table data from a checkpoint until the table is first
//! materialized (i.e. bound with a ClientContext). This allows the column types to be resolved
//! lazily, rather than requiring fully-bound types at checkpoint-read time.
static constexpr bool DEFER_TABLE_DATA_LOAD = false;

//! Captured at checkpoint-read time so that the actual table data (statistics + row group metadata)
//! can be read later, once the column types have been bound. See CheckpointReader::ReadTableData.
struct DeferredTableData {
	//! The metadata manager holding the table data blocks (stable for the lifetime of the database).
	optional_ptr<MetadataManager> manager;
	//! Absolute pointer to the table data unit (statistics + row group metadata).
	MetaBlockPointer table_pointer;
	//! Total row count, read inline at checkpoint-read time.
	idx_t total_rows = 0;
	//! Next row id, read inline at checkpoint-read time.
	idx_t next_row_id = 0;
	//! Index storage infos, read inline at checkpoint-read time.
	vector<IndexStorageInfo> indexes;
};

} // namespace duckdb
