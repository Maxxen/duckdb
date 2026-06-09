//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {
class ColumnList;
class PersistentTableData;

//! The table data reader is responsible for reading the data of a table from the block manager
class TableDataReader {
public:
	TableDataReader(MetadataReader &reader, ColumnList &columns, PersistentTableData &data,
	                MetaBlockPointer table_pointer);

	void ReadTableData();

private:
	MetadataReader &reader;
	ColumnList &columns;
	PersistentTableData &data;
};

} // namespace duckdb
