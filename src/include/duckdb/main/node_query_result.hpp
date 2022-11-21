#pragma once

#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

struct NodeQueryResult : public duckdb::QueryResult {
public:
	friend class ClientContext;

	using NodeResultCallback = std::function<void(const DataChunk &)>;

	DUCKDB_API NodeQueryResult(StatementType statement_type, StatementProperties properties,
	                                   vector<string> names, unique_ptr<ColumnDataCollection> collection,
	                                   ClientProperties client_properties, const NodeResultCallback &callback);
	
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit NodeQueryResult(PreservedError error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;

	//! Gets the (index) value of the (column index) column.
	//! Note: this is very slow. Scanning over the underlying collection is much faster.
	DUCKDB_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	DUCKDB_API idx_t RowCount() const;

	//! Returns a reference to the underlying column data collection
	ColumnDataCollection &Collection();

private:
	unique_ptr<ColumnDataCollection> collection;
	//! Row collection, only created if GetValue is called
	unique_ptr<ColumnDataRowCollection> row_collection;
	//! Scan state for Fetch calls
	ColumnDataScanState scan_state;
	bool scan_initialized;

	const NodeResultCallback &callback;
};

}; // namespace node_duckdb
