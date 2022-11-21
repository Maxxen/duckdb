#include "duckdb/main/node_query_result.hpp"

#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"

#include <iostream>

namespace duckdb {

NodeQueryResult::NodeQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
                                 unique_ptr<ColumnDataCollection> collection_p, ClientProperties client_properties,
                                 const NodeQueryResult::NodeResultCallback &callback_p)
    :QueryResult(QueryResultType::EXTENSION_QUERY_RESULT, statement_type, properties,
                                      collection_p->Types(), move(names_p), move(client_properties)), callback(callback_p) {
}


static NodeQueryResult::NodeResultCallback* DefaultCallback = new NodeQueryResult::NodeResultCallback([](const DataChunk &chunk) { });

NodeQueryResult::NodeQueryResult(PreservedError error)
    : QueryResult(QueryResultType::EXTENSION_QUERY_RESULT, 
	move(error)), 
	scan_initialized(false), 
	callback(*DefaultCallback) {
}

string NodeQueryResult::ToString() {
	return "NodeQueryResult";
}

unique_ptr<DataChunk> NodeQueryResult::FetchRaw() {


	if (HasError()) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", GetError());
	}
	auto result = make_unique<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}

	callback(*result);

	return result;
}

Value NodeQueryResult::GetValue(idx_t column, idx_t index) {
	if (!row_collection) {
		row_collection = make_unique<ColumnDataRowCollection>(collection->GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t NodeQueryResult::RowCount() const {
	return collection ? collection->Count() : 0;
}

ColumnDataCollection &NodeQueryResult::Collection() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		throw InternalException("Missing collection from materialized query result");
	}
	return *collection;
}

unique_ptr<DataChunk> NodeQueryResult::Fetch() {
	return FetchRaw();
}

} // namespace duckdb