#include "duckdb/function/table/index_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//-------------------------------------------------------------------------
// Bind info
//-------------------------------------------------------------------------
static BindInfo IndexScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<IndexScanBindData>();
	return BindInfo(bind_data.table);
}

//-------------------------------------------------------------------------
// Global state
//-------------------------------------------------------------------------
struct IndexScanGlobalState final : public GlobalTableFunctionState {
	//! The DataChunk containing all read columns (filter columns are removed via projection_ids).
	DataChunk all_columns;
	vector<idx_t> projection_ids;

	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<StorageIndex> column_ids;

	//! The index scan state and the row-ids it produces.
	unique_ptr<IndexScanState> index_state;
	Vector row_ids = Vector(LogicalType::ROW_TYPE);
};

static unique_ptr<GlobalTableFunctionState> IndexScanInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IndexScanBindData>();
	auto result = make_uniq<IndexScanGlobalState>();

	// Set up the local storage scan (for transaction-local, not-yet-indexed rows).
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	result->column_ids.reserve(input.column_ids.size());
	for (auto &id : input.column_ids) {
		storage_t col_id = id;
		if (id != DConstants::INVALID_INDEX) {
			col_id = bind_data.table.GetColumn(LogicalIndex(id)).StorageOid();
		}
		result->column_ids.emplace_back(col_id);
	}
	result->local_storage_state.Initialize(result->column_ids, context, input.filters);
	local_storage.InitializeScan(bind_data.table.GetStorage(), result->local_storage_state.local_state, input.filters);

	// Initialize the index scan.
	result->index_state = bind_data.index.InitializeScan(*bind_data.scan_info, context);

	if (!input.CanRemoveFilterColumns()) {
		return std::move(result);
	}

	// We need this to project out what we scan from the underlying table.
	result->projection_ids = input.projection_ids;
	const auto &columns = bind_data.table.GetColumns();
	vector<LogicalType> scanned_types;
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	result->all_columns.Initialize(context, scanned_types);
	return std::move(result);
}

//-------------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------------
static void IndexScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<IndexScanBindData>();
	auto &state = data_p.global_state->Cast<IndexScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);

	// Ask the index for the next batch of row-ids.
	auto row_count = bind_data.index.Scan(*state.index_state, STANDARD_VECTOR_SIZE, state.row_ids);

	if (row_count == 0) {
		// The index is exhausted. Scan transaction-local storage for not-yet-indexed rows.
		auto &local_storage = LocalStorage::Get(transaction);
		if (state.projection_ids.empty()) {
			local_storage.Scan(state.local_storage_state.local_state, state.column_ids, output);
			return;
		}
		state.all_columns.Reset();
		local_storage.Scan(state.local_storage_state.local_state, state.column_ids, state.all_columns);
		output.ReferenceColumns(state.all_columns, state.projection_ids);
		return;
	}

	// Fetch the table columns for the produced row-ids.
	if (state.projection_ids.empty()) {
		bind_data.table.GetStorage().Fetch(transaction, output, state.column_ids, state.row_ids, row_count,
		                                   state.fetch_state);
		return;
	}
	state.all_columns.Reset();
	bind_data.table.GetStorage().Fetch(transaction, state.all_columns, state.column_ids, state.row_ids, row_count,
	                                   state.fetch_state);
	output.ReferenceColumns(state.all_columns, state.projection_ids);
}

//-------------------------------------------------------------------------
// Statistics
//-------------------------------------------------------------------------
static unique_ptr<BaseStatistics> IndexScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = bind_data_p->Cast<IndexScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	if (local_storage.Find(bind_data.table.GetStorage())) {
		// no statistics for tables with outstanding transaction-local data
		return nullptr;
	}
	return bind_data.table.GetStatistics(context, column_id);
}

//-------------------------------------------------------------------------
// Dependency
//-------------------------------------------------------------------------
static void IndexScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<IndexScanBindData>();
	entries.AddDependency(bind_data.table);
}

//-------------------------------------------------------------------------
// Cardinality
//-------------------------------------------------------------------------
static unique_ptr<NodeStatistics> IndexScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<IndexScanBindData>();
	return make_uniq<NodeStatistics>(bind_data.estimated_cardinality, bind_data.estimated_cardinality);
}

//-------------------------------------------------------------------------
// ToString
//-------------------------------------------------------------------------
static InsertionOrderPreservingMap<string> IndexScanToString(TableFunctionToStringInput &input) {
	D_ASSERT(input.bind_data);
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<IndexScanBindData>();
	result["Table"] = bind_data.table.name.GetIdentifierName();
	result["Index"] = bind_data.index.GetIndexName().GetIdentifierName();
	return result;
}

//-------------------------------------------------------------------------
// Get function
//-------------------------------------------------------------------------
TableFunction IndexScanFunction::GetFunction() {
	TableFunction func("index_scan", {}, IndexScanExecute);
	func.init_local = nullptr;
	func.init_global = IndexScanInitGlobal;
	func.statistics = IndexScanStatistics;
	func.dependency = IndexScanDependency;
	func.cardinality = IndexScanCardinality;
	func.pushdown_complex_filter = nullptr;
	func.to_string = IndexScanToString;
	func.table_scan_progress = nullptr;
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.get_bind_info = IndexScanGetBindInfo;
	func.parallelism = TableFunctionParallelism::SEQUENTIAL;
	return func;
}

} // namespace duckdb
