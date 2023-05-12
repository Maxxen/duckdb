#include "duckdb/execution/operator/schema/physical_create_rtree_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/execution/index/rtree/rtree.hpp"

namespace duckdb {

PhysicalCreateRTreeIndex::PhysicalCreateRTreeIndex(LogicalOperator &op, TableCatalogEntry &table_p,
                                         const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                         vector<unique_ptr<Expression>> unbound_expressions,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)),
      unbound_expressions(std::move(unbound_expressions)) {
	D_ASSERT(table_p.IsDuckTable());
	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateRTreeIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<Index> global_index;
};

/*
class CreateRTreeIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateRTreeIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<Index> local_index;
	ArenaAllocator arena_allocator;
	DataChunk key_chunk;
	vector<column_t> key_column_ids;
};
*/

unique_ptr<GlobalSinkState> PhysicalCreateRTreeIndex::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(info->index_type == IndexType::RTREE);
	auto state = make_uniq<CreateRTreeIndexGlobalSinkState>();

	// create the index
	

	return (std::move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateRTreeIndex::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LocalSinkState>();
}

SinkResultType PhysicalCreateRTreeIndex::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateRTreeIndexGlobalSinkState>();

	throw NotImplementedException("CreateRTreeIndex::Sink()");

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCreateRTreeIndex::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	throw NotImplementedException("CreateRTreeIndex::Combine()");
}

SinkFinalizeType PhysicalCreateRTreeIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {

	// here, we just set the resulting global index as the newly created index of the table
	auto &state = gstate_p.Cast<CreateRTreeIndexGlobalSinkState>();
	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = table.schema;
	auto index_entry = schema.CreateIndex(context, *info, table).get();
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}
	auto &index = index_entry->Cast<DuckIndexEntry>();

	index.index = state.global_index.get();
	index.info = storage.info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// vacuum excess memory
	// state.global_index->Vacuum();

	// add index to storage
	storage.info->indexes.AddIndex(std::move(state.global_index));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalCreateRTreeIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
