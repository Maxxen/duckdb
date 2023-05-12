#include "duckdb/execution/index/rtree/rtree.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include <algorithm>
#include <cstring>

namespace duckdb {


//------------------------------------------------------------------------------
// RTree Methods
//------------------------------------------------------------------------------

RTree::RTree(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
             const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
             AttachedDatabase &db, const idx_t block_id, const idx_t block_offset)
    : Index(db, IndexType::RTREE, table_io_manager, column_ids, unbound_expressions, constraint_type) {
}

RTree::~RTree() {
}

//------------------------------------------------------------------------------
// Index Interface
//------------------------------------------------------------------------------

unique_ptr<IndexScanState> RTree::InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                                const ExpressionType expression_type) {
	throw NotImplementedException("RTree::InitializeScanSinglePredicate");
}

//! Initialize a two predicate scan on the index with the given expression and column IDs
unique_ptr<IndexScanState> RTree::InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
                                                              const ExpressionType low_expression_type,
                                                              const Value &high_value,
                                                              const ExpressionType high_expression_type) {
	throw NotImplementedException("RTree::InitializeScanTwoPredicates");
};

bool RTree::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
                 vector<row_t> &result_ids) {
	throw NotImplementedException("RTree::Scan");
}

/*
void RTree::InitializeLock(IndexLock &state) {
	throw NotImplementedException("RTree::InitializeLock");
}
*/

PreservedError RTree::Append(IndexLock &state, DataChunk &entries, Vector &row_identifiers) {
	throw NotImplementedException("RTree::Append");
}

void RTree::VerifyAppend(DataChunk &chunk) {
	throw NotImplementedException("RTree::VerifyAppend");
}

void RTree::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	throw NotImplementedException("RTree::VerifyAppend");
}

void RTree::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {
	throw NotImplementedException("RTree::CheckConstraintsForChunk");
}

void RTree::Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) {
	throw NotImplementedException("RTree::Delete");
}

PreservedError RTree::Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) {
	throw NotImplementedException("RTree::Insert");
}

bool RTree::MergeIndexes(IndexLock &state, Index &other_index) {
	throw NotImplementedException("RTree::MergeIndexes");
}

void RTree::Vacuum(IndexLock &state) {
	throw NotImplementedException("RTree::Vacuum");
}

string RTree::ToString() {
	return "<RTREE>";
}

} // namespace duckdb
