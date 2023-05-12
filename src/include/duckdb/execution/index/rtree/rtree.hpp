//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/rtree/rtree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/index.hpp"

namespace duckdb {

class ARTKey;

class RTree : public Index {
public:
	RTree(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	      const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
	      AttachedDatabase &db, const idx_t block_id = DConstants::INVALID_INDEX,
	      const idx_t block_offset = DConstants::INVALID_INDEX);

	~RTree() final;

    void ConstructFromSorted(idx_t count, vector<ARTKey> &keys, Vector &row_identifiers);

public: // Index Interface
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                         const ExpressionType expression_type) final;

	//! Initialize a two predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
	                                                       const ExpressionType low_expression_type,
	                                                       const Value &high_value,
	                                                       const ExpressionType high_expression_type) final;

	bool Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
	          vector<row_t> &result_ids) final;

	PreservedError Append(IndexLock &state, DataChunk &entries, Vector &row_identifiers) final;
	void VerifyAppend(DataChunk &chunk) final;
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) final;
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) final;
	void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) final;
	PreservedError Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) final;
	bool MergeIndexes(IndexLock &state, Index &other_index) final;
	void Vacuum(IndexLock &state) final;
	string ToString() final;
};

} // namespace duckdb
