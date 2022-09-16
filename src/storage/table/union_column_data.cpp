#include "duckdb/storage/table/union_column_data.hpp"
#include "duckdb/storage/statistics/union_statistics.hpp"

namespace duckdb {

UnionColumnData::UnionColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, LogicalType type_p,
                                 ColumnData *parent)
    : ColumnData(info, column_index, start_row, move(type_p), parent), validity(info, 0, start_row, this) {
	D_ASSERT(type.InternalType() == PhysicalType::UNION);
	auto &child_types = UnionType::GetChildTypes(type);
	D_ASSERT(child_types.size() > 0);
	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	for (auto &child_type : child_types) {
		sub_columns.push_back(
		    ColumnData::CreateColumnUnique(info, sub_column_index, start_row, child_type.second, this));
		sub_column_index++;
	}
}

bool UnionColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for union columns
	return false;
}

idx_t UnionColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void UnionColumnData::InitializeScan(ColumnScanState &state) {
	D_ASSERT(state.child_states.empty());

	// Initialize the tag column
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScan(validity_state);
	state.child_states.push_back(move(validity_state));

	// initialize the sub-columns
	for (auto &sub_column : sub_columns) {
		ColumnScanState child_state;
		sub_column->InitializeScan(child_state);
		state.child_states.push_back(move(child_state));
	}
}

void UnionColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	D_ASSERT(state.child_states.empty());

	// Initialize the tag column
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	ColumnScanState validity_state;
	validity.InitializeScanWithOffset(validity_state, row_idx);
	state.child_states.push_back(move(validity_state));

	// initialize the sub-columns
	for (auto &sub_column : sub_columns) {
		ColumnScanState child_state;
		sub_column->InitializeScanWithOffset(child_state, row_idx);
		state.child_states.push_back(move(child_state));
	}
}

idx_t UnionColumnData::Scan(Transaction &transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	// scan the tag
	auto scan_count = ColumnData::Scan(transaction, vector_index, state, result);
	// scan the validity mask
	validity.Scan(transaction, vector_index, state.child_states[0], result);

	// scan the sub-columns
	auto &child_entries = UnionVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->Scan(transaction, vector_index, state.child_states[i + 1], *child_entries[i]);
	}
	return scan_count;
}

idx_t UnionColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	auto scan_count = ColumnData::ScanCommitted(vector_index, state, result, allow_updates);

	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	auto &child_entries = UnionVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCommitted(vector_index, state.child_states[i + 1], *child_entries[i], allow_updates);
	}
	return scan_count;
}

idx_t UnionColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = ColumnData::ScanCount(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);
	auto &child_entries = UnionVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCount(state.child_states[i + 1], *child_entries[i], count);
	}
	return scan_count;
}

void UnionColumnData::InitializeAppend(ColumnAppendState &state) {

	ColumnData::InitializeAppend(state);

	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(move(validity_append));

	for (auto &sub_column : sub_columns) {
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(move(child_append));
	}
}

void UnionColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);

	vector.Flatten(count);

	// append the tags like a standard column
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	ColumnData::AppendData(stats, state, vdata, count);

	// append the null values
	validity.Append(*stats.validity_stats, state.child_appends[0], vector, count);

	auto &union_validity = FlatVector::Validity(vector);

	auto &union_stats = (UnionStatistics &)stats;
	auto &child_entries = UnionVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (!union_validity.AllValid()) {
			// we set the child entries of the union to NULL
			// for any values in which the union itself is NULL
			child_entries[i]->Flatten(count);

			auto &child_validity = FlatVector::Validity(*child_entries[i]);
			child_validity.Combine(union_validity, count);
		}
		sub_columns[i]->Append(*union_stats.child_stats[i], state.child_appends[i + 1], *child_entries[i], count);
	}
}

void UnionColumnData::RevertAppend(row_t start_row) {
	// TODO

	validity.RevertAppend(start_row);
	for (auto &sub_column : sub_columns) {
		sub_column->RevertAppend(start_row);
	}
}

idx_t UnionColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("Union Fetch");
	/*
	// fetch validity mask
	auto &child_entries = UnionVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
	    ColumnScanState child_state;
	    state.child_states.push_back(move(child_state));
	}
	// fetch the validity state
	idx_t scan_count = validity.Fetch(state.child_states[0], row_id, result);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
	    sub_columns[i]->Fetch(state.child_states[i + 1], row_id, *child_entries[i]);
	}
	return scan_count;
	*/
}

void UnionColumnData::Update(Transaction &transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                             idx_t update_count) {
	throw NotImplementedException("Union Update is not supported");
	/*
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
	auto &child_entries = UnionVector::GetEntries(update_vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
	    sub_columns[i]->Update(transaction, column_index, *child_entries[i], row_ids, update_count);
	}
	*/
}

void UnionColumnData::UpdateColumn(Transaction &transaction, const vector<column_t> &column_path, Vector &update_vector,
                                   row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("Union Update Column is not supported");
	/*
	// we can never DIRECTLY update a union column
	if (depth >= column_path.size()) {
	    throw InternalException("Attempting to directly update a union column - this should not be possible");
	}
	auto update_column = column_path[depth];
	if (update_column == 0) {
	    // update the validity column
	    validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	} else {
	    if (update_column > sub_columns.size()) {
	        throw InternalException("Update column_path out of range");
	    }
	    sub_columns[update_column - 1]->UpdateColumn(transaction, column_path, update_vector, row_ids, update_count,
	                                                 depth + 1);
	}
	*/
}

unique_ptr<BaseStatistics> UnionColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type, StatisticsType::GLOBAL_STATS);
	auto &union_stats = (UnionStatistics &)*stats;
	stats->validity_stats = validity.GetUpdateStatistics();
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto child_stats = sub_columns[i]->GetUpdateStatistics();
		if (child_stats) {
			union_stats.child_stats[i] = move(child_stats);
		}
	}
	return stats;
}

void UnionColumnData::FetchRow(Transaction &transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                               idx_t result_idx) {
	// insert any child states that are required
	// (validity, and all the sub-columns)

	// sub-columns
	auto &child_entries = UnionVector::GetEntries(result);
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		auto child_state = make_unique<ColumnFetchState>();
		state.child_states.push_back(move(child_state));
	}

	// fetch the union tag and the validity mask for it
	auto segment = (ColumnSegment *)data.GetSegment(0);
	segment->FetchRow(state, row_id, result, result_idx);
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	// fetch the sub-column states (if the union is not NULL)
	auto &validity = FlatVector::Validity(result);
	if (!validity.RowIsValid(result_idx)) {
		// the union itself is NULL, we dont need to fetch any of the sub-columns
		return;
	}

	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_entries[i], result_idx);
	}

	/*
	// fetch validity mask
	auto &child_entries = UnionVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
	    auto child_state = make_unique<ColumnFetchState>();
	    state.child_states.push_back(move(child_state));
	}
	// fetch the validity state
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
	    sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_entries[i], result_idx);
	}
	*/
}

void UnionColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}
}

struct UnionColumnCheckpointState : public ColumnCheckpointState {
	UnionColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
	    : ColumnCheckpointState(row_group, column_data, writer) {
		global_stats = make_unique<UnionStatistics>(column_data.type);
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = make_unique<UnionStatistics>(column_data.type);
		D_ASSERT(stats->child_stats.size() == child_states.size());
		stats->validity_stats = validity_state->GetStatistics();
		for (idx_t i = 0; i < child_states.size(); i++) {
			stats->child_stats[i] = child_states[i]->GetStatistics();
			D_ASSERT(stats->child_stats[i]);
		}
		return move(stats);
	}

	void FlushToDisk() override {
		validity_state->FlushToDisk();
		for (auto &state : child_states) {
			state->FlushToDisk();
		}
	}
};

unique_ptr<ColumnCheckpointState> UnionColumnData::CreateCheckpointState(RowGroup &row_group, TableDataWriter &writer) {
	return make_unique<UnionColumnCheckpointState>(row_group, *this, writer);
}

unique_ptr<ColumnCheckpointState> UnionColumnData::Checkpoint(RowGroup &row_group, TableDataWriter &writer,
                                                              ColumnCheckpointInfo &checkpoint_info) {
	auto checkpoint_state = make_unique<UnionColumnCheckpointState>(row_group, *this, writer);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, writer, checkpoint_info);
	for (auto &sub_column : sub_columns) {
		checkpoint_state->child_states.push_back(sub_column->Checkpoint(row_group, writer, checkpoint_info));
	}
	return move(checkpoint_state);
}

void UnionColumnData::DeserializeColumn(Deserializer &source) {
	validity.DeserializeColumn(source);
	for (auto &sub_column : sub_columns) {
		sub_column->DeserializeColumn(source);
	}
}

void UnionColumnData::GetStorageInfo(idx_t row_group_index, vector<idx_t> col_path, vector<vector<Value>> &result) {
	col_path.push_back(0);
	validity.GetStorageInfo(row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetStorageInfo(row_group_index, col_path, result);
	}
}

void UnionColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (auto &sub_column : sub_columns) {
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb
