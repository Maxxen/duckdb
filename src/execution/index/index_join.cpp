#include "duckdb/execution/index/index_join.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Physical operator
//===--------------------------------------------------------------------===//
class PhysicalIndexJoin final : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalIndexJoin(PhysicalPlan &physical_plan, const vector<LogicalType> &types_p, idx_t estimated_cardinality,
	                  DuckTableEntry &table_p, BoundIndex &index_p, unique_ptr<IndexScanInfo> probe_info_p,
	                  bool emit_rank_p)
	    : PhysicalOperator(physical_plan, TYPE, types_p, estimated_cardinality), table(table_p), index(index_p),
	      probe_info(std::move(probe_info_p)), emit_rank(emit_rank_p) {
		limit = probe_info->HasLimit() ? probe_info->limit : 1;
	}

	string GetName() const override {
		return "INDEX_JOIN";
	}
	bool ParallelOperator() const override {
		return false;
	}
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, GlobalOperatorState &gstate,
	                           OperatorState &state) const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	DuckTableEntry &table;
	BoundIndex &index;
	unique_ptr<IndexScanInfo> probe_info;
	bool emit_rank;
	idx_t limit;

	vector<column_t> inner_column_ids;
	vector<ProjectionIndex> inner_projection_ids;
};

class IndexJoinState final : public OperatorState {
public:
	//! Next outer row to probe within the current input chunk.
	idx_t input_idx = 0;
	//! The accumulated matches for the current fill, and how many have been emitted so far. One
	//! outer row can produce more than a vector of matches (unbounded fan-out), so matches are
	//! accumulated across a set of outer rows and emitted in STANDARD_VECTOR_SIZE-sized chunks.
	idx_t accumulated = 0;
	idx_t emit_cursor = 0;
	//! Per accumulated match: the outer row it belongs to, and (for OrderLimit joins) its rank.
	vector<idx_t> outer_of_match;
	vector<int64_t> rank_of_match;

	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<StorageIndex> physical_column_ids;
	unique_ptr<IndexScanState> index_state;
	SelectionVector match_sel;
};

unique_ptr<OperatorState> PhysicalIndexJoin::GetOperatorState(ExecutionContext &context) const {
	auto result = make_uniq<IndexJoinState>();

	auto &local_storage = LocalStorage::Get(context.client, table.catalog);
	result->physical_column_ids.reserve(inner_column_ids.size());
	for (auto &id : inner_column_ids) {
		storage_t col_id = id;
		if (id != DConstants::INVALID_INDEX) {
			col_id = table.GetColumn(LogicalIndex(id)).StorageOid();
		}
		result->physical_column_ids.emplace_back(col_id);
	}

	result->match_sel.Initialize();
	result->local_storage_state.Initialize(result->physical_column_ids, nullptr);
	local_storage.InitializeScan(table.GetStorage(), result->local_storage_state.local_state, nullptr);
	result->index_state = index.InitializeProbe(*probe_info, context.client);
	return std::move(result);
}

OperatorResultType PhysicalIndexJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                              GlobalOperatorState &gstate, OperatorState &ostate) const {
	auto &state = ostate.Cast<IndexJoinState>();
	auto &transaction = DuckTransaction::Get(context.client, table.catalog);

	input.Flatten();

	// Layout of the output chunk: [inner columns][optional rank][outer columns]
	const auto MATCH_COLUMN_OFFSET = inner_column_ids.size();
	const auto OUTER_COLUMN_OFFSET = MATCH_COLUMN_OFFSET + (emit_rank ? 1 : 0);
	int64_t *rank_vector = emit_rank ? FlatVector::GetDataMutable<int64_t>(chunk.data[MATCH_COLUMN_OFFSET]) : nullptr;

	// (Re)fill the accumulator once we have emitted everything from the previous fill.
	if (state.emit_cursor >= state.accumulated) {
		if (state.input_idx >= input.size()) {
			state.input_idx = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
		index.ResetProbe(*state.index_state);
		state.outer_of_match.clear();
		state.rank_of_match.clear();
		state.accumulated = 0;
		state.emit_cursor = 0;
		// Accumulate matches from outer rows until we have at least a full output vector (bounding a
		// single fill), or the input is exhausted. A single outer row may still contribute more than
		// STANDARD_VECTOR_SIZE matches; those are emitted across multiple chunks below.
		while (state.input_idx < input.size() && state.accumulated < STANDARD_VECTOR_SIZE) {
			const auto match_count = index.ProbeRow(*state.index_state, input, state.input_idx, *probe_info);
			for (idx_t i = 0; i < match_count; i++) {
				state.outer_of_match.push_back(state.input_idx);
				if (emit_rank) {
					state.rank_of_match.push_back(static_cast<int64_t>(i + 1)); // 1-indexed rank
				}
			}
			state.accumulated += match_count;
			state.input_idx++;
		}
		if (state.accumulated == 0) {
			chunk.SetCardinality(0);
			if (state.input_idx < input.size()) {
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			state.input_idx = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	// Emit up to STANDARD_VECTOR_SIZE accumulated matches.
	const auto &row_ids_full = index.GetProbeResult(*state.index_state);
	const idx_t n = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.accumulated - state.emit_cursor);
	for (idx_t i = 0; i < n; i++) {
		state.match_sel.set_index(i, state.outer_of_match[state.emit_cursor + i]);
		if (rank_vector) {
			rank_vector[i] = state.rank_of_match[state.emit_cursor + i];
		}
	}
	// Fetch the inner columns for this chunk's row-ids (a slice of the accumulator).
	Vector emit_ids(row_ids_full, state.emit_cursor, state.emit_cursor + n);
	emit_ids.Flatten();
	table.GetStorage().Fetch(transaction, chunk, state.physical_column_ids, emit_ids, n, state.fetch_state);
	// Attach the outer columns by slicing the input with the outer-row mapping.
	chunk.Slice(input, state.match_sel, n, OUTER_COLUMN_OFFSET);
	chunk.SetCardinality(n);
	state.emit_cursor += n;

	if (state.emit_cursor < state.accumulated || state.input_idx < input.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	state.input_idx = 0;
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalIndexJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result.insert("Table", table.name.GetIdentifierName());
	result.insert("Index", index.GetIndexName().GetIdentifierName());
	if (emit_rank) {
		result.insert("Limit", to_string(limit));
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

//===--------------------------------------------------------------------===//
// Logical operator
//===--------------------------------------------------------------------===//
LogicalIndexJoin::LogicalIndexJoin(TableIndex table_index_p, DuckTableEntry &table_p, BoundIndex &index_p,
                                   unique_ptr<IndexScanInfo> probe_info_p, bool emit_rank_p)
    : table_index(table_index_p), table(table_p), index(index_p), probe_info(std::move(probe_info_p)),
      emit_rank(emit_rank_p) {
}

string LogicalIndexJoin::GetName() const {
	return "INDEX_JOIN";
}

string LogicalIndexJoin::GetExtensionName() const {
	return "index_join";
}

void LogicalIndexJoin::ResolveTypes() {
	if (inner_column_ids.empty()) {
		inner_column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	types.clear();

	auto add_type = [&](column_t index_id) {
		if (index_id == COLUMN_IDENTIFIER_ROW_ID) {
			types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			types.push_back(inner_returned_types[index_id]);
		}
	};
	if (inner_projection_ids.empty()) {
		for (const auto &index_id : inner_column_ids) {
			add_type(index_id);
		}
	} else {
		for (const auto &proj_index : inner_projection_ids) {
			add_type(inner_column_ids[proj_index]);
		}
	}

	// The optional rank column follows the inner columns.
	if (emit_rank) {
		types.emplace_back(LogicalType::BIGINT);
	}

	// Followed by the outer (right child) columns.
	auto &right_types = children[0]->types;
	types.insert(types.end(), right_types.begin(), right_types.end());
}

vector<ColumnBinding> LogicalIndexJoin::GetLeftBindings() {
	vector<ColumnBinding> result;
	if (inner_projection_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < inner_column_ids.size(); col_idx++) {
			result.emplace_back(table_index, ProjectionIndex(col_idx));
		}
	} else {
		for (auto proj_id : inner_projection_ids) {
			result.emplace_back(table_index, proj_id);
		}
	}
	if (emit_rank) {
		// The rank column is bound at the position after the inner columns.
		result.emplace_back(table_index, ProjectionIndex(inner_column_ids.size()));
	}
	return result;
}

vector<ColumnBinding> LogicalIndexJoin::GetRightBindings() {
	vector<ColumnBinding> result;
	for (auto &binding : children[0]->GetColumnBindings()) {
		result.push_back(binding);
	}
	return result;
}

vector<ColumnBinding> LogicalIndexJoin::GetColumnBindings() {
	auto result = GetLeftBindings();
	auto right_bindings = GetRightBindings();
	result.insert(result.end(), right_bindings.begin(), right_bindings.end());
	return result;
}

PhysicalOperator &LogicalIndexJoin::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &result =
	    planner.Make<PhysicalIndexJoin>(types, estimated_cardinality, table, index, std::move(probe_info), emit_rank);
	auto &cast_result = result.Cast<PhysicalIndexJoin>();
	cast_result.inner_column_ids = inner_column_ids;
	cast_result.inner_projection_ids = inner_projection_ids;

	auto &plan = planner.CreatePlan(*children[0]);
	result.children.push_back(plan);
	return result;
}

idx_t LogicalIndexJoin::EstimateCardinality(ClientContext &context) {
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	const auto child_cardinality = children[0]->EstimateCardinality(context);
	const auto k = probe_info && probe_info->HasLimit() ? probe_info->limit : 1;
	estimated_cardinality = child_cardinality * k;
	has_estimated_cardinality = true;
	return estimated_cardinality;
}

} // namespace duckdb
