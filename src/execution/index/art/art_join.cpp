#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Probe state
//===--------------------------------------------------------------------===//
struct ARTProbeState final : public IndexScanState {
	explicit ARTProbeState(Allocator &allocator) : arena(allocator), row_id_vec(LogicalType::ROW_TYPE) {
	}

	ArenaAllocator arena;
	//! The outer input chunk whose keys are currently generated/cached.
	optional_ptr<DataChunk> cached_input;
	//! Generated ART keys for the equality/lower-bound column and (for ranges) the upper-bound column.
	unsafe_vector<ARTKey> lo_keys;
	unsafe_vector<ARTKey> hi_keys;
	//! Accumulated matching inner row-ids, wrapped as a Vector for GetProbeResult.
	vector<row_t> row_ids;
	Vector row_id_vec;
};

//! Generate ART keys for the given outer input column into `keys`. The outer value is already the
//! indexed value (the join condition equates it to the index expression), so keys are generated from
//! it directly - GenerateKeys does not re-apply the index expression.
void ART::GenerateProbeKeys(ArenaAllocator &arena, DataChunk &input, idx_t column, unsafe_vector<ARTKey> &keys) {
	DataChunk key_chunk;
	key_chunk.InitializeEmpty(vector<LogicalType> {logical_types[0]});
	key_chunk.data[0].Reference(input.data[column]);
	key_chunk.SetCardinality(input.size());
	keys.clear();
	keys.resize(input.size()); // TemplatedGenerateKeys writes keys[i]; must be pre-sized
	GenerateKeys(arena, key_chunk, keys);
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
//! Rebase the ART's stored (unbound) indexed expression onto the current LogicalGet's bindings.
static void RewriteARTIndexExpression(const ART &art, const LogicalGet &get, Expression &expr, bool &rewrite_possible) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		bound_colref.BindingMutable().table_index = get.table_index;
		auto &column_ids = art.GetColumnIds();
		auto &get_column_ids = get.GetColumnIds();
		column_t referenced_column = column_ids[bound_colref.Binding().column_index];
		for (idx_t i = 0; i < get_column_ids.size(); i++) {
			if (get_column_ids[i].GetPrimaryIndex() == referenced_column) {
				bound_colref.BindingMutable().column_index = ProjectionIndex(i);
				return;
			}
		}
		rewrite_possible = false;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteARTIndexExpression(art, get, child, rewrite_possible); });
}

//! Rebase the index expression onto `inner_get` and check it equals the join-condition inner key.
bool ART::MatchesIndexExpression(const LogicalGet &inner_get, const Expression &inner_key) {
	if (unbound_expressions.size() != 1 || GetColumnIds().size() != 1) {
		return false;
	}
	auto index_expr = unbound_expressions[0]->Copy();
	bool rewrite_possible = true;
	RewriteARTIndexExpression(*this, inner_get, *index_expr, rewrite_possible);
	return rewrite_possible && index_expr->Equals(inner_key) &&
	       index_expr->GetReturnType() == logical_types[0]; // keep encoding type consistent
}

unique_ptr<IndexMatch> ART::TryBindPredicateJoin(ClientContext &, const LogicalGet &inner_get,
                                                 const Expression &inner_key, const Expression &outer_key) {
	if (outer_key.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	if (!MatchesIndexExpression(inner_get, inner_key) || outer_key.GetReturnType() != logical_types[0]) {
		return nullptr;
	}
	auto &outer_ref = outer_key.Cast<BoundColumnRefExpression>();
	auto match = make_uniq<IndexMatch>();
	match->query_params = make_uniq<ARTJoinInfo>(ARTJoinInfo::Equality(outer_ref.Binding().column_index));
	match->is_exact = true; // an equality lookup is exact - no residual recheck needed
	match->probe_key_binding = outer_ref.Binding();
	return match;
}

unique_ptr<IndexMatch> ART::TryBindRangeJoin(ClientContext &, const LogicalGet &inner_get, const Expression &inner_key,
                                             optional_ptr<const Expression> lo_key, bool lo_inclusive,
                                             optional_ptr<const Expression> hi_key, bool hi_inclusive) {
	if (!lo_key && !hi_key) {
		return nullptr;
	}
	if (!MatchesIndexExpression(inner_get, inner_key)) {
		return nullptr;
	}
	idx_t lo_column = DConstants::INVALID_INDEX;
	idx_t hi_column = DConstants::INVALID_INDEX;
	if (lo_key) {
		if (lo_key->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    lo_key->GetReturnType() != logical_types[0]) {
			return nullptr;
		}
		lo_column = lo_key->Cast<BoundColumnRefExpression>().Binding().column_index;
	}
	if (hi_key) {
		if (hi_key->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    hi_key->GetReturnType() != logical_types[0]) {
			return nullptr;
		}
		hi_column = hi_key->Cast<BoundColumnRefExpression>().Binding().column_index;
	}
	auto match = make_uniq<IndexMatch>();
	match->query_params =
	    make_uniq<ARTJoinInfo>(ARTJoinInfo::Range(lo_column, lo_inclusive, hi_column, hi_inclusive));
	match->is_exact = true; // an ordered range search is exact
	match->probe_key_binding = (lo_key ? lo_key : hi_key)->Cast<BoundColumnRefExpression>().Binding();
	return match;
}

//===--------------------------------------------------------------------===//
// Probe execution
//===--------------------------------------------------------------------===//
unique_ptr<IndexScanState> ART::InitializeProbe(const IndexScanInfo &, ClientContext &context) {
	return make_uniq<ARTProbeState>(Allocator::Get(context));
}

void ART::ResetProbe(IndexScanState &state) {
	state.Cast<ARTProbeState>().row_ids.clear();
}

idx_t ART::ProbeRow(IndexScanState &state, DataChunk &input, idx_t row_idx, const IndexScanInfo &info) {
	auto &pstate = state.Cast<ARTProbeState>();
	auto &join_info = info.Cast<ARTJoinInfo>();

	// Generate the ART keys for the whole outer key/bound column(s) once per input chunk.
	if (pstate.cached_input.get() != &input) {
		pstate.arena.Reset();
		if (join_info.equality) {
			GenerateProbeKeys(pstate.arena, input, join_info.probe_key_column, pstate.lo_keys);
		} else {
			if (join_info.lo_column != DConstants::INVALID_INDEX) {
				GenerateProbeKeys(pstate.arena, input, join_info.lo_column, pstate.lo_keys);
			}
			if (join_info.hi_column != DConstants::INVALID_INDEX) {
				GenerateProbeKeys(pstate.arena, input, join_info.hi_column, pstate.hi_keys);
			}
		}
		pstate.cached_input = input;
	}

	set<row_t> matches;
	{
		lock_guard<mutex> l(lock);
		if (join_info.equality) {
			auto &key = pstate.lo_keys[row_idx];
			if (key.Empty()) {
				return 0; // a NULL key never equi-matches
			}
			SearchEqual(key, NumericLimits<idx_t>::Maximum(), matches);
		} else {
			const bool has_lo = join_info.lo_column != DConstants::INVALID_INDEX;
			const bool has_hi = join_info.hi_column != DConstants::INVALID_INDEX;
			// A NULL bound makes the comparison NULL, i.e. no match.
			if ((has_lo && pstate.lo_keys[row_idx].Empty()) || (has_hi && pstate.hi_keys[row_idx].Empty())) {
				return 0;
			}
			if (has_lo && has_hi) {
				SearchCloseRange(pstate.lo_keys[row_idx], pstate.hi_keys[row_idx], join_info.lo_inclusive,
				                 join_info.hi_inclusive, NumericLimits<idx_t>::Maximum(), matches);
			} else if (has_lo) {
				SearchGreater(pstate.lo_keys[row_idx], join_info.lo_inclusive, NumericLimits<idx_t>::Maximum(), matches);
			} else {
				SearchLess(pstate.hi_keys[row_idx], join_info.hi_inclusive, NumericLimits<idx_t>::Maximum(), matches);
			}
		}
	}
	for (auto row_id : matches) {
		pstate.row_ids.push_back(row_id);
	}
	return matches.size();
}

const Vector &ART::GetProbeResult(IndexScanState &state) {
	auto &pstate = state.Cast<ARTProbeState>();
	FlatVector::SetData(pstate.row_id_vec, reinterpret_cast<data_ptr_t>(pstate.row_ids.data()),
	                    count_t(pstate.row_ids.size()));
	return pstate.row_id_vec;
}

} // namespace duckdb
