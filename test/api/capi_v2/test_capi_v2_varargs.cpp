#include "capi_v2_test_helpers.hpp"

#include <cstdlib>
#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 varargs, ANY construction / gates, bind-argument introspection, and
// expression folding (C surface). The C++ wrapper has its own suite in
// test_cpp_api_varargs.cpp.
// ---------------------------------------------------------------------------

namespace {

// Row count of a scalar exec's input chunk (every test here has >= 1 column).
idx_t ChunkRows(duckdb_v2_data_chunk_handle chunk) {
	idx_t rows = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &rows, nullptr) == DUCKDB_V2_ERROR_NONE);
	return rows;
}

idx_t ChunkCols(duckdb_v2_data_chunk_handle chunk) {
	idx_t cols = 0;
	REQUIRE(duckdb_v2_data_chunk_get_vector_count(chunk, &cols, nullptr) == DUCKDB_V2_ERROR_NONE);
	return cols;
}

// Sums every INTEGER input column into the result. Used for both a pure-varargs
// function and a fixed-param-plus-varargs function.
void IntSumExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &chunk, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle result = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result, err) == DUCKDB_V2_ERROR_NONE);
	const idx_t rows = ChunkRows(chunk);
	const idx_t cols = ChunkCols(chunk);
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result, reinterpret_cast<void **>(&out), err) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows; r++) {
		out[r] = 0;
	}
	for (idx_t c = 0; c < cols; c++) {
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, c, &vec, err) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view;
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, err) == DUCKDB_V2_ERROR_NONE);
		const auto data = static_cast<const int32_t *>(view.data);
		for (idx_t r = 0; r < rows; r++) {
			out[r] += data[view.sel ? view.sel[r] : r];
		}
	}
}

// ANY varargs: reads each column's own logical type and counts the INTEGER ones.
void CountIntsExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &chunk, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle result = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result, err) == DUCKDB_V2_ERROR_NONE);
	const idx_t rows = ChunkRows(chunk);
	const idx_t cols = ChunkCols(chunk);
	int32_t n = 0;
	for (idx_t c = 0; c < cols; c++) {
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, c, &vec, err) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_logical_type_handle lt = nullptr;
		REQUIRE(duckdb_v2_vector_get_logical_type(vec, &lt, err) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		duckdb_v2_logical_type_get_id(lt, &id, err);
		duckdb_v2_logical_type_destroy(&lt);
		if (id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER) {
			n++;
		}
	}
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result, reinterpret_cast<void **>(&out), err) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows; r++) {
		out[r] = n;
	}
}

// ANY varargs + SPECIAL null handling: counts the non-NULL argument cells per row.
void CountNonNullExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &chunk, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle result = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result, err) == DUCKDB_V2_ERROR_NONE);
	const idx_t rows = ChunkRows(chunk);
	const idx_t cols = ChunkCols(chunk);
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result, reinterpret_cast<void **>(&out), err) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows; r++) {
		out[r] = 0;
	}
	for (idx_t c = 0; c < cols; c++) {
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, c, &vec, err) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view;
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, err) == DUCKDB_V2_ERROR_NONE);
		for (idx_t r = 0; r < rows; r++) {
			const idx_t idx = view.sel ? view.sel[r] : r;
			if (RowValid(view, idx)) {
				out[r]++;
			}
		}
	}
}

// Fixed-arity ANY parameters: two ANY params, ignores them, returns 42.
void TwoAnyExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &chunk, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle result = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result, err) == DUCKDB_V2_ERROR_NONE);
	const idx_t rows = ChunkRows(chunk);
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result, reinterpret_cast<void **>(&out), err) == DUCKDB_V2_ERROR_NONE);
	for (idx_t r = 0; r < rows; r++) {
		out[r] = 42;
	}
}

// probe_bind: exercises the bind-argument accessors and their error paths, and
// signals success/failure back through a captured flag in user data.
struct ProbeResult {
	bool ran = false;
};

void ProbeBind(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_context_handle ctx,
               duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	REQUIRE(duckdb_v2_scalar_function_bind_get_user_data(info, &user_data, err) == DUCKDB_V2_ERROR_NONE);
	auto *probe = static_cast<ProbeResult *>(user_data);
	probe->ran = true;

	duckdb_v2_bind_arguments_handle arguments = nullptr;
	REQUIRE(duckdb_v2_scalar_function_bind_get_arguments(info, &arguments, err) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_bind_arguments_get_count(arguments, &count, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	// Per-argument resolved types: both slots are INTEGER.
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_logical_type_handle arg_type = nullptr;
		REQUIRE(duckdb_v2_bind_arguments_get_type(arguments, i, &arg_type, err) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		duckdb_v2_logical_type_get_id(arg_type, &id, err);
		duckdb_v2_logical_type_destroy(&arg_type);
		REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	}

	// arg 0 is a column reference: folding it fails.
	duckdb_v2_value_handle bad = nullptr;
	REQUIRE(duckdb_v2_bind_arguments_fold(arguments, ctx, 0, &bad, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(bad == nullptr);

	// arg 1 is the constant literal 7; fold it.
	duckdb_v2_value_handle folded = nullptr;
	REQUIRE(duckdb_v2_bind_arguments_fold(arguments, ctx, 1, &folded, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2LeafPayloadConsume<int32_t>(folded) == 7);

	// Out-of-range index and null handles are rejected.
	duckdb_v2_logical_type_handle oob_type = nullptr;
	REQUIRE(duckdb_v2_bind_arguments_get_type(arguments, 5, &oob_type, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(oob_type == nullptr);
	duckdb_v2_value_handle oob_val = nullptr;
	REQUIRE(duckdb_v2_bind_arguments_fold(arguments, ctx, 5, &oob_val, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_bind_arguments_get_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_bind_arguments_get_type(nullptr, 0, &oob_type, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_bind_arguments_fold(nullptr, ctx, 0, &oob_val, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_bind_arguments_fold(arguments, nullptr, 0, &oob_val, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

// probe_bind exec: returns arg0 + arg1.
void ProbeExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
               duckdb_v2_error_info_handle *err) {
	IntSumExec(info, nullptr, err);
}

// --- registration helpers ---------------------------------------------------

duckdb_v2_scalar_function_builder_handle NewScalar(duckdb_v2_context_handle ctx, const char *name,
                                                   duckdb_v2_error_info_handle *err) {
	duckdb_v2_scalar_function_builder_handle b = nullptr;
	REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &b, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_builder_set_name(b, V2Str(name), err) == DUCKDB_V2_ERROR_NONE);
	return b;
}

} // namespace

// ---------------------------------------------------------------------------
// ANY construction and text round-trip.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: ANY is constructible via from_id", "[capi_v2][varargs]") {
	duckdb_v2_logical_type_handle any = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_ANY, &any, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(any != nullptr);

	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(any, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_ANY);

	// to_text yields "ANY".
	char *text = nullptr;
	REQUIRE(duckdb_v2_logical_type_to_text(any, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(text) == "ANY");
	free(text);
	duckdb_v2_logical_type_destroy(&any);

	// The remaining bind-time-only ids and TYPE / INVALID stay rejected.
	duckdb_v2_logical_type_handle rejected = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL, &rejected, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN, &rejected, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_TYPE, &rejected, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INVALID, &rejected, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(rejected == nullptr);
}

TEST_CASE("V2 varargs: from_text cannot parse ANY back", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_logical_type_handle t = nullptr;
	V2WithContext(fix.conn, [&](duckdb_v2_context_handle ctx) {
		rc = duckdb_v2_logical_type_create_from_text(ctx, V2Str("ANY"), &t, nullptr);
	});
	// ANY renders as text but is not a parseable SQL type.
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(t == nullptr);
}

// ---------------------------------------------------------------------------
// ANY gates on data-creating surfaces (context-free).
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: ANY rejected by value / data_chunk creation", "[capi_v2][varargs]") {
	auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);

	// data_chunk_create
	duckdb_v2_logical_type_handle types[] = {any};
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(chunk == nullptr);

	// value_create_null
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_null(any, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	// value_create_from_data (ANY has no committed leaf layout)
	int32_t payload = 0;
	REQUIRE(duckdb_v2_value_create_from_data(any, &payload, sizeof(payload), &v, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	// value_create_type (no ANY type token either)
	REQUIRE(duckdb_v2_value_create_type(any, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	duckdb_v2_logical_type_destroy(&any);
}

// ---------------------------------------------------------------------------
// ANY gates on the context-scoped registration surfaces.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: ANY rejected by registration surfaces", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

		// A signature ACCEPTS an ANY parameter on every builder.
		duckdb_v2_scalar_function_builder_handle sf = nullptr;
		duckdb_v2_scalar_function_builder_create(ctx, &sf, nullptr);
		duckdb_v2_scalar_function_builder_set_name(sf, V2Str("bad_scalar"), nullptr);
		// ANY parameter accepted; ANY return type rejected (IsComplete gate) at register.
		V2ScalarSignature(sf, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", any);
			V2SigReturn(sig, any);
		});
		duckdb_v2_scalar_function_builder_set_exec_callback(sf, TwoAnyExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, sf, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_scalar_function_builder_destroy(&sf);

		// aggregate return type ANY rejected at register.
		duckdb_v2_aggregate_function_builder_handle af = nullptr;
		duckdb_v2_aggregate_function_builder_create(ctx, &af, nullptr);
		duckdb_v2_aggregate_function_builder_set_name(af, V2Str("bad_agg"), nullptr);
		V2AggregateSignature(af, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", any);
			V2SigReturn(sig, any);
		});
		// dummy callbacks so we reach the return-type gate
		duckdb_v2_aggregate_function_builder_set_size_callback(
		    af,
		    [](duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
			    duckdb_v2_aggregate_function_size_set_size(info, 8, err);
		    },
		    nullptr);
		duckdb_v2_aggregate_function_builder_set_init_callback(
		    af, [](duckdb_v2_aggregate_function_init_info_handle, duckdb_v2_error_info_handle *) {}, nullptr);
		duckdb_v2_aggregate_function_builder_set_update_callback(
		    af, [](duckdb_v2_aggregate_function_update_info_handle, duckdb_v2_error_info_handle *) {}, nullptr);
		duckdb_v2_aggregate_function_builder_set_combine_callback(
		    af, [](duckdb_v2_aggregate_function_combine_info_handle, duckdb_v2_error_info_handle *) {}, nullptr);
		duckdb_v2_aggregate_function_builder_set_finalize_callback(
		    af, [](duckdb_v2_aggregate_function_finalize_info_handle, duckdb_v2_error_info_handle *) {}, nullptr);
		REQUIRE(duckdb_v2_aggregate_function_builder_register(ctx, af, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_aggregate_function_builder_destroy(&af);

		// cast source / target ANY rejected at register.
		duckdb_v2_cast_function_builder_handle cf = nullptr;
		duckdb_v2_cast_function_builder_create(ctx, &cf, nullptr);
		duckdb_v2_cast_function_builder_set_source_type(cf, any, nullptr);
		duckdb_v2_cast_function_builder_set_target_type(cf, integer, nullptr);
		duckdb_v2_cast_function_builder_set_exec_callback(
		    cf, [](duckdb_v2_cast_function_exec_info_handle, duckdb_v2_error_info_handle *) -> void {}, nullptr);
		REQUIRE(duckdb_v2_cast_function_builder_register(ctx, cf, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_cast_function_builder_destroy(&cf);

		// custom type base ANY rejected at register.
		duckdb_v2_custom_type_builder_handle tb = nullptr;
		duckdb_v2_custom_type_builder_create(ctx, &tb, nullptr);
		duckdb_v2_custom_type_builder_set_name(tb, V2Str("bad_type"), nullptr);
		duckdb_v2_custom_type_builder_set_base_type(tb, any, nullptr);
		REQUIRE(duckdb_v2_custom_type_builder_register(ctx, tb, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_custom_type_builder_destroy(&tb);

		duckdb_v2_logical_type_destroy(&any);
		duckdb_v2_logical_type_destroy(&integer);
	});
}

// ---------------------------------------------------------------------------
// Scalar varargs execution.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: scalar concrete and ANY varargs", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);

		// int_sum: 0 fixed params + INTEGER varargs.
		auto b = NewScalar(ctx, "int_sum", nullptr);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigVarargs(sig, integer);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_exec_callback(b, IntSumExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);

		// prefix_sum: 1 fixed INTEGER param + INTEGER varargs.
		auto p = NewScalar(ctx, "prefix_sum", nullptr);
		V2ScalarSignature(p, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "base", integer);
			V2SigVarargs(sig, integer);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_exec_callback(p, IntSumExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, p, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&p);

		// count_ints: ANY varargs, reads per-column types.
		auto c = NewScalar(ctx, "count_ints", nullptr);
		V2ScalarSignature(c, [&](duckdb_v2_function_signature_handle sig) {
			V2SigVarargs(sig, any);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_exec_callback(c, CountIntsExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, c, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&c);

		// NULL / INVALID varargs type rejected by the signature setter.
		auto bad_sig = V2SigCreate();
		REQUIRE(duckdb_v2_function_signature_set_varargs(bad_sig, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_function_signature_destroy(&bad_sig) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_logical_type_destroy(&integer);
		duckdb_v2_logical_type_destroy(&any);
	});

	auto scalar_i32 = [&](const char *sql) -> int32_t {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fix.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto chunk = V2StepChunk(r);
		REQUIRE(chunk != nullptr);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		int32_t out = static_cast<const int32_t *>(view.data)[view.sel ? view.sel[0] : 0];
		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&r);
		return out;
	};

	// Concrete varargs: extras cast to INTEGER (including a string literal).
	REQUIRE(scalar_i32("SELECT int_sum(1, 2, 3)") == 6);
	REQUIRE(scalar_i32("SELECT int_sum(1, '2', 3)") == 6);
	// Named varargs through SQL: the named extra lands in a varargs slot.
	REQUIRE(scalar_i32("SELECT int_sum(1, 2, x := 3)") == 6);
	// Fixed param plus varargs, and zero extra arguments.
	REQUIRE(scalar_i32("SELECT prefix_sum(10, 1, 2)") == 13);
	REQUIRE(scalar_i32("SELECT prefix_sum(42)") == 42);
	// ANY varargs: mixed types, per-column type inspection.
	REQUIRE(scalar_i32("SELECT count_ints(1::INTEGER, 'x', 2::INTEGER, 3.0::DOUBLE)") == 2);
}

// ---------------------------------------------------------------------------
// ANY varargs with a NULL argument + SPECIAL null handling.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: ANY varargs with NULL and special null handling", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto b = NewScalar(ctx, "count_non_null", nullptr);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigVarargs(sig, any);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_property(b, DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
		                                               DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL, nullptr);
		duckdb_v2_scalar_function_builder_set_exec_callback(b, CountNonNullExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);
		duckdb_v2_logical_type_destroy(&any);
		duckdb_v2_logical_type_destroy(&integer);
	});

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT count_non_null(1, NULL, 3)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	REQUIRE(static_cast<const int32_t *>(view.data)[view.sel ? view.sel[0] : 0] == 2);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&r);
}

// ---------------------------------------------------------------------------
// Fixed-arity ANY parameters.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: fixed-arity ANY parameters", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto b = NewScalar(ctx, "two_any", nullptr);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", any);
			V2SigParam(sig, "b", any);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_exec_callback(b, TwoAnyExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);
		duckdb_v2_logical_type_destroy(&any);
		duckdb_v2_logical_type_destroy(&integer);
	});

	auto call = [&](const char *sql) {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fix.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto chunk = V2StepChunk(r);
		REQUIRE(chunk != nullptr);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		int32_t out = static_cast<const int32_t *>(view.data)[view.sel ? view.sel[0] : 0];
		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&r);
		return out;
	};
	REQUIRE(call("SELECT two_any(1, 'x')") == 42);
	REQUIRE(call("SELECT two_any(3.0, TRUE)") == 42);
}

// ---------------------------------------------------------------------------
// Bind introspection: count, get_type, fold.
// ---------------------------------------------------------------------------
TEST_CASE("V2 varargs: bind argument accessors", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	static ProbeResult probe;
	probe.ran = false;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto b = NewScalar(ctx, "probe_add", nullptr);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", integer);
			V2SigParam(sig, "b", integer);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_scalar_function_builder_set_user_data(b, {&probe, nullptr, nullptr}, nullptr);
		duckdb_v2_scalar_function_builder_set_bind_callback(b, ProbeBind, nullptr);
		duckdb_v2_scalar_function_builder_set_exec_callback(b, ProbeExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);
		duckdb_v2_logical_type_destroy(&integer);
	});

	// arg 0 is a column (non-foldable), arg 1 the literal 7.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT probe_add(x, 7) FROM (VALUES (100)) t(x)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	// probe_add returns arg0 + arg1 = 100 + 7 = 107.
	REQUIRE(static_cast<const int32_t *>(view.data)[view.sel ? view.sel[0] : 0] == 107);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&r);
	REQUIRE(probe.ran);
}

// ---------------------------------------------------------------------------
// Aggregate varargs.
// ---------------------------------------------------------------------------
namespace {

void AggSize(duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
	duckdb_v2_aggregate_function_size_set_size(info, sizeof(int64_t), err);
}
void AggInit(duckdb_v2_aggregate_function_init_info_handle info, duckdb_v2_error_info_handle *err) {
	void *state = nullptr;
	duckdb_v2_aggregate_function_init_get_state(info, &state, err);
	*static_cast<int64_t *>(state) = 0;
}
void AggUpdateSumAll(duckdb_v2_aggregate_function_update_info_handle info, duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle input = nullptr;
	duckdb_v2_aggregate_function_update_get_input(info, &input, err);
	idx_t cols = 0;
	duckdb_v2_data_chunk_get_vector_count(input, &cols, err);
	void **states_raw = nullptr;
	duckdb_v2_aggregate_function_update_get_states(info, &states_raw, err);
	auto states = reinterpret_cast<int64_t **>(states_raw);
	idx_t count = 0;
	duckdb_v2_aggregate_function_update_get_count(info, &count, err);
	for (idx_t c = 0; c < cols; c++) {
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(input, c, &vec, err);
		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, err);
		const auto data = static_cast<const int32_t *>(view.data);
		for (idx_t i = 0; i < count; i++) {
			const idx_t idx = view.sel ? view.sel[i] : i;
			if (RowValid(view, idx)) {
				*states[i] += data[idx];
			}
		}
	}
}
void AggCombine(duckdb_v2_aggregate_function_combine_info_handle info, duckdb_v2_error_info_handle *err) {
	void **sources_raw = nullptr;
	duckdb_v2_aggregate_function_combine_get_sources(info, &sources_raw, err);
	auto sources = reinterpret_cast<int64_t **>(sources_raw);
	void **targets_raw = nullptr;
	duckdb_v2_aggregate_function_combine_get_targets(info, &targets_raw, err);
	auto targets = reinterpret_cast<int64_t **>(targets_raw);
	idx_t count = 0;
	duckdb_v2_aggregate_function_combine_get_count(info, &count, err);
	for (idx_t i = 0; i < count; i++) {
		*targets[i] += *sources[i];
	}
}
void AggFinalize(duckdb_v2_aggregate_function_finalize_info_handle info, duckdb_v2_error_info_handle *err) {
	void **states_raw = nullptr;
	duckdb_v2_aggregate_function_finalize_get_states(info, &states_raw, err);
	auto states = reinterpret_cast<int64_t **>(states_raw);
	duckdb_v2_vector_handle result_vec = nullptr;
	duckdb_v2_aggregate_function_finalize_get_result(info, &result_vec, err);
	int64_t *out = nullptr;
	duckdb_v2_vector_get_data_mutable(result_vec, reinterpret_cast<void **>(&out), err);
	idx_t count = 0;
	duckdb_v2_aggregate_function_finalize_get_count(info, &count, err);
	idx_t result_offset = 0;
	duckdb_v2_aggregate_function_finalize_get_result_offset(info, &result_offset, err);
	for (idx_t i = 0; i < count; i++) {
		out[result_offset + i] = *states[i];
	}
}

void RegisterAgg(duckdb_v2_context_handle ctx, const char *name,
                 duckdb_v2_aggregate_function_bind_callback_fn bind_cb) {
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto bigint = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_aggregate_function_builder_handle b = nullptr;
	duckdb_v2_aggregate_function_builder_create(ctx, &b, nullptr);
	duckdb_v2_aggregate_function_builder_set_name(b, V2Str(name), nullptr);
	V2AggregateSignature(b, [&](duckdb_v2_function_signature_handle sig) {
		V2SigVarargs(sig, integer);
		V2SigReturn(sig, bigint);
	});
	duckdb_v2_aggregate_function_builder_set_size_callback(b, AggSize, nullptr);
	duckdb_v2_aggregate_function_builder_set_init_callback(b, AggInit, nullptr);
	duckdb_v2_aggregate_function_builder_set_update_callback(b, AggUpdateSumAll, nullptr);
	duckdb_v2_aggregate_function_builder_set_combine_callback(b, AggCombine, nullptr);
	duckdb_v2_aggregate_function_builder_set_finalize_callback(b, AggFinalize, nullptr);
	if (bind_cb) {
		duckdb_v2_aggregate_function_builder_set_bind_callback(b, bind_cb, nullptr);
	}
	REQUIRE(duckdb_v2_aggregate_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_aggregate_function_builder_destroy(&b);
	duckdb_v2_logical_type_destroy(&integer);
	duckdb_v2_logical_type_destroy(&bigint);
}

int64_t AggResult(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	int64_t out = static_cast<const int64_t *>(view.data)[view.sel ? view.sel[0] : 0];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&r);
	return out;
}

} // namespace

TEST_CASE("V2 varargs: aggregate varargs", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) { RegisterAgg(ctx, "multi_sum", nullptr); });

	// multi_sum sums every varargs column across every row: 1+2+3+4 = 10.
	REQUIRE(AggResult(fix.conn, "SELECT multi_sum(a, b) FROM (VALUES (1,2),(3,4)) t(a,b)") == 10);
}

// ---------------------------------------------------------------------------
// Table function varargs + positional parameter count.
// ---------------------------------------------------------------------------
namespace {

struct TfState {
	idx_t param_count = 0;
	bool emitted = false;
};

void TfBind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle,
            duckdb_v2_error_info_handle *err) {
	idx_t count = 0;
	REQUIRE(duckdb_v2_table_function_bind_get_parameter_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("n"), integer, err);
	duckdb_v2_logical_type_destroy(&integer);
	auto *bind = new TfState();
	bind->param_count = count;
	duckdb_v2_table_function_bind_set_bind_data(
	    info, {bind, [](void *p) { delete static_cast<TfState *>(p); }, nullptr}, err);
}

void TfInitGlobal(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle,
                  duckdb_v2_error_info_handle *err) {
	auto *state = new TfState();
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<TfState *>(p); }, nullptr}, err);
}

void TfExec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
            duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);
	void *global = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global, err);
	auto &gstate = *static_cast<TfState *>(global);
	void *bind = nullptr;
	duckdb_v2_table_function_exec_get_bind_data(info, &bind, err);
	auto &bstate = *static_cast<TfState *>(bind);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	if (gstate.emitted) {
		duckdb_v2_vector_set_size(vec, 0, err);
		return;
	}
	int32_t *out = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&out), err);
	out[0] = static_cast<int32_t>(bstate.param_count);
	gstate.emitted = true;
	duckdb_v2_vector_set_size(vec, 1, err);
}

} // namespace

TEST_CASE("V2 varargs: table function varargs + parameter count", "[capi_v2][varargs]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		duckdb_v2_table_function_builder_handle b = nullptr;
		duckdb_v2_table_function_builder_create(ctx, &b, nullptr);
		duckdb_v2_table_function_builder_set_name(b, V2Str("tf_count"), nullptr);
		V2TableSignature(b, [&](duckdb_v2_function_signature_handle sig) { V2SigVarargs(sig, integer); });
		duckdb_v2_table_function_builder_set_bind_callback(b, TfBind, nullptr);
		duckdb_v2_table_function_builder_set_init_global_callback(b, TfInitGlobal, nullptr);
		duckdb_v2_table_function_builder_set_exec_callback(b, TfExec, nullptr);
		REQUIRE(duckdb_v2_table_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_table_function_builder_destroy(&b);
		duckdb_v2_logical_type_destroy(&integer);
	});

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT n FROM tf_count(10, 20, 30)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 1);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	REQUIRE(static_cast<const int32_t *>(view.data)[view.sel ? view.sel[0] : 0] == 3);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&r);
}
