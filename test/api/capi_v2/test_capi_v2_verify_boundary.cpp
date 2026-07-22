#include "capi_v2_test_helpers.hpp"
#include "duckdb/main/config.hpp"

#include <string>

// ---------------------------------------------------------------------------
// Boundary verification probes.
//
// The engine requires that a NULL STRUCT/ARRAY row has only NULL descendant
// slots (nested NULL invariant, vector.yaml module header). The bridge runs
// the engine chunk verifier on user-written chunks at the two boundaries the
// engine itself does not cover: the table-function exec output and
// column_data_collection_append. The verifier is a no-op unless
// debug_verification_mode='verify_vectors'; under the mode a violating chunk
// is rejected with INVALID_INPUT at the boundary instead of reaching engine
// consumers that trust child validity masks.
//
// The violating chunks are mask-only violations: real string payloads under
// a raw-masked NULL parent. Planting garbage payloads instead would be
// undefined behavior wherever the verifier is off, so the probes stay
// mask-only and run in every configuration.
// ---------------------------------------------------------------------------

namespace {

// Scoped verify_vectors: sets the process-global mode, restores NONE on exit.
struct VerifyVectorsGuard {
	VerifyVectorsGuard() {
		duckdb::DBConfigOptions::global_verification_mode = duckdb::DebugVerificationMode::VERIFY_VECTORS;
	}
	~VerifyVectorsGuard() {
		duckdb::DBConfigOptions::global_verification_mode = duckdb::DebugVerificationMode::NONE;
	}
};

struct EmitOnceState {
	bool emitted = false;
};

void struct_fn_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	auto vtype = V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_str names[1] = {V2Str("v")};
	duckdb_v2_logical_type_handle struct_type = nullptr;
	duckdb_v2_logical_type_create(ctx, V2Str("struct"), names, &vtype, 1, &struct_type, err);
	duckdb_v2_value_destroy(&vtype);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("s"), struct_type, err);
	duckdb_v2_logical_type_destroy(&struct_type);
}

void emit_once_init(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	auto *state = new EmitOnceState();
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<EmitOnceState *>(p); }, nullptr}, err);
}

// Emits one 2-row STRUCT(v VARCHAR) chunk with row 0 NULL. `violate` nulls
// only the parent row via the raw mask (the child slot stays valid);
// otherwise the NULL goes through vector_set_null, which recurses.
void EmitStructChunk(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_error_info_handle *err, bool violate) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);
	auto &state = *static_cast<EmitOnceState *>(global_state);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	if (state.emitted) {
		duckdb_v2_vector_set_size(vec, 0, err);
		return;
	}
	state.emitted = true;

	duckdb_v2_vector_set_size(vec, 2, err);
	duckdb_v2_vector_handle child = nullptr;
	duckdb_v2_vector_get_child(vec, 0, &child, err);
	V2VectorAssignString(child, 0, "zero", 4, err);
	V2VectorAssignString(child, 1, "one", 3, err);

	if (violate) {
		uint64_t *validity = nullptr;
		duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, err);
		validity[0] &= ~uint64_t(1);
	} else {
		duckdb_v2_vector_set_null(vec, 0, err);
	}
}

void bad_struct_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                     duckdb_v2_error_info_handle *err) {
	EmitStructChunk(info, err, true);
}

void good_struct_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	EmitStructChunk(info, err, false);
}

void RegisterStructFn(duckdb_v2_context_handle ctx, const char *name, duckdb_v2_table_function_exec_fn exec,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_table_function_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str(name), err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, struct_fn_bind, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, emit_once_init, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, exec, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_builder_destroy(&builder);
}

// Steps a result until it errors; FAILs if it finishes cleanly. Returns the
// error code and fills `message` from the err info.
DUCKDB_V2_ERROR StepUntilError(duckdb_v2_result_handle result, std::string &message) {
	duckdb_v2_error_info_handle err = nullptr;
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(result, &chunk, &status, &err);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			FAIL("result finished without the expected error");
		}
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			duckdb_v2_result_wait(result, nullptr);
		}
	}
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	message = V2StrTo(msg);
	duckdb_v2_error_info_destroy(&err);
	return rc;
}

// Builds a 2-row STRUCT(v VARCHAR) chunk, row 0 NULL, violating or
// set_null-clean as in EmitStructChunk. Caller destroys the chunk.
duckdb_v2_data_chunk_handle BuildStructChunk(duckdb_v2_logical_type_handle struct_type, bool violate) {
	duckdb_v2_logical_type_handle types[1] = {struct_type};
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle child = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2VectorAssignString(child, 0, "zero", 4, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2VectorAssignString(child, 1, "one", 3, nullptr) == DUCKDB_V2_ERROR_NONE);

	if (violate) {
		uint64_t *validity = nullptr;
		REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, nullptr) == DUCKDB_V2_ERROR_NONE);
		validity[0] &= ~uint64_t(1);
	} else {
		REQUIRE(duckdb_v2_vector_set_null(vec, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	return chunk;
}

} // namespace

TEST_CASE("V2 verify boundary: table function output", "[capi_v2][verify_boundary]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    RegisterStructFn(ctx, "bad_struct", bad_struct_exec, err);
		    RegisterStructFn(ctx, "good_struct", good_struct_exec, err);
	    },
	    nullptr, nullptr);

	SECTION("violating chunk is rejected under verify_vectors") {
		VerifyVectorsGuard guard;
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT * FROM bad_struct()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		std::string message;
		REQUIRE(StepUntilError(result, message) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(message.find("table function exec callback") != std::string::npos);
		REQUIRE(message.find("chunk failed vector verification") != std::string::npos);
		REQUIRE(message.find("Struct NULL mismatch") != std::string::npos);

		duckdb_v2_result_destroy(&result);
	}

	SECTION("set_null-written chunk passes under verify_vectors") {
		VerifyVectorsGuard guard;
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT * FROM good_struct()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		auto chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);
		idx_t size = 0;
		REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(size == 2);

		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view {};
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(RowValid(view, view.sel ? view.sel[0] : 0));
		REQUIRE(RowValid(view, view.sel ? view.sel[1] : 1));

		duckdb_v2_data_chunk_destroy(&chunk);
		REQUIRE(V2StepChunk(result) == nullptr);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("set_null-written chunk passes under the default mode") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT count(*) FROM good_struct()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2DrainRowCount(result) == 1);
		duckdb_v2_result_destroy(&result);
	}
}

TEST_CASE("V2 verify boundary: column_data_collection_append", "[capi_v2][verify_boundary]") {
	V2EnvFixture fix;
	auto struct_type = V2StructType(fix.conn, {"v"}, {DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});

	duckdb_v2_column_data_collection_handle cdc = nullptr;
	V2WithContext(fix.conn, [&](duckdb_v2_context_handle ctx) {
		duckdb_v2_logical_type_handle types[1] = {struct_type};
		REQUIRE(duckdb_v2_column_data_collection_create(ctx, types, 1, &cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	});
	duckdb_v2_column_data_collection_append_state_handle append_state = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &append_state, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("violating chunk is rejected under verify_vectors") {
		VerifyVectorsGuard guard;
		auto chunk = BuildStructChunk(struct_type, true);

		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, append_state, chunk, &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		auto message = V2StrTo(msg);
		REQUIRE(message.find("duckdb_v2_column_data_collection_append") != std::string::npos);
		REQUIRE(message.find("Struct NULL mismatch") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);

		duckdb_v2_data_chunk_destroy(&chunk);
	}

	SECTION("set_null-written chunk passes under verify_vectors") {
		VerifyVectorsGuard guard;
		auto chunk = BuildStructChunk(struct_type, false);
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, append_state, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_chunk_destroy(&chunk);
	}

	SECTION("set_null-written chunk passes under the default mode") {
		auto chunk = BuildStructChunk(struct_type, false);
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, append_state, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_chunk_destroy(&chunk);
	}

	duckdb_v2_column_data_collection_append_state_destroy(&append_state);
	duckdb_v2_column_data_collection_destroy(&cdc);
	duckdb_v2_logical_type_destroy(&struct_type);
}
