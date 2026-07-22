#include "capi_v2_test_helpers.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 scalar function builder (C surface). The C++ wrapper has its own suite
// in test_cpp_api_function.cpp.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Scalar Function API
// ---------------------------------------------------------------------------
TEST_CASE("V2 scalar: create / destroy", "[capi_v2][scalar]") {
	// Setup a minimal environment + database so we can get a connection handle for the
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);

	// Function callbacks
	static auto bind_callback = [](duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_context_handle context,
	                               duckdb_v2_error_info_handle *err) {
		/* TODO */
	};

	static auto init_callback = [](duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_context_handle context,
	                               duckdb_v2_error_info_handle *err) {
		/* TODO */
	};

	static auto exec_callback = [](duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
	                               duckdb_v2_error_info_handle *err) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &chunk, err) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_handle lhs_vec = nullptr;
		duckdb_v2_vector_handle rhs_vec = nullptr;
		duckdb_v2_vector_handle out_vec = nullptr;
		REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &out_vec, err) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &lhs_vec, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &rhs_vec, err) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_view lhs_view;
		duckdb_v2_vector_view rhs_view;

		REQUIRE(duckdb_v2_vector_get_view(lhs_vec, &lhs_view, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_get_view(rhs_vec, &rhs_view, err) == DUCKDB_V2_ERROR_NONE);

		const auto lhs_data = static_cast<const int32_t *>(lhs_view.data);
		const auto rhs_data = static_cast<const int32_t *>(rhs_view.data);

		int32_t *out_data = nullptr;
		REQUIRE(duckdb_v2_vector_get_data_mutable(out_vec, (void **)&out_data, err) == DUCKDB_V2_ERROR_NONE);

		for (idx_t i = 0; i < lhs_view.count; i++) {
			const auto lhs_idx = lhs_view.sel ? lhs_view.sel[i] : i;
			const auto rhs_idx = rhs_view.sel ? rhs_view.sel[i] : i;

			out_data[i] = lhs_data[lhs_idx] + rhs_data[rhs_idx];
		}
	};

	SECTION("Basic registration and cleanup") {
		// Run in transaction to get a context we can register within

		duckdb_v2_connection_execute_with_context(
		    conn,
		    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
			    duckdb_v2_scalar_function_builder_handle builder = nullptr;
			    REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Make type
			    duckdb_v2_logical_type_handle type = nullptr;
			    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Setup parameters
			    REQUIRE(duckdb_v2_scalar_function_builder_add_parameter(builder, V2Str("a"), type, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_add_parameter(builder, V2Str("b"), type, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Also add return type
			    REQUIRE(duckdb_v2_scalar_function_builder_set_return_type(builder, type, err) == DUCKDB_V2_ERROR_NONE);

			    // Setup function callbacks
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(builder, V2Str("my_func"), err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(builder, bind_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(builder, init_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(builder, exec_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Expected-failure probes opt out of detail (nullptr err): writing
			    // their codes into the callback's slot would fail the whole scope,
			    // since a non-NONE code left on the slot at return is the callback's
			    // failure signal.

			    // Empty name not supported
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(builder, V2Str(""), nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);

			    // Does not work with NULL
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(nullptr, V2Str("my_func"), nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(nullptr, bind_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(nullptr, init_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(nullptr, exec_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);

			    // Register
			    REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Default is ALTER_ON_CONFLICT, and so we can register twice
			    REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Destroy should null out the builder handle and be safe to call twice.
			    REQUIRE(duckdb_v2_scalar_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
			    REQUIRE(builder == nullptr);
			    REQUIRE(duckdb_v2_scalar_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);

			    duckdb_v2_logical_type_destroy(&type);
		    },
		    nullptr, nullptr);
	}

	// Now get the result
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, "SELECT my_func(1, 2)", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	duckdb_v2_vector_handle result_vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &result_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view result_view;
	REQUIRE(duckdb_v2_vector_get_view(result_vec, &result_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_view.count == 1);

	REQUIRE((static_cast<const int32_t *>(result_view.data))[0] == 3);

	// Cleanup
	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}
