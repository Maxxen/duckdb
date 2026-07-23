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

			    // Setup parameters and return type through a signature.
			    V2ScalarSignature(builder, [&](duckdb_v2_function_signature_handle sig) {
				    V2SigParam(sig, "a", type);
				    V2SigParam(sig, "b", type);
				    V2SigReturn(sig, type);
			    });

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
			            DUCKDB_V2_ERROR_INPUT_INVALID);

			    // Does not work with NULL
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(nullptr, V2Str("my_func"), nullptr) ==
			            DUCKDB_V2_ERROR_INPUT_INVALID);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(nullptr, bind_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INPUT_INVALID);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(nullptr, init_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INPUT_INVALID);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(nullptr, exec_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INPUT_INVALID);

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

// ---------------------------------------------------------------------------
// Signature parameter defaults, named-argument resolution, and the
// registration-time Verify() gate (duplicate names, non-trailing defaults).
// ---------------------------------------------------------------------------

namespace {

// Sums the first two input columns per row (handles constant columns, which is
// how a defaulted argument arrives).
void SumTwoExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle input = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &input, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle v0 = nullptr;
	duckdb_v2_vector_handle v1 = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(input, 0, &v0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_chunk_get_vector(input, 1, &v1, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view0;
	duckdb_v2_vector_view view1;
	REQUIRE(duckdb_v2_vector_get_view(v0, &view0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_get_view(v1, &view1, err) == DUCKDB_V2_ERROR_NONE);
	const auto d0 = static_cast<const int32_t *>(view0.data);
	const auto d1 = static_cast<const int32_t *>(view1.data);
	duckdb_v2_vector_handle result_vec = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result_vec, err) == DUCKDB_V2_ERROR_NONE);
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result_vec, reinterpret_cast<void **>(&out), err) ==
	        DUCKDB_V2_ERROR_NONE);
	for (idx_t i = 0; i < view0.count; i++) {
		out[i] = d0[SelAt(view0.sel, i)] + d1[SelAt(view1.sel, i)];
	}
}

// Returns column 0 plus 100 for every row whose column-1 cell is NULL (a
// typed-NULL default arrives as an all-NULL constant column).
void NullDefaultProbeExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                          duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle input = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_input(info, &input, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle v0 = nullptr;
	duckdb_v2_vector_handle v1 = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(input, 0, &v0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_chunk_get_vector(input, 1, &v1, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view0;
	duckdb_v2_vector_view view1;
	REQUIRE(duckdb_v2_vector_get_view(v0, &view0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_get_view(v1, &view1, err) == DUCKDB_V2_ERROR_NONE);
	const auto d0 = static_cast<const int32_t *>(view0.data);
	duckdb_v2_vector_handle result_vec = nullptr;
	REQUIRE(duckdb_v2_scalar_function_exec_get_result(info, &result_vec, err) == DUCKDB_V2_ERROR_NONE);
	int32_t *out = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(result_vec, reinterpret_cast<void **>(&out), err) ==
	        DUCKDB_V2_ERROR_NONE);
	for (idx_t i = 0; i < view0.count; i++) {
		const bool b_null = !RowValid(view1, SelAt(view1.sel, i));
		out[i] = d0[SelAt(view0.sel, i)] + (b_null ? 100 : 0);
	}
}

void NoopExec(duckdb_v2_scalar_function_exec_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}

} // namespace

TEST_CASE("V2 scalar: parameter defaults and named-argument calls", "[capi_v2][scalar][signature]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

		// defsum(a INTEGER, b INTEGER DEFAULT 5) -> a + b
		duckdb_v2_scalar_function_builder_handle b = nullptr;
		REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_set_name(b, V2Str("defsum"), nullptr);
		duckdb_v2_value_handle five = V2Int32Value(5);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", integer);
			V2SigParamDefault(sig, "b", integer, five);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_value_destroy(&five);
		duckdb_v2_scalar_function_builder_set_exec_callback(b, SumTwoExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);

		duckdb_v2_logical_type_destroy(&integer);
	});

	// Omitted default -> b is 5; provided -> the value; named args resolve to the
	// signature parameter names "a" and "b".
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT defsum(10)") == 15);
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT defsum(10, 20)") == 30);
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT defsum(a := 7)") == 12);
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT defsum(a := 7, b := 8)") == 15);
}

TEST_CASE("V2 scalar: a typed-NULL default on an ANY parameter binds and executes", "[capi_v2][scalar][signature]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);

		// null_probe(a INTEGER, b ANY DEFAULT NULL::INTEGER) -> a + (b IS NULL ? 100 : 0)
		duckdb_v2_scalar_function_builder_handle b = nullptr;
		REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_set_name(b, V2Str("null_probe"), nullptr);
		duckdb_v2_value_handle null_int = nullptr;
		REQUIRE(duckdb_v2_value_create_null(integer, &null_int, nullptr) == DUCKDB_V2_ERROR_NONE);
		V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
			V2SigParam(sig, "a", integer);
			V2SigParamDefault(sig, "b", any, null_int);
			V2SigReturn(sig, integer);
		});
		duckdb_v2_value_destroy(&null_int);
		// SPECIAL null handling so exec runs even when the (defaulted) argument is
		// NULL; DEFAULT handling would short-circuit the whole result to NULL.
		duckdb_v2_scalar_function_builder_set_property(b, DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
		                                               DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL, nullptr);
		duckdb_v2_scalar_function_builder_set_exec_callback(b, NullDefaultProbeExec, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&b);

		duckdb_v2_logical_type_destroy(&integer);
		duckdb_v2_logical_type_destroy(&any);
	});

	// Omitting b materializes the typed-NULL default -> +100. Providing a non-NULL
	// b keeps the base value.
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT null_probe(7)") == 107);
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT null_probe(7, 5)") == 7);
	REQUIRE(V2QueryCell<int32_t>(fix.conn, "SELECT null_probe(7, 'x')") == 7);
}

TEST_CASE("V2 scalar: registration rejects a signature that fails Verify()", "[capi_v2][scalar][signature]") {
	V2EnvFixture fix;
	V2WithContext(fix.conn, [](duckdb_v2_context_handle ctx) {
		auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

		// Duplicate parameter names are rejected at register (Verify).
		{
			duckdb_v2_scalar_function_builder_handle b = nullptr;
			REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &b, nullptr) == DUCKDB_V2_ERROR_NONE);
			duckdb_v2_scalar_function_builder_set_name(b, V2Str("dup_names"), nullptr);
			duckdb_v2_scalar_function_builder_set_exec_callback(b, NoopExec, nullptr);
			auto sig = V2SigCreate();
			V2SigParam(sig, "x", integer);
			V2SigParam(sig, "x", integer);
			V2SigReturn(sig, integer);
			REQUIRE(duckdb_v2_scalar_function_builder_set_signature(b, sig, nullptr) == DUCKDB_V2_ERROR_NONE);
			duckdb_v2_function_signature_destroy(&sig);
			REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
			duckdb_v2_scalar_function_builder_destroy(&b);
		}

		// A required parameter following a defaulted one is rejected at register.
		{
			duckdb_v2_scalar_function_builder_handle b = nullptr;
			REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &b, nullptr) == DUCKDB_V2_ERROR_NONE);
			duckdb_v2_scalar_function_builder_set_name(b, V2Str("bad_default_order"), nullptr);
			duckdb_v2_scalar_function_builder_set_exec_callback(b, NoopExec, nullptr);
			duckdb_v2_value_handle five = V2Int32Value(5);
			auto sig = V2SigCreate();
			V2SigParamDefault(sig, "a", integer, five);
			V2SigParam(sig, "b", integer);
			V2SigReturn(sig, integer);
			duckdb_v2_value_destroy(&five);
			REQUIRE(duckdb_v2_scalar_function_builder_set_signature(b, sig, nullptr) == DUCKDB_V2_ERROR_NONE);
			duckdb_v2_function_signature_destroy(&sig);
			REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, b, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
			duckdb_v2_scalar_function_builder_destroy(&b);
		}

		duckdb_v2_logical_type_destroy(&integer);
	});
}
