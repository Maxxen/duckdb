#include "capi_v2_test_helpers.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 custom cast functions
//
// Registers a custom type TEMPERATURE (an alias of INTEGER) and two casts:
//   - TEMPERATURE -> VARCHAR : formats the integer as "<n>C"
//   - VARCHAR -> TEMPERATURE : parses "<n>C" back into an integer, failing
//                              (with try/strict semantics) on malformed input
// ---------------------------------------------------------------------------

namespace {

// Parse a string of the form "<digits>C" into an int32. Returns false on any malformed input.
bool ParseTemperature(const char *data, idx_t len, int32_t &out) {
	if (len < 2 || data[len - 1] != 'C') {
		return false;
	}
	int64_t value = 0;
	bool negative = false;
	idx_t i = 0;
	if (data[0] == '-') {
		negative = true;
		i = 1;
	}
	if (i == len - 1) {
		return false; // no digits before the 'C'
	}
	for (; i < len - 1; i++) {
		if (data[i] < '0' || data[i] > '9') {
			return false;
		}
		value = value * 10 + (data[i] - '0');
	}
	out = static_cast<int32_t>(negative ? -value : value);
	return true;
}

// TEMPERATURE -> VARCHAR. Infallible; just formats every (valid) row.
void TemperatureToVarchar(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_error_info_handle *err) {
	duckdb_v2_vector_handle input = nullptr;
	REQUIRE(duckdb_v2_cast_function_exec_get_input(info, &input, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle output = nullptr;
	REQUIRE(duckdb_v2_cast_function_exec_get_output(info, &output, err) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_cast_function_exec_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view;
	REQUIRE(duckdb_v2_vector_get_view(input, &view, err) == DUCKDB_V2_ERROR_NONE);
	auto *in = static_cast<const int32_t *>(view.data);

	for (idx_t i = 0; i < count; i++) {
		idx_t idx = SelAt(view.sel, i);
		if (!RowValid(view, idx)) {
			uint64_t *validity = nullptr;
			REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(output, &validity, err) == DUCKDB_V2_ERROR_NONE);
			validity[i / 64] &= ~(UINT64_C(1) << (i % 64));
			continue;
		}
		std::string formatted = std::to_string(in[idx]) + "C";
		REQUIRE(V2VectorAssignString(output, i, formatted.c_str(), formatted.size(), err) == DUCKDB_V2_ERROR_NONE);
	}
}

// VARCHAR -> TEMPERATURE. Fails on malformed input: in a TRY cast the failing rows are set to NULL,
// in a strict cast the reported error aborts the query. Also exercises user_data retrieval.
void VarcharToTemperature(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_error_info_handle *err) {
	// Verify the user data made it through.
	void *user_data = nullptr;
	REQUIRE(duckdb_v2_cast_function_exec_get_user_data(info, &user_data, err) == DUCKDB_V2_ERROR_NONE);
	auto *secret = static_cast<std::string *>(user_data);
	if (!secret || *secret != "secret") {
		duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_error_info_set_text(*err, V2Str("user data was not threaded through to the cast callback"));
		return;
	}

	duckdb_v2_vector_handle input = nullptr;
	REQUIRE(duckdb_v2_cast_function_exec_get_input(info, &input, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_handle output = nullptr;
	REQUIRE(duckdb_v2_cast_function_exec_get_output(info, &output, err) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_cast_function_exec_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view;
	REQUIRE(duckdb_v2_vector_get_view(input, &view, err) == DUCKDB_V2_ERROR_NONE);
	auto *in = static_cast<const duckdb_v2_varchar_t *>(view.data);

	void *out_ptr = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(output, &out_ptr, err) == DUCKDB_V2_ERROR_NONE);
	auto *out = static_cast<int32_t *>(out_ptr);

	for (idx_t i = 0; i < count; i++) {
		idx_t idx = SelAt(view.sel, i);
		if (!RowValid(view, idx)) {
			uint64_t *validity = nullptr;
			REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(output, &validity, err) == DUCKDB_V2_ERROR_NONE);
			validity[i / 64] &= ~(UINT64_C(1) << (i % 64));
			continue;
		}

		duckdb_v2_str bytes = V2StringView(in[idx]);

		int32_t parsed = 0;
		if (!ParseTemperature(bytes.ptr, bytes.len, parsed)) {
			// Mark the row NULL (used by TRY_CAST) and report the error (used by a strict CAST).
			uint64_t *validity = nullptr;
			REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(output, &validity, err) == DUCKDB_V2_ERROR_NONE);
			validity[i / 64] &= ~(UINT64_C(1) << (i % 64));

			std::string message = "Could not convert '" + V2StrTo(bytes) + "' to TEMPERATURE";
			duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_TYPE_CONVERSION);
			duckdb_v2_error_info_set_text(*err, V2Str(message));
			continue;
		}
		out[i] = parsed;
	}
}

// Registers the TEMPERATURE custom type and both casts within the connection's transaction context.
void RegisterTemperature(duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle int_type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, err) ==
	        DUCKDB_V2_ERROR_NONE);

	// Register the custom type TEMPERATURE as an alias of INTEGER.
	duckdb_v2_custom_type_builder_handle type_builder = nullptr;
	REQUIRE(duckdb_v2_custom_type_builder_create(ctx, &type_builder, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_custom_type_builder_set_name(type_builder, V2Str("TEMPERATURE"), err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_custom_type_builder_set_base_type(type_builder, int_type, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_custom_type_builder_register(ctx, type_builder, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_custom_type_builder_destroy(&type_builder);

	// Build a TEMPERATURE logical type handle to use as the cast source/target.
	duckdb_v2_logical_type_handle temperature_type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_with_alias(int_type, V2Str("TEMPERATURE"), &temperature_type, err) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_handle varchar_type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, err) ==
	        DUCKDB_V2_ERROR_NONE);

	// TEMPERATURE -> VARCHAR
	duckdb_v2_cast_function_builder_handle to_varchar = nullptr;
	REQUIRE(duckdb_v2_cast_function_builder_create(ctx, &to_varchar, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_source_type(to_varchar, temperature_type, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_target_type(to_varchar, varchar_type, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_implicit_cast_cost(to_varchar, 0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_exec_callback(to_varchar, TemperatureToVarchar, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_register(ctx, to_varchar, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_destroy(&to_varchar) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(to_varchar == nullptr);

	// VARCHAR -> TEMPERATURE (with user data)
	duckdb_v2_cast_function_builder_handle from_varchar = nullptr;
	REQUIRE(duckdb_v2_cast_function_builder_create(ctx, &from_varchar, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_source_type(from_varchar, varchar_type, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_target_type(from_varchar, temperature_type, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_implicit_cast_cost(from_varchar, 0, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_set_exec_callback(from_varchar, VarcharToTemperature, err) ==
	        DUCKDB_V2_ERROR_NONE);

	auto *secret = new std::string("secret");
	auto destroy = [](void *data) {
		delete static_cast<std::string *>(data);
	};
	REQUIRE(duckdb_v2_cast_function_builder_set_user_data(from_varchar, {secret, destroy, nullptr}, err) ==
	        DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_cast_function_builder_register(ctx, from_varchar, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_builder_destroy(&from_varchar) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_destroy(&temperature_type);
	duckdb_v2_logical_type_destroy(&varchar_type);
	duckdb_v2_logical_type_destroy(&int_type);
}

// Helper: run a query and return the single int32 result (asserting it is non-NULL).
int32_t QuerySingleInt(duckdb_v2_connection_handle conn, const std::string &sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, sql.c_str(), &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view;
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 1);
	REQUIRE(RowValid(view, SelAt(view.sel, 0)));
	int32_t value = static_cast<const int32_t *>(view.data)[SelAt(view.sel, 0)];

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	return value;
}

// Helper: run a query and return the single VARCHAR result (asserting it is non-NULL).
std::string QuerySingleVarchar(duckdb_v2_connection_handle conn, const std::string &sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, sql.c_str(), &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view;
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 1);
	REQUIRE(RowValid(view, SelAt(view.sel, 0)));

	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);
	std::string value = V2StrTo(V2StringView(arr[SelAt(view.sel, 0)]));

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	return value;
}

} // namespace

TEST_CASE("V2 cast: custom type round-trip casts", "[capi_v2][cast]") {
	V2EnvFixture fixture;
	auto conn = fixture.conn;

	REQUIRE(duckdb_v2_connection_execute_with_context(conn, RegisterTemperature, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("TEMPERATURE -> VARCHAR") {
		REQUIRE(QuerySingleVarchar(conn, "SELECT CAST(CAST(42 AS TEMPERATURE) AS VARCHAR)") == "42C");
		REQUIRE(QuerySingleVarchar(conn, "SELECT CAST(CAST(-7 AS TEMPERATURE) AS VARCHAR)") == "-7C");
	}

	SECTION("VARCHAR -> TEMPERATURE") {
		REQUIRE(QuerySingleInt(conn, "SELECT CAST(CAST('100C' AS TEMPERATURE) AS INTEGER)") == 100);
		REQUIRE(QuerySingleInt(conn, "SELECT CAST(CAST('-5C' AS TEMPERATURE) AS INTEGER)") == -5);
	}

	SECTION("NULL propagation") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(conn, "SELECT CAST(CAST(NULL AS TEMPERATURE) AS VARCHAR) IS NULL", &result, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_chunk_handle chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view;
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(static_cast<const bool *>(view.data)[SelAt(view.sel, 0)] == true);
		REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("strict cast failure aborts the query") {
		duckdb_v2_result_handle result = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		// Lazy streaming: the query prepares fine; the strict-cast error only
		// surfaces once the stream is stepped.
		REQUIRE(V2Query(conn, "SELECT CAST('not-a-temp' AS TEMPERATURE)", &result, &err) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
		while (true) {
			duckdb_v2_data_chunk_handle chunk = nullptr;
			DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
			rc = duckdb_v2_result_step(result, &chunk, &status, &err);
			if (rc != DUCKDB_V2_ERROR_NONE || status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
				break;
			}
			if (chunk) {
				duckdb_v2_data_chunk_destroy(&chunk);
			}
		}
		REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
		duckdb_v2_error_info_destroy(&err);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("TRY_CAST failure yields NULL") {
		REQUIRE(QuerySingleVarchar(conn, "SELECT CASE WHEN TRY_CAST('not-a-temp' AS TEMPERATURE) IS NULL THEN 'null' "
		                                 "ELSE 'value' END") == "null");
	}
}

TEST_CASE("V2 cast: builder validation errors", "[capi_v2][cast]") {
	V2EnvFixture fixture;
	auto conn = fixture.conn;

	auto check = [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		// create requires a non-null context and out pointer
		REQUIRE(duckdb_v2_cast_function_builder_create(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

		duckdb_v2_logical_type_handle int_type = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, err) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_logical_type_handle varchar_type = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, err) ==
		        DUCKDB_V2_ERROR_NONE);

		duckdb_v2_cast_function_builder_handle builder = nullptr;
		REQUIRE(duckdb_v2_cast_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		// register without source/target/exec set -> error (no err out param to keep it simple)
		REQUIRE(duckdb_v2_cast_function_builder_register(ctx, builder, nullptr) != DUCKDB_V2_ERROR_NONE);

		REQUIRE(duckdb_v2_cast_function_builder_set_source_type(builder, int_type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_cast_function_builder_register(ctx, builder, nullptr) != DUCKDB_V2_ERROR_NONE);

		REQUIRE(duckdb_v2_cast_function_builder_set_target_type(builder, varchar_type, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		// still missing the exec callback
		REQUIRE(duckdb_v2_cast_function_builder_register(ctx, builder, nullptr) != DUCKDB_V2_ERROR_NONE);

		duckdb_v2_cast_function_builder_destroy(&builder);
		duckdb_v2_logical_type_destroy(&int_type);
		duckdb_v2_logical_type_destroy(&varchar_type);
	};
	REQUIRE(duckdb_v2_connection_execute_with_context(conn, check, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);

	// null-safe destroy
	REQUIRE(duckdb_v2_cast_function_builder_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
}
