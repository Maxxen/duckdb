#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include <cstring>

namespace {

struct CreateExtensionState {
	bool called = false;
	bool handles_ok = false;
};

void RecordingInit(duckdb_v2_extension_handle extension, duckdb_v2_context_handle context, void *user_data,
                   duckdb_v2_error_info_handle *err) {
	auto &state = *static_cast<CreateExtensionState *>(user_data);
	state.called = true;
	state.handles_ok = extension != nullptr && context != nullptr && err != nullptr && *err != nullptr;
}

void FailingInit(duckdb_v2_extension_handle extension, duckdb_v2_context_handle context, void *user_data,
                 duckdb_v2_error_info_handle *err) {
	(void)extension;
	(void)context;
	(void)user_data;
	REQUIRE(duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str message {"init failed on purpose", 22};
	REQUIRE(duckdb_v2_error_info_set_text(*err, message) == DUCKDB_V2_ERROR_NONE);
}

} // namespace

// ===========================================================================
// get_class
// ===========================================================================

TEST_CASE("V2 extension: connection defined", "[capi_v2][extension]") {
	V2EnvFixture fixture;
	auto conn = fixture.conn;

	CreateExtensionState state;
	REQUIRE(duckdb_v2_connection_create_extension(conn, V2Str("test_extension"), RecordingInit, &state, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(state.called);
	REQUIRE(state.handles_ok);

	auto found = QuerySingleVarchar(
	    conn, "SELECT extension_name FROM duckdb_extensions() WHERE extension_name = 'test_extension'");
	REQUIRE(found == "test_extension");

	// A second extension with the same name is rejected
	REQUIRE(duckdb_v2_connection_create_extension(conn, V2Str("test_extension"), RecordingInit, &state, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 extension: callback error abandons the creation", "[capi_v2][extension]") {
	V2EnvFixture fixture;
	auto conn = fixture.conn;

	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_connection_create_extension(conn, V2Str("failing_ext"), FailingInit, nullptr, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_str text {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &text) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(text.ptr != nullptr);
	REQUIRE(std::strlen(text.ptr) == text.len);
	duckdb_v2_error_info_destroy(&err);

	// Null callback is rejected
	REQUIRE(duckdb_v2_connection_create_extension(conn, V2Str("x"), nullptr, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}
