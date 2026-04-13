#include "catch.hpp"
#include "capi_v2_internal.hpp"

TEST_CASE("V2: smoke test - open and close database", "[capi_v2]") {
	duckdb_v2_database_ptr db = nullptr;
	auto rc = duckdb_v2_open(nullptr, nullptr, &db);
	// Stubs return DUCKDB_V2_API_ERROR (not yet implemented)
	REQUIRE(rc == DUCKDB_V2_API_ERROR);
}
