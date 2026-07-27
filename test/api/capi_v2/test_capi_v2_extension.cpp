#include "catch.hpp"
#include "capi_v2_internal.hpp"

namespace {} // namespace

// ===========================================================================
// get_class
// ===========================================================================

TEST_CASE("V2 extension: connection defined", "[capi_v2][extension]") {
	V2EnvFixture fixture;
	auto conn = fixture.conn;

	REQUIRE(duckdb_v2_connection_create_extension(conn, V2Str("test_extension"), nullptr) == DUCKDB_V2_ERROR_NONE);

	auto found = QuerySingleVarchar(
	    conn, "SELECT extension_name FROM duckdb_extensions() WHERE extension_name = 'test_extension'");
	REQUIRE(found == "test_extension");
}
