#include "capi_v2_test_helpers.hpp"
#include "capi_v2_internal.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>

// ---------------------------------------------------------------------------
// V2 identifier alias: duckdb_v2_identifier_t is a weak typedef of
// duckdb_v2_str (same layout, same calling convention), marking a string the
// engine treats as a SQL identifier. These tests pin the alias identity and
// that a duckdb_v2_str value built the usual V2 way interchanges freely at a
// re-typed call site.
// ---------------------------------------------------------------------------

static_assert(std::is_same<duckdb_v2_identifier_t, duckdb_v2_str>::value,
              "duckdb_v2_identifier_t must be a weak alias of duckdb_v2_str");

TEST_CASE("V2 identifier: str and identifier interchange at a re-typed call site", "[capi_v2][identifier]") {
	// duckdb_v2_option_create.name is now duckdb_v2_identifier_t; option_get_name.out_name
	// likewise. Build the input via V2Str (a duckdb_v2_str value) and round-trip it through
	// both to prove the two types are freely interchangeable.
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_identifier_t out_name = {nullptr, 0};
	REQUIRE(duckdb_v2_option_get_name(opt, &out_name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_name == "memory_limit");
	// Borrowed, null-terminated view: strlen and the reported length agree.
	REQUIRE(out_name.ptr != nullptr);
	REQUIRE(std::strlen(out_name.ptr) == out_name.len);

	duckdb_v2_option_destroy(&opt);
}

// Render name through the identifier-quoting entry point and return the owned result.
static std::string RenderQuoted(const char *name) {
	char *out = nullptr;
	REQUIRE(duckdb_v2_identifier_render_quoted(V2Str(name), &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out != nullptr);
	std::string result(out);
	std::free(out);
	return result;
}

TEST_CASE("V2 identifier: render_quoted quotes and escapes only when required", "[capi_v2][identifier]") {
	// A legal bare identifier is returned verbatim.
	REQUIRE(RenderQuoted("col") == "col");
	// Casing is preserved and does not force quoting.
	REQUIRE(RenderQuoted("MyCol") == "MyCol");
	// A reserved keyword is double-quoted.
	REQUIRE(RenderQuoted("select") == "\"select\"");
	// A name with a space requires quoting.
	REQUIRE(RenderQuoted("my col") == "\"my col\"");
	// Interior double quotes are escaped by doubling, inside quotes.
	REQUIRE(RenderQuoted("a\"b") == "\"a\"\"b\"");

	// An empty view is a valid (empty) name and renders without error.
	char *empty_out = nullptr;
	REQUIRE(duckdb_v2_identifier_render_quoted(V2Str(""), &empty_out, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::free(empty_out);

	// A null out pointer is rejected; the return code is authoritative.
	REQUIRE(duckdb_v2_identifier_render_quoted(V2Str("x"), nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
}
