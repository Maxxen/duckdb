#include "test_capi_v2.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 identifier tests: rendering names into SQL through the engine's own
// quoting rules.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

// Renders a name into SQL text.
std::string RenderQuoted(const char *name, DUCKDB_V2_ERROR &rc) {
	auto view = Convert(name);
	return RenderText([&](duckdb_v2_bytebuf *out) { return duckdb_v2_identifier_render_quoted(&view, out, nullptr); },
	                  rc);
}

std::string RenderQuoted(const char *name) {
	auto rc = DUCKDB_V2_ERROR_NONE;
	auto out = RenderQuoted(name, rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return out;
}

} // namespace

TEST_CASE("V2 identifier: render quotes only when required", "[capi_v2][identifier]") {
	// Legal bare identifiers pass through, casing preserved.
	REQUIRE(RenderQuoted("col") == "col");
	REQUIRE(RenderQuoted("MyCol") == "MyCol");
	REQUIRE(RenderQuoted("a_1") == "a_1");

	// Keywords, spaces, leading digits and embedded quotes are quoted and escaped.
	REQUIRE(RenderQuoted("select") == "\"select\"");
	REQUIRE(RenderQuoted("my col") == "\"my col\"");
	REQUIRE(RenderQuoted("1abc") == "\"1abc\"");
	REQUIRE(RenderQuoted("a\"b") == "\"a\"\"b\"");
}

TEST_CASE("V2 identifier: rendered names round-trip through the parser", "[capi_v2][identifier]") {
	EnvFixture fx;

	// A quoted name is usable verbatim in SQL and comes back as the original name.
	const char *names[] = {"plain", "Mixed Case", "select", "we\"ird", "1abc"};
	for (auto name : names) {
		ExecSQL(fx.conn, ("CREATE TABLE " + RenderQuoted(name) + " (i INTEGER)").c_str());
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(Query(fx.conn, ("SELECT * FROM " + RenderQuoted(name)).c_str(), &result) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_result_destroy(&result);
	}
}

TEST_CASE("V2 identifier: owned result and null arguments", "[capi_v2][identifier]") {
	auto view = Convert("my col");

	// The result is null-terminated and owned by the caller.
	duckdb_v2_bytebuf buf;
	REQUIRE(duckdb_v2_identifier_render_quoted(&view, &buf, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(buf.size == 8);
	REQUIRE(std::string(buf.data) == "\"my col\"");
	REQUIRE(duckdb_v2_bytebuf_destroy(&buf) == DUCKDB_V2_ERROR_NONE);

	// A null name view with a non-zero length, or a null output, is an input error.
	duckdb_v2_identifier_t malformed = {nullptr, 3};
	REQUIRE(duckdb_v2_identifier_render_quoted(&malformed, &buf, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(buf.data == nullptr);
	REQUIRE(buf.release == nullptr);
	REQUIRE(duckdb_v2_identifier_render_quoted(&view, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 identifier: names must be valid UTF-8", "[capi_v2][identifier]") {
	duckdb_v2_bytebuf buf;
	auto name_str = Convert("\x80");
	REQUIRE(duckdb_v2_identifier_render_quoted(&name_str, &buf, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(buf.release == nullptr);
	const std::string trailing_invalid("ok\0\x80", 4);
	auto name_str2 = Convert(trailing_invalid);
	REQUIRE(duckdb_v2_identifier_render_quoted(&name_str2, &buf, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(buf.release == nullptr);
	auto name_str3 = Convert("caf\xc3\xa9");
	REQUIRE(duckdb_v2_identifier_render_quoted(&name_str3, &buf, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buf.data, buf.size) == "\"caf\xc3\xa9\"");
	REQUIRE(duckdb_v2_bytebuf_destroy(&buf) == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
