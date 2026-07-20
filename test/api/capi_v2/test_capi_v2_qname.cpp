#include "capi_v2_test_helpers.hpp"

#include <cstdlib>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 qname: an owned qualified name, an ordered path of one to three
// non-empty identifier parts whose last element is the object name. These
// tests pin the constructors (parse and from-parts), the part getters, the
// engine-accurate SQL rendering, case-insensitive equality and hashing, and
// the handle lifecycle.
// ---------------------------------------------------------------------------

namespace {

// Construct from parts and assert success.
duckdb_v2_qname_handle QnameOf(const std::vector<const char *> &parts) {
	std::vector<duckdb_v2_identifier_t> views;
	for (auto *part : parts) {
		views.push_back(V2Str(part));
	}
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_create(views.data(), views.size(), &qname, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(qname != nullptr);
	return qname;
}

// Render and return the owned text.
std::string RenderOf(duckdb_v2_qname_handle qname) {
	char *text = nullptr;
	REQUIRE(duckdb_v2_qname_render(qname, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(text != nullptr);
	std::string result(text);
	std::free(text);
	return result;
}

} // namespace

TEST_CASE("V2 qname: parse splits dotted text into parts", "[capi_v2][qname]") {
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_parse(V2Str("memory.main.tbl"), &qname, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_qname_get_part_count(qname, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 3);

	duckdb_v2_identifier_t part = {nullptr, 0};
	REQUIRE(duckdb_v2_qname_get_part(qname, 0, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "memory");
	REQUIRE(duckdb_v2_qname_get_part(qname, 1, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "main");
	REQUIRE(duckdb_v2_qname_get_part(qname, 2, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "tbl");

	// An out-of-range index is rejected; the borrowed part is untouched by the failure.
	REQUIRE(duckdb_v2_qname_get_part(qname, 3, &part, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_qname_destroy(&qname);
	REQUIRE(qname == nullptr);
}

TEST_CASE("V2 qname: parse honors quoted parts", "[capi_v2][qname]") {
	// A dot inside a quoted part belongs to the part, not the path.
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_parse(V2Str("s.\"a.b\""), &qname, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_qname_get_part_count(qname, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	duckdb_v2_identifier_t part = {nullptr, 0};
	REQUIRE(duckdb_v2_qname_get_part(qname, 0, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "s");
	REQUIRE(duckdb_v2_qname_get_part(qname, 1, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "a.b");

	// The quoted part renders back quoted; the plain part stays bare.
	REQUIRE(RenderOf(qname) == "s.\"a.b\"");

	duckdb_v2_qname_destroy(&qname);
}

TEST_CASE("V2 qname: parse rejects malformed text", "[capi_v2][qname]") {
	duckdb_v2_qname_handle qname = nullptr;

	// Empty text carries no name.
	REQUIRE(duckdb_v2_qname_parse(V2Str(""), &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(qname == nullptr);
	// A zero-length delimited identifier ("") is rejected by the engine parser.
	REQUIRE(duckdb_v2_qname_parse(V2Str("\"\""), &qname, nullptr) == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(qname == nullptr);
	// The malformed null-pointer-with-nonzero-length view.
	REQUIRE(duckdb_v2_qname_parse(duckdb_v2_str {nullptr, 5}, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	// A null out pointer.
	REQUIRE(duckdb_v2_qname_parse(V2Str("t"), nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	// More than three parts is an engine parser error, surfaced through err.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_qname_parse(V2Str("a.b.c.d"), &qname, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(qname == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// An unterminated quote is an engine parser error.
	REQUIRE(duckdb_v2_qname_parse(V2Str("\"unterminated"), &qname, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(qname == nullptr);
}

TEST_CASE("V2 qname: create builds a path from parts", "[capi_v2][qname]") {
	auto one = QnameOf({"tbl"});
	auto two = QnameOf({"s", "tbl"});
	auto three = QnameOf({"cat", "s", "tbl"});

	idx_t count = 0;
	REQUIRE(duckdb_v2_qname_get_part_count(one, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	REQUIRE(duckdb_v2_qname_get_part_count(two, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	REQUIRE(duckdb_v2_qname_get_part_count(three, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 3);

	// Parts are stored verbatim, outermost first.
	duckdb_v2_identifier_t part = {nullptr, 0};
	REQUIRE(duckdb_v2_qname_get_part(three, 0, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "cat");
	REQUIRE(duckdb_v2_qname_get_part(three, 2, &part, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(part == "tbl");

	duckdb_v2_qname_destroy(&one);
	duckdb_v2_qname_destroy(&two);
	duckdb_v2_qname_destroy(&three);
}

TEST_CASE("V2 qname: create rejects empty and oversized paths", "[capi_v2][qname]") {
	duckdb_v2_qname_handle qname = nullptr;

	// Zero parts.
	REQUIRE(duckdb_v2_qname_create(nullptr, 0, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(qname == nullptr);

	// More than three parts: deeper qualification does not exist engine-side.
	duckdb_v2_identifier_t four[] = {V2Str("a"), V2Str("b"), V2Str("c"), V2Str("d")};
	REQUIRE(duckdb_v2_qname_create(four, 4, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	// An empty part is a placeholder, and placeholders do not cross the ABI:
	// partial qualification is expressed by passing fewer parts.
	duckdb_v2_identifier_t with_empty[] = {V2Str(""), V2Str("tbl")};
	REQUIRE(duckdb_v2_qname_create(with_empty, 2, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	// A null parts array with a nonzero count.
	REQUIRE(duckdb_v2_qname_create(nullptr, 2, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	// A null out pointer.
	duckdb_v2_identifier_t just_name[] = {V2Str("tbl")};
	REQUIRE(duckdb_v2_qname_create(just_name, 1, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2 qname: render quotes each part only when required", "[capi_v2][qname]") {
	// Bare parts render bare.
	auto plain = QnameOf({"memory", "main", "tbl"});
	REQUIRE(RenderOf(plain) == "memory.main.tbl");
	duckdb_v2_qname_destroy(&plain);

	// A keyword, a spaced name, and an interior quote each force quoting for
	// their own part only; casing is preserved and does not force quoting.
	auto mixed = QnameOf({"MySchema", "select"});
	REQUIRE(RenderOf(mixed) == "MySchema.\"select\"");
	duckdb_v2_qname_destroy(&mixed);

	auto spaced = QnameOf({"my table"});
	REQUIRE(RenderOf(spaced) == "\"my table\"");
	duckdb_v2_qname_destroy(&spaced);

	auto quoted = QnameOf({"a\"b"});
	REQUIRE(RenderOf(quoted) == "\"a\"\"b\"");
	duckdb_v2_qname_destroy(&quoted);

	// Rendered text round-trips through parse when no part contains a quote
	// character.
	auto original = QnameOf({"my catalog", "main", "my table"});
	auto rendered = RenderOf(original);
	duckdb_v2_qname_handle reparsed = nullptr;
	REQUIRE(duckdb_v2_qname_parse(V2Str(rendered), &reparsed, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool equal = false;
	REQUIRE(duckdb_v2_qname_equals(original, reparsed, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(equal);
	duckdb_v2_qname_destroy(&original);
	duckdb_v2_qname_destroy(&reparsed);
}

TEST_CASE("V2 qname: equality and hash are case-insensitive", "[capi_v2][qname]") {
	auto lower = QnameOf({"main", "foobar"});
	auto mixed = QnameOf({"MAIN", "FooBar"});
	auto other = QnameOf({"main", "other"});
	auto shorter = QnameOf({"foobar"});

	bool equal = false;
	REQUIRE(duckdb_v2_qname_equals(lower, mixed, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(equal);
	REQUIRE(duckdb_v2_qname_equals(lower, other, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!equal);
	// A different path length is a different name, even with a matching last part.
	REQUIRE(duckdb_v2_qname_equals(lower, shorter, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!equal);

	// Equal names hash equal.
	uint64_t lower_hash = 0;
	uint64_t mixed_hash = 0;
	REQUIRE(duckdb_v2_qname_hash(lower, &lower_hash, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_qname_hash(mixed, &mixed_hash, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lower_hash == mixed_hash);

	// Parse and create agree: the same path hashes and compares the same
	// regardless of how it was constructed.
	duckdb_v2_qname_handle parsed = nullptr;
	REQUIRE(duckdb_v2_qname_parse(V2Str("main.foobar"), &parsed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_qname_equals(lower, parsed, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(equal);
	uint64_t parsed_hash = 0;
	REQUIRE(duckdb_v2_qname_hash(parsed, &parsed_hash, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(parsed_hash == lower_hash);
	duckdb_v2_qname_destroy(&parsed);

	// Null arguments are rejected.
	REQUIRE(duckdb_v2_qname_equals(nullptr, lower, &equal, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_qname_equals(lower, nullptr, &equal, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	uint64_t hash = 0;
	REQUIRE(duckdb_v2_qname_hash(nullptr, &hash, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_qname_destroy(&lower);
	duckdb_v2_qname_destroy(&mixed);
	duckdb_v2_qname_destroy(&other);
	duckdb_v2_qname_destroy(&shorter);
}

TEST_CASE("V2 qname: destroy is null-safe and idempotent", "[capi_v2][qname]") {
	REQUIRE(duckdb_v2_qname_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_destroy(&qname) == DUCKDB_V2_ERROR_NONE);

	qname = QnameOf({"tbl"});
	REQUIRE(duckdb_v2_qname_destroy(&qname) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(qname == nullptr);
	REQUIRE(duckdb_v2_qname_destroy(&qname) == DUCKDB_V2_ERROR_NONE);

	// Null getter subjects are rejected.
	idx_t count = 0;
	REQUIRE(duckdb_v2_qname_get_part_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	char *text = nullptr;
	REQUIRE(duckdb_v2_qname_render(nullptr, &text, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}
