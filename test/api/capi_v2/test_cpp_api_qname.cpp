#include "catch.hpp"
#include "duckdb_cpp.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API: QualifiedName, the owned wrapper over the V2 qname handle.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: QualifiedName parse, parts, and render", "[cpp_api][qname]") {
	using namespace duckdb_api;

	auto parsed = QualifiedName::Parse("memory.main.tbl");
	REQUIRE(parsed.GetPartCount() == 3);
	REQUIRE(parsed.GetPart(0) == "memory");
	REQUIRE(parsed.GetPart(1) == "main");
	REQUIRE(parsed.GetPart(2) == "tbl");
	REQUIRE(parsed.Render() == "memory.main.tbl");

	// Quoting is applied per part, only when required.
	auto mixed = QualifiedName::FromParts({"MySchema", "my table"});
	REQUIRE(mixed.GetPartCount() == 2);
	REQUIRE(mixed.GetPart(1) == "my table");
	REQUIRE(mixed.Render() == "MySchema.\"my table\"");

	// A dot inside a quoted part belongs to the part.
	auto dotted = QualifiedName::Parse("s.\"a.b\"");
	REQUIRE(dotted.GetPartCount() == 2);
	REQUIRE(dotted.GetPart(1) == "a.b");
}

TEST_CASE("Stable C++API: QualifiedName equality and hash are case-insensitive", "[cpp_api][qname]") {
	using namespace duckdb_api;

	auto lower = QualifiedName::FromParts({"main", "foobar"});
	auto mixed = QualifiedName::FromParts({"MAIN", "FooBar"});
	auto shorter = QualifiedName::FromParts({"foobar"});

	REQUIRE(lower == mixed);
	REQUIRE(lower.Hash() == mixed.Hash());
	REQUIRE(lower != shorter);

	// Parse and FromParts agree on the same path.
	REQUIRE(lower == QualifiedName::Parse("main.foobar"));
}

TEST_CASE("Stable C++API: QualifiedName rejects invalid construction", "[cpp_api][qname]") {
	using namespace duckdb_api;

	// Empty text, empty parts, and placeholder parts are invalid: partial
	// qualification is expressed by passing fewer parts.
	REQUIRE_THROWS_AS(QualifiedName::Parse(""), Exception);
	REQUIRE_THROWS_AS(QualifiedName::FromParts({}), Exception);
	REQUIRE_THROWS_AS(QualifiedName::FromParts({"", "tbl"}), Exception);
	REQUIRE_THROWS_AS(QualifiedName::FromParts({"a", "b", "c", "d"}), Exception);
	REQUIRE_THROWS_AS(QualifiedName::Parse("a.b.c.d"), Exception);
	REQUIRE_THROWS_AS(QualifiedName::Parse("\"unterminated"), Exception);
}
