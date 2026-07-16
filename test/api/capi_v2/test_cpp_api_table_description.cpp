#include "catch.hpp"
#include "duckdb_cpp.hpp"

#include <string>

// ---------------------------------------------------------------------------
// Stable C++ API: TableDescription, the owned snapshot returned by
// Connection::DescribeTable.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: DescribeTable reports the resolved location", "[cpp_api][catalog]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(i INTEGER)").Drain();
	conn.Execute("CREATE TEMP TABLE tt(i INTEGER)").Drain();
	conn.Execute("CREATE TABLE \"FooBar\"(i INTEGER)").Drain();

	// An unqualified name fills in the resolved catalog and schema.
	auto desc = conn.DescribeTable(QualifiedName::Parse("t"));
	REQUIRE(desc.GetQualifiedName().Render() == "memory.main.t");
	REQUIRE(!desc.IsReadonly());

	// A temp table resolves through the temp catalog; the catalog renders
	// quoted because temp is a parser keyword.
	REQUIRE(conn.DescribeTable(QualifiedName::Parse("tt")).GetQualifiedName().Render() == "\"temp\".main.tt");

	// The name comes back with its DDL time casing, not the lookup casing.
	REQUIRE(conn.DescribeTable(QualifiedName::Parse("foobar")).GetQualifiedName().Render() == "memory.main.FooBar");

	// Missing tables and views are rejected.
	REQUIRE_THROWS_AS(conn.DescribeTable(QualifiedName::Parse("no_such_table")), Exception);
	conn.Execute("CREATE VIEW v AS SELECT 42 AS i").Drain();
	REQUIRE_THROWS_AS(conn.DescribeTable(QualifiedName::Parse("v")), Exception);
}

TEST_CASE("Stable C++API: TableDescription schema and per-column flags", "[cpp_api][catalog]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE facts("
	             "i INTEGER, "
	             "j VARCHAR DEFAULT 'x', "
	             "k INTEGER GENERATED ALWAYS AS (i + 1))")
	    .Drain();

	auto desc = conn.DescribeTable(QualifiedName::Parse("facts"));

	// Every column in declared order, the generated one included; the flag
	// getters are index-aligned with the schema.
	auto schema = desc.GetSchema();
	REQUIRE(schema.GetFieldCount() == 3);
	REQUIRE(schema.GetFieldName(0) == "i");
	REQUIRE(schema.GetFieldName(1) == "j");
	REQUIRE(schema.GetFieldName(2) == "k");
	REQUIRE(schema.GetFieldType(1).GetId() == TypeId::VARCHAR);

	REQUIRE(!desc.ColumnIsGenerated(0));
	REQUIRE(!desc.ColumnIsGenerated(1));
	REQUIRE(desc.ColumnIsGenerated(2));
	REQUIRE(!desc.ColumnHasDefault(0));
	REQUIRE(desc.ColumnHasDefault(1));
	REQUIRE(!desc.ColumnHasDefault(2));

	REQUIRE_THROWS_AS(desc.ColumnIsGenerated(3), Exception);

	// The description is a snapshot: it survives the table it describes.
	conn.Execute("DROP TABLE facts").Drain();
	REQUIRE(desc.GetQualifiedName().Render() == "memory.main.facts");
	REQUIRE(desc.GetSchema().GetFieldCount() == 3);
}
