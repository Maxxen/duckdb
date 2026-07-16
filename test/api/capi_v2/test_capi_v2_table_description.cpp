#include "capi_v2_test_helpers.hpp"
#include "test_helpers.hpp"

#include <cstdlib>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 table description: resolve one table name and snapshot where it
// resolved, its columns, and per-column catalog facts. These tests pin the
// resolution semantics (search path, two-part schema-versus-catalog reading,
// the error cases), the resolved-name reporting, the schema and flag getters,
// and the snapshot lifecycle.
// ---------------------------------------------------------------------------

namespace {

// Owned qname built from parts; the caller destroys it.
duckdb_v2_qname_handle MakeQname(const std::vector<const char *> &parts) {
	std::vector<duckdb_v2_identifier_t> views;
	for (auto *part : parts) {
		views.push_back(V2Str(part));
	}
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_create(views.data(), views.size(), &qname, nullptr) == DUCKDB_V2_ERROR_NONE);
	return qname;
}

// Owned description for a name given as parts; asserts success.
duckdb_v2_table_description_handle DescribeOf(duckdb_v2_connection_handle conn,
                                              const std::vector<const char *> &parts) {
	auto qname = MakeQname(parts);
	duckdb_v2_table_description_handle desc = nullptr;
	REQUIRE(duckdb_v2_table_description_create(conn, qname, &desc, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(desc != nullptr);
	duckdb_v2_qname_destroy(&qname);
	return desc;
}

// The description's resolved name as rendered SQL text.
std::string ResolvedNameOf(duckdb_v2_table_description_handle desc) {
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_table_description_get_qname(desc, &qname, nullptr) == DUCKDB_V2_ERROR_NONE);
	char *text = nullptr;
	REQUIRE(duckdb_v2_qname_render(qname, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string result(text);
	std::free(text);
	duckdb_v2_qname_destroy(&qname);
	return result;
}

} // namespace

TEST_CASE("V2 table description: resolution reports the resolved location", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(i INTEGER)");
	V2ExecSQL(fx.conn, "CREATE TEMP TABLE tt(i INTEGER)");
	V2ExecSQL(fx.conn, "CREATE TABLE \"FooBar\"(i INTEGER)");

	// An unqualified name fills in the resolved catalog and schema.
	auto desc = DescribeOf(fx.conn, {"t"});
	REQUIRE(ResolvedNameOf(desc) == "memory.main.t");
	bool readonly = true;
	REQUIRE(duckdb_v2_table_description_is_readonly(desc, &readonly, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!readonly);
	duckdb_v2_table_description_destroy(&desc);

	// A temp table resolves through the temp catalog, which is writable. The
	// catalog renders quoted because temp is a parser keyword.
	desc = DescribeOf(fx.conn, {"tt"});
	REQUIRE(ResolvedNameOf(desc) == "\"temp\".main.tt");
	REQUIRE(duckdb_v2_table_description_is_readonly(desc, &readonly, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!readonly);
	duckdb_v2_table_description_destroy(&desc);

	// The name comes back with its DDL time casing, not the lookup casing.
	desc = DescribeOf(fx.conn, {"foobar"});
	REQUIRE(ResolvedNameOf(desc) == "memory.main.FooBar");
	duckdb_v2_table_description_destroy(&desc);

	// A fully qualified name resolves as written.
	desc = DescribeOf(fx.conn, {"memory", "main", "t"});
	REQUIRE(ResolvedNameOf(desc) == "memory.main.t");
	duckdb_v2_table_description_destroy(&desc);
}

TEST_CASE("V2 table description: two-part names read as SQL reads them", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE SCHEMA s");
	V2ExecSQL(fx.conn, "CREATE TABLE s.t2(i INTEGER)");
	V2ExecSQL(fx.conn, "ATTACH ':memory:' AS other");
	V2ExecSQL(fx.conn, "CREATE TABLE other.t3(i INTEGER)");

	// schema.table resolves within the default catalog.
	auto desc = DescribeOf(fx.conn, {"s", "t2"});
	REQUIRE(ResolvedNameOf(desc) == "memory.s.t2");
	duckdb_v2_table_description_destroy(&desc);

	// catalog.table promotes the first part to an attached database.
	desc = DescribeOf(fx.conn, {"other", "t3"});
	REQUIRE(ResolvedNameOf(desc) == "other.main.t3");
	duckdb_v2_table_description_destroy(&desc);

	// A first part naming both a schema and an attached database is ambiguous.
	V2ExecSQL(fx.conn, "CREATE SCHEMA amb");
	V2ExecSQL(fx.conn, "ATTACH ':memory:' AS amb");
	auto qname = MakeQname({"amb", "t2"});
	duckdb_v2_table_description_handle ambiguous = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_table_description_create(fx.conn, qname, &ambiguous, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(ambiguous == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_str message = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &message) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2StrTo(message).find("Ambiguous") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_qname_destroy(&qname);
}

TEST_CASE("V2 table description: missing tables and views are rejected", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE VIEW v AS SELECT 42 AS i");

	// A name that resolves to nothing.
	auto qname = MakeQname({"no_such_table"});
	duckdb_v2_table_description_handle desc = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_table_description_create(fx.conn, qname, &desc, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(desc == nullptr);
	duckdb_v2_str message = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &message) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2StrTo(message).find("does not exist") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_qname_destroy(&qname);

	// A name that resolves to a view: a description snapshots a base table.
	qname = MakeQname({"v"});
	REQUIRE(duckdb_v2_table_description_create(fx.conn, qname, &desc, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(desc == nullptr);
	duckdb_v2_qname_destroy(&qname);

	// Null arguments are rejected.
	qname = MakeQname({"v"});
	REQUIRE(duckdb_v2_table_description_create(nullptr, qname, &desc, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_table_description_create(fx.conn, nullptr, &desc, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_table_description_create(fx.conn, qname, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_qname_destroy(&qname);
}

TEST_CASE("V2 table description: schema and per-column flags", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE facts("
	                   "i INTEGER, "
	                   "j VARCHAR DEFAULT 'x', "
	                   "k INTEGER GENERATED ALWAYS AS (i + 1))");

	auto desc = DescribeOf(fx.conn, {"facts"});

	// Every column in declared order, the generated one included.
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_table_description_get_schema(desc, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 3);

	const char *expected_names[] = {"i", "j", "k"};
	DUCKDB_V2_LOGICAL_TYPE_ID expected_types[] = {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
	                                              DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER};
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_identifier_t name = {nullptr, 0};
		duckdb_v2_logical_type_handle type = nullptr;
		REQUIRE(duckdb_v2_schema_get_field(schema, i, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(name == expected_names[i]);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(id == expected_types[i]);
	}
	duckdb_v2_schema_destroy(&schema);

	// The flag getters are index-aligned with the schema.
	bool is_generated = false;
	bool has_default = false;
	const bool expected_generated[] = {false, false, true};
	const bool expected_default[] = {false, true, false};
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE(duckdb_v2_table_description_column_is_generated(desc, i, &is_generated, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(is_generated == expected_generated[i]);
		REQUIRE(duckdb_v2_table_description_column_has_default(desc, i, &has_default, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(has_default == expected_default[i]);
	}

	// An out-of-range index is rejected.
	REQUIRE(duckdb_v2_table_description_column_is_generated(desc, 3, &is_generated, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_table_description_column_has_default(desc, 3, &has_default, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_table_description_destroy(&desc);
}

TEST_CASE("V2 table description: a read only catalog reports readonly", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	auto path = duckdb::TestCreatePath("v2_catalog_readonly.db");
	duckdb::DeleteDatabase(path);

	// Create the file backed table through a writable attach, then reattach read only.
	V2ExecSQL(fx.conn, ("ATTACH '" + path + "' AS rw").c_str());
	V2ExecSQL(fx.conn, "CREATE TABLE rw.rt(i INTEGER)");
	V2ExecSQL(fx.conn, "DETACH rw");
	V2ExecSQL(fx.conn, ("ATTACH '" + path + "' AS ro (READ_ONLY)").c_str());

	auto desc = DescribeOf(fx.conn, {"ro", "rt"});
	REQUIRE(ResolvedNameOf(desc) == "ro.main.rt");
	bool readonly = false;
	REQUIRE(duckdb_v2_table_description_is_readonly(desc, &readonly, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(readonly);
	duckdb_v2_table_description_destroy(&desc);

	V2ExecSQL(fx.conn, "DETACH ro");
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2 table description: a description is a snapshot", "[capi_v2][catalog]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE snap(i INTEGER)");

	auto desc = DescribeOf(fx.conn, {"snap"});
	V2ExecSQL(fx.conn, "DROP TABLE snap");

	// The snapshot stays readable after the table is gone.
	REQUIRE(ResolvedNameOf(desc) == "memory.main.snap");
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_table_description_get_schema(desc, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_table_description_destroy(&desc);

	// Destroy is null-safe and idempotent.
	REQUIRE(duckdb_v2_table_description_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_description_handle null_desc = nullptr;
	REQUIRE(duckdb_v2_table_description_destroy(&null_desc) == DUCKDB_V2_ERROR_NONE);

	// Null getter subjects are rejected.
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_table_description_get_qname(nullptr, &qname, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	bool flag = false;
	REQUIRE(duckdb_v2_table_description_is_readonly(nullptr, &flag, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}
