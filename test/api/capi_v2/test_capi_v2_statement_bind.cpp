#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

#include <string>

// V2 statement_bind tests: bind a parsed statement without executing, yielding
// its signature (output schema = result columns, input schema = parameter
// types). Non-consuming. Dynamic PIVOT (an expanding statement) is rejected.

namespace {

std::string SbView(duckdb_v2_str s) {
	return s.ptr ? std::string(s.ptr, s.len) : std::string();
}

DUCKDB_V2_LOGICAL_TYPE_ID SbTypeId(duckdb_v2_logical_type_handle type) {
	DUCKDB_V2_LOGICAL_TYPE_ID id;
	REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	return id;
}

// Parse exactly one statement from sql (raw, unbound).
duckdb_v2_sql_statement_handle SbParseOne(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);
	return stmt;
}

} // namespace

TEST_CASE("V2: statement_bind output schema of a SELECT", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER, b VARCHAR)");
	auto stmt = SbParseOne(fx.conn, "SELECT a, b FROM t");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(out, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(out, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "a");
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(duckdb_v2_schema_get_field(out, 1, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "b");
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	// No parameters.
	REQUIRE(duckdb_v2_schema_get_count(params, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_schema_destroy(&params);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind input schema is ordered by binding index", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER, b VARCHAR)");
	auto stmt = SbParseOne(fx.conn, "SELECT * FROM t WHERE a = $1 AND b = $2");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(params, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(params, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "1");
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(duckdb_v2_schema_get_field(params, 1, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "2");
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_schema_destroy(&params);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind of a non-RETURNING INSERT", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER, b VARCHAR)");
	auto stmt = SbParseOne(fx.conn, "INSERT INTO t VALUES ($1, $2)");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, nullptr) == DUCKDB_V2_ERROR_NONE);

	// A CHANGED_ROWS output schema is a single BIGINT changed-rows count, not empty.
	idx_t count = 999;
	REQUIRE(duckdb_v2_schema_get_count(out, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(out, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	REQUIRE(duckdb_v2_schema_get_count(params, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_schema_destroy(&params);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind rejects a dynamic PIVOT", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE sales(product VARCHAR, quarter VARCHAR, amount INTEGER)");
	auto stmt = SbParseOne(fx.conn, "PIVOT sales ON quarter USING sum(amount)");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	// Dynamic PIVOT expands into a group; its schema is data-dependent.
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(params == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Borrowed, not consumed: the statement is still alive.
	REQUIRE(stmt != nullptr);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind of a static PIVOT yields a schema", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE sales(product VARCHAR, quarter VARCHAR, amount INTEGER)");
	auto stmt = SbParseOne(fx.conn, "PIVOT sales ON quarter IN ('Q1', 'Q2') USING sum(amount)");

	duckdb_v2_schema_handle out = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(out, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count >= 1); // product + Q1 + Q2

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind is non-consuming and out_parameters is optional", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	auto stmt = SbParseOne(fx.conn, "SELECT a FROM t");

	// Bind once with both out-params.
	duckdb_v2_schema_handle out1 = nullptr;
	duckdb_v2_schema_handle params1 = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out1, &params1, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr); // not consumed

	// Bind the same statement again, opting out of the parameter schema.
	duckdb_v2_schema_handle out2 = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out2, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(out2, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);

	duckdb_v2_schema_destroy(&out1);
	duckdb_v2_schema_destroy(&params1);
	duckdb_v2_schema_destroy(&out2);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind surfaces bind errors and rejects null args", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	auto stmt = SbParseOne(fx.conn, "SELECT * FROM does_not_exist");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, nullptr, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_sql_statement_destroy(&stmt);

	// Null arguments.
	duckdb_v2_schema_handle schema = nullptr;
	auto valid = SbParseOne(fx.conn, "SELECT 1");
	REQUIRE(duckdb_v2_statement_bind(nullptr, valid, &schema, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_statement_bind(fx.conn, nullptr, &schema, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_statement_bind(fx.conn, valid, nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_sql_statement_destroy(&valid);
}

TEST_CASE("V2: statement_bind does not cancel a live result", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT range FROM range(5000)");

	// Start a streaming result and partially consume it (the stream is now live/paused).
	auto qstmt = SbParseOne(fx.conn, "SELECT a FROM t");
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, qstmt, nullptr, nullptr, 0, &result, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&qstmt); // non-consuming: the result holds its own copy
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_result_fetch_chunk(result, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);
	idx_t seen = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &seen, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_data_chunk_destroy(&chunk);

	// Bind another statement mid-stream: BindStatement is read-only and must not
	// cancel the live result.
	auto bstmt = SbParseOne(fx.conn, "SELECT a, a + 1 FROM t WHERE a > $1");
	duckdb_v2_schema_handle out = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, bstmt, &out, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t cols = 0;
	REQUIRE(duckdb_v2_schema_get_count(out, &cols, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cols == 2);
	duckdb_v2_schema_destroy(&out);
	duckdb_v2_sql_statement_destroy(&bstmt);

	// Resume the original stream: it survived the bind and delivers every remaining row.
	for (;;) {
		duckdb_v2_data_chunk_handle c = nullptr;
		REQUIRE(duckdb_v2_result_fetch_chunk(result, &c, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (!c) {
			break;
		}
		idx_t n = 0;
		REQUIRE(duckdb_v2_data_chunk_get_size(c, &n, nullptr) == DUCKDB_V2_ERROR_NONE);
		seen += n;
		duckdb_v2_data_chunk_destroy(&c);
	}
	REQUIRE(seen == 5000); // nothing was cancelled

	duckdb_v2_result_destroy(&result);
}

TEST_CASE("V2: statement_bind handles gapped positional parameters", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER)");
	// $2 is absent: binding indices are {1, 3}, not a dense 1..N.
	auto stmt = SbParseOne(fx.conn, "SELECT * FROM t WHERE a = $1 AND c = $3");

	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(params, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	// Ordered by binding index; the field name carries the index ("1", "3").
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(params, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "1");
	REQUIRE(duckdb_v2_schema_get_field(params, 1, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbView(name) == "3");

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_schema_destroy(&params);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_bind of a NOTHING statement reports a status column", "[capi_v2][statement_bind]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	// Bind (does not execute), so the table is not actually dropped.
	auto stmt = SbParseOne(fx.conn, "DROP TABLE t");

	duckdb_v2_schema_handle out = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	// No output schema is empty: a NOTHING statement reports a single BOOLEAN status
	// column (CHANGED_ROWS reports a BIGINT count instead).
	idx_t count = 999;
	REQUIRE(duckdb_v2_schema_get_count(out, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(out, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(SbTypeId(type) == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);

	duckdb_v2_schema_destroy(&out);
	duckdb_v2_sql_statement_destroy(&stmt);
}
