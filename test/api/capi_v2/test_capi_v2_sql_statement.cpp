#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 sql_statement tests: parse_sql, the statement iterator, and the
// transfer of statements into connection_query.
// ---------------------------------------------------------------------------

namespace {} // namespace

// ===========================================================================
// The canonical loop: parse a multi-statement string, execute each
// statement on the connection, one result at a time.
// ===========================================================================

TEST_CASE("V2: parse_sql iterates a multi-statement string", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 42; SELECT 84; SELECT 126", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(iter != nullptr);

	int statement_count = 0;
	while (true) {
		duckdb_v2_sql_statement_handle stmt = nullptr;
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (!stmt) {
			break; // exhausted
		}
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_connection_query(fx.conn, &stmt, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt == nullptr); // consumed by the transfer
		REQUIRE(V2DrainRowCount(r) == 1);
		duckdb_v2_result_destroy(&r);
		statement_count++;
	}
	REQUIRE(statement_count == 3);

	// Exhaustion is idempotent.
	duckdb_v2_sql_statement_handle stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt == nullptr);

	duckdb_v2_statement_iterator_destroy(&iter);
	REQUIRE(iter == nullptr);
}

// ===========================================================================
// Statements outlive the iterator, and unconsumed statements are
// destroyed independently.
// ===========================================================================

TEST_CASE("V2: statements are independently owned", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELECT 2", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_sql_statement_handle first = nullptr;
	duckdb_v2_sql_statement_handle second = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first != nullptr);
	REQUIRE(second != nullptr);

	// Destroy the iterator first; the yielded statements stay valid.
	duckdb_v2_statement_iterator_destroy(&iter);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, &first, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	// The second statement is never executed; destroy it directly.
	REQUIRE(duckdb_v2_sql_statement_destroy(&second) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(second == nullptr);
}

// ===========================================================================
// Parse errors carry the parser error type. With the current eager
// backend they surface at parse_sql; the contract also permits them to
// surface from the next() call that reaches the failing statement.
// ===========================================================================

TEST_CASE("V2: parse errors surface with QUERY_PARSER", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	// Backend-agnostic loop, per the spec commentary: an eager parser
	// reports the error from parse_sql and yields nothing; an incremental
	// parser reports it from the next() call that reaches the failing
	// statement, after yielding the statements before it.
	duckdb_v2_statement_iterator_handle iter = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELEKT 2", &iter, &err);
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_sql_statement_handle stmt = nullptr;
		rc = duckdb_v2_statement_iterator_next(iter, &stmt, &err);
		if (rc == DUCKDB_V2_ERROR_NONE && !stmt) {
			FAIL("iterator exhausted without surfacing the parse error");
		}
		duckdb_v2_sql_statement_destroy(&stmt);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(msg.ptr[0] != '\0');
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_statement_iterator_destroy(&iter);
}

// ===========================================================================
// No-statement input yields an immediately exhausted iterator.
// ===========================================================================

TEST_CASE("V2: no-statement input parses to an exhausted iterator", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	for (const char *sql : {"", "   ", ";", ";;;"}) {
		INFO("sql: '" << sql << "'");
		duckdb_v2_statement_iterator_handle iter = nullptr;
		REQUIRE(duckdb_v2_parse_sql(fx.conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_sql_statement_handle stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt == nullptr);
		duckdb_v2_statement_iterator_destroy(&iter);
	}
}

// ===========================================================================
// Transfer semantics at connection_query: consumed on success and on
// prepare-time failure; left intact by the busy and null-arg refusals.
// ===========================================================================

TEST_CASE("V2: connection_query consumes the statement on prepare failure", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT * FROM no_such_table", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, &stmt, &r, nullptr) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
	REQUIRE(stmt == nullptr); // consumed: the engine took ownership
}

TEST_CASE("V2: the busy refusal leaves the statement intact", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_statement_iterator_destroy(&iter);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, &stmt, &r, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(stmt != nullptr); // not consumed: the engine was never reached

	// Draining the live result frees the connection; the same statement
	// then runs.
	REQUIRE(V2DrainRowCount(live) == 100000);
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_connection_query(fx.conn, &stmt, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt == nullptr);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Null-arg validation and destroy null-safety.
// ===========================================================================

TEST_CASE("V2: sql_statement null-arg rejection and null-safe destroys", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_result_handle r = nullptr;

	REQUIRE(duckdb_v2_parse_sql(nullptr, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, nullptr, &iter, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_statement_iterator_next(nullptr, &stmt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_statement_iterator_destroy(&iter);

	// connection_query rejects a NULL statement handle and a NULL slot,
	// leaving any statement intact.
	REQUIRE(duckdb_v2_connection_query(fx.conn, nullptr, &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_query(fx.conn, &stmt, &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT); // *stmt == NULL

	// Destroys are null-safe.
	REQUIRE(duckdb_v2_sql_statement_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_sql_statement_destroy(&stmt) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_statement_iterator_handle already_null = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
}

// ===========================================================================
// Statement-level preprocessing parity with the old string path: a
// PRAGMA parses and runs through the iterator.
// ===========================================================================

TEST_CASE("V2: pragma statements parse and execute through the iterator", "[capi_v2][sql_statement]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "PRAGMA enable_progress_bar", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(r);
	duckdb_v2_result_destroy(&r);
}
