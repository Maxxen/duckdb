#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 query_result tests — schema and metadata only. Row-data access lives
// on the data_chunk + vector surfaces and is exercised separately.
//
// INVARIANTS THIS FILE RELIES ON:
//   - duckdb_v2_result_ptr is a heap-allocated
//     duckdb::MaterializedQueryResult cast to void *. The bridge's
//     ToResult cast and destructor depend on this layout; if a wrapper
//     is ever added, this file must change.
//   - duckdb_v2_logical_type_ptr returned by result_column_logical_type
//     is owned (caller destroys). Lifetime is independent of the result.
//   - Borrowed name strings returned by duckdb_v2_result_column_name are
//     valid until duckdb_v2_result_destroy.
//   - Error message strings reachable via duckdb_v2_error_info_get_message
//     are valid until duckdb_v2_error_info_destroy.
// ---------------------------------------------------------------------------

namespace {} // namespace

// ===========================================================================
// Smoke: SELECT round-trip — the canonical "SELECT 1" check.
// ===========================================================================

TEST_CASE("V2: connection_query SELECT returns QUERY_RESULT with INTEGER column", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT 1 AS one", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	idx_t cols = 0;
	REQUIRE(duckdb_v2_result_column_count(r, &cols, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cols == 1);

	const char *name = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_result_column_name(r, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "one");
	REQUIRE(std::strlen(name) == len); // null-terminated and length agree

	duckdb_v2_logical_type_ptr lt = nullptr;
	REQUIRE(duckdb_v2_result_column_logical_type(r, 0, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lt != nullptr);
	DUCKDB_V2_LOGICAL_TYPE_ID lt_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(lt, &lt_id, nullptr);
	REQUIRE(lt_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&lt);

	idx_t changed = 99;
	REQUIRE(duckdb_v2_result_rows_changed(r, &changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(changed == 0); // SELECT never reports changed rows

	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
}

// ===========================================================================
// Multi-column SELECT — column count, names, and types.
// ===========================================================================

TEST_CASE("V2: connection_query multi-column SELECT", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT 1::INTEGER AS a, 'hi' AS b, 3.14::DOUBLE AS c", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	idx_t cols = 0;
	duckdb_v2_result_column_count(r, &cols, nullptr);
	REQUIRE(cols == 3);

	struct {
		const char *name;
		DUCKDB_V2_LOGICAL_TYPE_ID id;
	} expected[] = {
	    {"a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {"b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR},
	    {"c", DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE},
	};
	for (idx_t i = 0; i < 3; i++) {
		const char *name = nullptr;
		idx_t len = 0;
		duckdb_v2_result_column_name(r, i, &name, &len, nullptr);
		REQUIRE(std::string(name, len) == expected[i].name);

		duckdb_v2_logical_type_ptr lt = nullptr;
		duckdb_v2_result_column_logical_type(r, i, &lt, nullptr);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		duckdb_v2_logical_type_get_id(lt, &id, nullptr);
		REQUIRE(id == expected[i].id);
		duckdb_v2_logical_type_destroy(&lt);
	}

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// CHANGED_ROWS: INSERT / UPDATE / DELETE report row counts.
// ===========================================================================

TEST_CASE("V2: INSERT / UPDATE / DELETE return CHANGED_ROWS with rows_changed", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr setup = nullptr;
	duckdb_v2_connection_query(fx.conn, "CREATE TABLE t (i INTEGER)", &setup, nullptr);
	duckdb_v2_result_destroy(&setup);

	// INSERT three rows.
	duckdb_v2_result_ptr ins = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "INSERT INTO t VALUES (1), (2), (3)", &ins, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(ins, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(ins, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_INSERT);

	idx_t changed = 0;
	duckdb_v2_result_rows_changed(ins, &changed, nullptr);
	REQUIRE(changed == 3);
	duckdb_v2_result_destroy(&ins);

	// UPDATE two rows.
	duckdb_v2_result_ptr upd = nullptr;
	duckdb_v2_connection_query(fx.conn, "UPDATE t SET i = i + 10 WHERE i >= 2", &upd, nullptr);
	duckdb_v2_result_get_statement_type(upd, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_UPDATE);
	duckdb_v2_result_rows_changed(upd, &changed, nullptr);
	REQUIRE(changed == 2);
	duckdb_v2_result_destroy(&upd);

	// DELETE one row.
	duckdb_v2_result_ptr del = nullptr;
	duckdb_v2_connection_query(fx.conn, "DELETE FROM t WHERE i = 1", &del, nullptr);
	duckdb_v2_result_get_statement_type(del, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_DELETE);
	duckdb_v2_result_rows_changed(del, &changed, nullptr);
	REQUIRE(changed == 1);
	duckdb_v2_result_destroy(&del);
}

// ===========================================================================
// DDL: CREATE / DROP report NOTHING.
// ===========================================================================

TEST_CASE("V2: DDL returns NOTHING with rows_changed = 0", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "CREATE TABLE u (i INTEGER)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	duckdb_v2_result_get_result_type(r, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_NOTHING);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(r, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_CREATE);

	idx_t changed = 99;
	duckdb_v2_result_rows_changed(r, &changed, nullptr);
	REQUIRE(changed == 0);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Error path: malformed SQL surfaces detail, out_result is null on failure.
// ===========================================================================

TEST_CASE("V2: connection_query surfaces parser error and leaves out_result null", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELEKT 1", &r, &err) == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);

	const char *msg = nullptr;
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg != nullptr);
	REQUIRE(msg[0] != '\0'); // detail propagated from the parser
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: connection_query binder error (unknown table)", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT * FROM no_such_table", &r, &err) ==
	        DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: connection_query failure tolerates err == nullptr", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "BADSQL", &r, nullptr) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
}

// ===========================================================================
// Lifetime: borrowed column name stays valid until result_destroy.
// ===========================================================================

TEST_CASE("V2: result_column_name borrow stays valid until destroy", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_connection_query(fx.conn, "SELECT 1 AS only_column", &r, nullptr);

	const char *first = nullptr;
	idx_t first_len = 0;
	duckdb_v2_result_column_name(r, 0, &first, &first_len, nullptr);
	REQUIRE(std::string(first, first_len) == "only_column");

	// Second read returns the same pointer (the names vector owns the storage).
	const char *second = nullptr;
	idx_t second_len = 0;
	duckdb_v2_result_column_name(r, 0, &second, &second_len, nullptr);
	REQUIRE(first == second);
	REQUIRE(first_len == second_len);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Lifetime: column logical_type outlives the result it came from.
// ===========================================================================

TEST_CASE("V2: result_column_logical_type is owned and outlives the result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_connection_query(fx.conn, "SELECT 1 AS only_column", &r, nullptr);

	duckdb_v2_logical_type_ptr lt = nullptr;
	REQUIRE(duckdb_v2_result_column_logical_type(r, 0, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_destroy(&r);

	// The handle is still valid after the result is gone — the
	// LogicalType copy is self-contained.
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(lt, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_logical_type_destroy(&lt);
}

// ===========================================================================
// Out-of-range column indices.
// ===========================================================================

TEST_CASE("V2: result_column_name out-of-range index", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_connection_query(fx.conn, "SELECT 1, 2", &r, nullptr);

	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_result_column_name(r, 2, &name, &len, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_logical_type_ptr lt = nullptr;
	REQUIRE(duckdb_v2_result_column_logical_type(r, 2, &lt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(lt == nullptr);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Null-arg validation across the surface.
// ===========================================================================

TEST_CASE("V2: connection_query null-arg rejection", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(nullptr, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_query(fx.conn, nullptr, &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT 1", nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: result_destroy is null-safe", "[capi_v2][query_result]") {
	REQUIRE(duckdb_v2_result_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_ptr already_null = nullptr;
	REQUIRE(duckdb_v2_result_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

TEST_CASE("V2: result accessors reject null handle and null out-params", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	DUCKDB_V2_RESULT_TYPE rt;
	DUCKDB_V2_STATEMENT_TYPE st;
	idx_t count;
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr lt = nullptr;

	REQUIRE(duckdb_v2_result_get_result_type(nullptr, &rt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_statement_type(nullptr, &st, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_name(nullptr, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_logical_type(nullptr, 0, &lt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_rows_changed(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_connection_query(fx.conn, "SELECT 1", &r, nullptr);
	REQUIRE(duckdb_v2_result_get_result_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_statement_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_count(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_name(r, 0, nullptr, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_name(r, 0, &name, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_column_logical_type(r, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_rows_changed(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Belt-and-braces: a succeeding call leaves a pre-existing *err untouched.
// ===========================================================================

TEST_CASE("V2: connection_query leaves pre-existing err untouched on success", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "BADSQL", &r, &err) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(err != nullptr);

	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT 1", &r, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_code_t code = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_get_code(err, &code);
	REQUIRE(code == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Numeric round-trip for higher-numbered StatementType values.
//
// Earlier values (SELECT=1, INSERT=2, …, CREATE=4) happen to line up
// between the various candidate numberings, so the regression that catches
// a spec/core drift has to exercise the higher-numbered ids whose values
// only line up under the §4 "numerically identical to core" convention.
// ===========================================================================

TEST_CASE("V2: statement_type numeric round-trip for higher-numbered values", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr setup = nullptr;
	duckdb_v2_connection_query(fx.conn, "CREATE TABLE t (i INTEGER)", &setup, nullptr);
	duckdb_v2_result_destroy(&setup);

	struct Case {
		const char *sql;
		DUCKDB_V2_STATEMENT_TYPE expected;
	} cases[] = {
	    {"EXPLAIN SELECT 1", DUCKDB_V2_STATEMENT_TYPE_EXPLAIN},          // 14
	    {"DROP TABLE t", DUCKDB_V2_STATEMENT_TYPE_DROP},                 // 15
	    {"PRAGMA enable_progress_bar", DUCKDB_V2_STATEMENT_TYPE_PRAGMA}, // 17
	    {"SET memory_limit='1GB'", DUCKDB_V2_STATEMENT_TYPE_SET},        // 20
	    {"ATTACH ':memory:' AS other", DUCKDB_V2_STATEMENT_TYPE_ATTACH}, // 25
	};
	for (auto &c : cases) {
		duckdb_v2_result_ptr r = nullptr;
		REQUIRE(duckdb_v2_connection_query(fx.conn, c.sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
		duckdb_v2_result_get_statement_type(r, &st, nullptr);
		REQUIRE(st == c.expected);
		duckdb_v2_result_destroy(&r);
	}
}

// ===========================================================================
// Drift detector: probe the first numeric value past the highest core
// variant V2 currently mirrors. EnumUtil::ToString throws
// NotImplementedException for values not present in its lookup table; if
// a new variant is appended to duckdb::StatementType, the call will
// instead return a string and this assertion will fire — signalling that
// DUCKDB_V2_STATEMENT_TYPE in api_spec/v2/query_result/query_result.yaml
// needs a matching id.
// ===========================================================================

TEST_CASE("V2: STATEMENT_TYPE has no gaps vs duckdb::StatementType", "[capi_v2][query_result]") {
	constexpr auto highest_known = static_cast<uint8_t>(duckdb::StatementType::DISCONNECT_STATEMENT);
	auto probe = static_cast<duckdb::StatementType>(highest_known + 1);
	REQUIRE_THROWS_AS(duckdb::EnumUtil::ToString(probe), duckdb::NotImplementedException);
}

// ===========================================================================
// CHANGED_ROWS with 0 affected rows. Exercises the rows_changed Value
// read on the synthetic Count column when the row count is genuinely 0.
// ===========================================================================

TEST_CASE("V2: rows_changed returns 0 when WHERE filter matches nothing", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr setup = nullptr;
	duckdb_v2_connection_query(fx.conn, "CREATE TABLE t (i INTEGER)", &setup, nullptr);
	duckdb_v2_result_destroy(&setup);

	duckdb_v2_result_ptr ins = nullptr;
	duckdb_v2_connection_query(fx.conn, "INSERT INTO t VALUES (1), (2)", &ins, nullptr);
	duckdb_v2_result_destroy(&ins);

	duckdb_v2_result_ptr upd = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "UPDATE t SET i = i WHERE 1=0", &upd, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(upd, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);
	idx_t changed = 99;
	duckdb_v2_result_rows_changed(upd, &changed, nullptr);
	REQUIRE(changed == 0);
	duckdb_v2_result_destroy(&upd);

	duckdb_v2_result_ptr del = nullptr;
	duckdb_v2_connection_query(fx.conn, "DELETE FROM t WHERE 1=0", &del, nullptr);
	duckdb_v2_result_rows_changed(del, &changed, nullptr);
	REQUIRE(changed == 0);
	duckdb_v2_result_destroy(&del);
}

// ===========================================================================
// Result schema on CHANGED_ROWS: the synthetic Count column is visible
// via the column accessors. This pins the impl/spec alignment — callers
// can enumerate the column if they want to, and rows_changed remains
// the typed accessor that returns the value as idx_t.
// ===========================================================================

TEST_CASE("V2: CHANGED_ROWS exposes synthetic count column via column accessors", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr setup = nullptr;
	duckdb_v2_connection_query(fx.conn, "CREATE TABLE t (i INTEGER)", &setup, nullptr);
	duckdb_v2_result_destroy(&setup);

	duckdb_v2_result_ptr ins = nullptr;
	duckdb_v2_connection_query(fx.conn, "INSERT INTO t VALUES (1)", &ins, nullptr);

	idx_t cols = 0;
	duckdb_v2_result_column_count(ins, &cols, nullptr);
	REQUIRE(cols == 1);

	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_result_column_name(ins, 0, &name, &len, nullptr);
	REQUIRE(name != nullptr);
	REQUIRE(len > 0);
	// The synthetic column name is set by core; pin the current spelling
	// so a future rename surfaces here loud rather than silent.
	REQUIRE(std::string(name, len) == "Count");

	duckdb_v2_logical_type_ptr lt = nullptr;
	duckdb_v2_result_column_logical_type(ins, 0, &lt, nullptr);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(lt, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_logical_type_destroy(&lt);

	duckdb_v2_result_destroy(&ins);
}

// ===========================================================================
// NOTHING results expose the synthetic Count column too (same shape as
// CHANGED_ROWS), but with no rows in it. Out-of-range index past the
// single column still fails.
// ===========================================================================

TEST_CASE("V2: NOTHING result exposes the synthetic Count column with zero rows", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	duckdb_v2_connection_query(fx.conn, "CREATE TABLE t (i INTEGER)", &r, nullptr);

	idx_t cols = 99;
	duckdb_v2_result_column_count(r, &cols, nullptr);
	REQUIRE(cols == 1);

	const char *name = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_result_column_name(r, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "Count");

	duckdb_v2_logical_type_ptr lt = nullptr;
	REQUIRE(duckdb_v2_result_column_logical_type(r, 0, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(lt, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_logical_type_destroy(&lt);

	// rows_changed reports 0 for NOTHING regardless of the column shape.
	idx_t changed = 99;
	duckdb_v2_result_rows_changed(r, &changed, nullptr);
	REQUIRE(changed == 0);

	// Index past the single column fails.
	REQUIRE(duckdb_v2_result_column_name(r, 1, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_ptr lt_oor = nullptr;
	REQUIRE(duckdb_v2_result_column_logical_type(r, 1, &lt_oor, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(lt_oor == nullptr);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Independent results don't alias each other's storage, and destroying
// one leaves the other's borrowed pointers intact.
// ===========================================================================

TEST_CASE("V2: results are independent — destroying one leaves the other usable", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr a = nullptr;
	duckdb_v2_result_ptr b = nullptr;
	duckdb_v2_connection_query(fx.conn, "SELECT 1 AS aa", &a, nullptr);
	duckdb_v2_connection_query(fx.conn, "SELECT 'hi' AS bb", &b, nullptr);

	const char *name_a = nullptr;
	idx_t len_a = 0;
	duckdb_v2_result_column_name(a, 0, &name_a, &len_a, nullptr);

	const char *name_b = nullptr;
	idx_t len_b = 0;
	duckdb_v2_result_column_name(b, 0, &name_b, &len_b, nullptr);

	REQUIRE(name_a != name_b);
	REQUIRE(std::string(name_a, len_a) == "aa");
	REQUIRE(std::string(name_b, len_b) == "bb");

	// Destroy a; b's borrowed name pointer must still be valid.
	duckdb_v2_result_destroy(&a);

	const char *name_b_after = nullptr;
	idx_t len_b_after = 0;
	REQUIRE(duckdb_v2_result_column_name(b, 0, &name_b_after, &len_b_after, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name_b_after, len_b_after) == "bb");

	duckdb_v2_result_destroy(&b);
}

// ===========================================================================
// Empty / whitespace-only SQL: Connection::Query parses these as
// no-statement input and returns a successful empty result
// (statement_type=INVALID, return_type=QUERY_RESULT, zero columns).
// Pin the behavior so a future change to either parser or
// Connection::Query surfaces here loudly.
// ===========================================================================

TEST_CASE("V2: empty / whitespace-only SQL succeeds with an empty result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	for (const char *sql : {"", "   ", ";"}) {
		duckdb_v2_result_ptr r = nullptr;
		REQUIRE(duckdb_v2_connection_query(fx.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(r != nullptr);

		DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_SELECT;
		duckdb_v2_result_get_statement_type(r, &st, nullptr);
		REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_INVALID);

		idx_t cols = 99;
		duckdb_v2_result_column_count(r, &cols, nullptr);
		REQUIRE(cols == 0);

		duckdb_v2_result_destroy(&r);
	}
}

// ===========================================================================
// Multi-statement SQL: Connection::Query runs all statements but the
// returned handle is the first result; subsequent results live on its
// internal `next` chain and are not reachable through the V2 surface.
//
// A future result_next() that walks the chain is deliberately deferred:
// QueryResult::next is unique_ptr<QueryResult> (Stream / Pending /
// Materialized), so exposing it forces us to settle the streaming and
// pending result shapes first. Until then, callers that need every
// result split statements client-side and call connection_query per
// statement. Pin the current behavior so a shift in core surfaces here.
// ===========================================================================

TEST_CASE("V2: multi-statement SQL returns the first result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_ptr r = nullptr;
	REQUIRE(duckdb_v2_connection_query(fx.conn, "SELECT 1 AS first_col; SELECT 'last' AS last_col", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	idx_t cols = 0;
	duckdb_v2_result_column_count(r, &cols, nullptr);
	REQUIRE(cols == 1);

	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_result_column_name(r, 0, &name, &len, nullptr);
	REQUIRE(std::string(name, len) == "first_col");

	duckdb_v2_result_destroy(&r);
}
