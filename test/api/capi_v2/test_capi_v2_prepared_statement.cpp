#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

#include <string>
#include <vector>

// V2 prepared-statement tests: the opt-in cached execution path. Prepare once,
// execute repeatedly with positional parameters, introspect plan reuse. The
// result handle behaves identically to statement_execute's (streaming, drain,
// schema, statement/result type, rows-changed); the parity tests assert that.

namespace {

// Parse exactly one statement from sql (raw, unbound).
duckdb_v2_sql_statement_handle PsParseOne(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);
	return stmt;
}

// Prepare one statement from sql; returns the prepared handle (or nullptr on
// the require_cacheable failure path, which the caller then asserts).
duckdb_v2_prepared_statement_handle PsPrepare(duckdb_v2_connection_handle conn, const char *sql, bool require_cacheable,
                                              DUCKDB_V2_ERROR *out_rc = nullptr) {
	auto stmt = PsParseOne(conn, sql);
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	auto rc = duckdb_v2_statement_prepare(conn, stmt, require_cacheable, &prepared, nullptr);
	if (out_rc) {
		*out_rc = rc;
	}
	duckdb_v2_sql_statement_destroy(&stmt);
	return prepared;
}

bool PsReusesPlan(duckdb_v2_connection_handle conn, const char *sql) {
	auto prepared = PsPrepare(conn, sql, false);
	REQUIRE(prepared != nullptr);
	bool reuses = false;
	REQUIRE(duckdb_v2_prepared_reuses_plan(prepared, &reuses, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_prepared_statement_destroy(&prepared);
	return reuses;
}

// Collect all INT32 values of a single-column result into a vector, draining it.
std::vector<int32_t> DrainInt32Column(duckdb_v2_result_handle r) {
	std::vector<int32_t> out;
	while (auto chunk = V2StepChunk(r)) {
		idx_t size = 0;
		REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view {};
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (idx_t i = 0; i < size; i++) {
			out.push_back(reinterpret_cast<const int32_t *>(view.data)[i]);
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	return out;
}

// Read the single VARCHAR value from a one-row single-column result.
std::string DrainVarcharScalar(duckdb_v2_result_handle r) {
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	// duckdb_v2_bytes is transparent: a short value is inlined, a longer one lives
	// behind value.pointer.ptr.
	auto *strings = reinterpret_cast<const duckdb_v2_bytes *>(view.data);
	const auto &s = strings[0];
	uint32_t len = s.value.inlined.length;
	const char *data = len <= DUCKDB_V2_BYTES_INLINE_LENGTH ? s.value.inlined.inlined : s.value.pointer.ptr;
	std::string result(data, len);
	duckdb_v2_data_chunk_destroy(&chunk);
	auto trailing = V2StepChunk(r);
	if (trailing) {
		duckdb_v2_data_chunk_destroy(&trailing);
		FAIL("expected a single-row result");
	}
	return result;
}

duckdb_v2_value_handle Int32Value(int32_t v) {
	return V2Int32Value(v);
}

// Seed a 4-row table t(x INTEGER) with {1,2,3,4}.
void SeedTable(duckdb_v2_connection_handle conn) {
	V2ExecSQL(conn, "CREATE TABLE t(x INTEGER)");
	V2ExecSQL(conn, "INSERT INTO t VALUES (1),(2),(3),(4)");
}

} // namespace

// ---------------------------------------------------------------------------
// Prepare + execute-many with parameters (non-consuming)
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_execute runs the same handle repeatedly with params", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);
	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t WHERE x > $1 ORDER BY x", false);
	REQUIRE(prepared != nullptr);

	// First execution: x > 2 -> {3, 4}.
	auto v2 = Int32Value(2);
	duckdb_v2_result_handle r1 = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, &v2, 1, &r1, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainInt32Column(r1) == std::vector<int32_t> {3, 4});
	duckdb_v2_result_destroy(&r1);
	duckdb_v2_value_destroy(&v2);

	// Same handle, different value: x > 0 -> {1, 2, 3, 4}.
	auto v0 = Int32Value(0);
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, &v0, 1, &r2, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainInt32Column(r2) == std::vector<int32_t> {1, 2, 3, 4});
	duckdb_v2_result_destroy(&r2);
	duckdb_v2_value_destroy(&v0);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ---------------------------------------------------------------------------
// prepared_reuses_plan: the honest cache signal
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_reuses_plan reports true and false honestly", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);

	// Unparameterized: reuses.
	REQUIRE(PsReusesPlan(fx.conn, "SELECT 42") == true);
	// Type-anchored parameters, no table scan: reuses. A bare SELECT $1 + $2 does
	// not (types unknown at prepare time, so it re-binds every execution).
	REQUIRE(PsReusesPlan(fx.conn, "SELECT $1::INTEGER + $2::INTEGER") == true);
	REQUIRE(PsReusesPlan(fx.conn, "SELECT $1 + $2") == false);
	// Parameterized over a base table: does not reuse (table scan re-binds for
	// catalog freshness).
	REQUIRE(PsReusesPlan(fx.conn, "SELECT * FROM t WHERE x = $1") == false);
}

// ---------------------------------------------------------------------------
// require_cacheable: strict opt-in gate
// ---------------------------------------------------------------------------

TEST_CASE("V2: statement_prepare require_cacheable accepts a cacheable plan", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	auto prepared = PsPrepare(fx.conn, "SELECT $1::INTEGER + $2::INTEGER", true, &rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared != nullptr);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: statement_prepare require_cacheable rejects an uncacheable plan", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	auto prepared = PsPrepare(fx.conn, "SELECT * FROM t WHERE x = $1", true, &rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(prepared == nullptr);
}

// ---------------------------------------------------------------------------
// Parameter-binding correctness
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_execute binds positional parameters correctly", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $1::VARCHAR || $2::VARCHAR", false);
	REQUIRE(prepared != nullptr);

	duckdb_v2_value_handle a = V2VarcharValue("a");
	duckdb_v2_value_handle b = V2VarcharValue("b");
	duckdb_v2_value_handle values[2] = {a, b};

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainVarcharScalar(r) == "ab");
	duckdb_v2_result_destroy(&r);

	duckdb_v2_value_destroy(&a);
	duckdb_v2_value_destroy(&b);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: prepared_execute binds named parameters correctly", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $x::VARCHAR || $y::VARCHAR", false);
	REQUIRE(prepared != nullptr);

	duckdb_v2_value_handle a = V2VarcharValue("a");
	duckdb_v2_value_handle b = V2VarcharValue("b");

	// Bind by name with the arrays deliberately in the reverse order of the SQL: the
	// result follows the names, not the positions.
	duckdb_v2_str names[2] = {V2Str("y"), V2Str("x")};
	duckdb_v2_value_handle values[2] = {b, a}; // y=b, x=a -> $x || $y == "ab"
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainVarcharScalar(r) == "ab");
	duckdb_v2_result_destroy(&r);

	// A wrong key set (a name the statement does not declare) is a bind error.
	duckdb_v2_str wrong[2] = {V2Str("x"), V2Str("z")};
	duckdb_v2_value_handle wrong_values[2] = {a, b};
	REQUIRE(duckdb_v2_prepared_execute(prepared, wrong, wrong_values, 2, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);

	duckdb_v2_value_destroy(&a);
	duckdb_v2_value_destroy(&b);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ---------------------------------------------------------------------------
// Behavioral parity with statement_execute
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_execute rows match statement_execute", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);

	// Via statement_execute (positional value).
	auto stmt = PsParseOne(fx.conn, "SELECT x FROM t WHERE x > $1 ORDER BY x");
	auto v2a = Int32Value(2);
	duckdb_v2_result_handle re = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, &v2a, 1, &re, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stateless_rows = DrainInt32Column(re);
	duckdb_v2_result_destroy(&re);
	duckdb_v2_value_destroy(&v2a);
	duckdb_v2_sql_statement_destroy(&stmt);

	// Via prepared_execute.
	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t WHERE x > $1 ORDER BY x", false);
	auto v2b = Int32Value(2);
	duckdb_v2_result_handle rp = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, &v2b, 1, &rp, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto prepared_rows = DrainInt32Column(rp);
	duckdb_v2_result_destroy(&rp);
	duckdb_v2_value_destroy(&v2b);
	duckdb_v2_prepared_statement_destroy(&prepared);

	REQUIRE(prepared_rows == stateless_rows);
	REQUIRE(prepared_rows == std::vector<int32_t> {3, 4});
}

TEST_CASE("V2: prepared_execute result metadata matches statement_execute", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER, b VARCHAR)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (1, 'x')");

	auto prepared = PsPrepare(fx.conn, "SELECT a, b FROM t", false);
	REQUIRE(prepared != nullptr);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Schema, statement type, result type all available before the first step.
	REQUIRE(V2ColumnCount(r) == 2);
	V2RequireColumn(r, 0, "a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	V2RequireColumn(r, 1, "b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	DUCKDB_V2_STATEMENT_TYPE stmt_type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &stmt_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt_type == DUCKDB_V2_STATEMENT_TYPE_SELECT);
	DUCKDB_V2_RESULT_TYPE result_type = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(r, &result_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_type == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);

	duckdb_v2_result_destroy(&r);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: prepared_execute of a DML reports rows-changed like statement_execute",
          "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(x INTEGER)");

	auto prepared = PsPrepare(fx.conn, "INSERT INTO t VALUES ($1)", false);
	REQUIRE(prepared != nullptr);

	auto v99 = Int32Value(99);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, &v99, 1, &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// CHANGED_ROWS result type, and drain reports one affected row.
	DUCKDB_V2_RESULT_TYPE result_type = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(r, &result_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);
	idx_t rows_changed = 0;
	REQUIRE(duckdb_v2_result_drain(r, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows_changed == 1);
	duckdb_v2_result_destroy(&r);

	// Execute again: both inserts applied (non-consuming, committed by the engine's
	// own per-query transaction).
	auto v7 = Int32Value(7);
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, &v7, 1, &r2, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(r2);
	duckdb_v2_result_destroy(&r2);

	duckdb_v2_value_destroy(&v99);
	duckdb_v2_value_destroy(&v7);
	duckdb_v2_prepared_statement_destroy(&prepared);

	duckdb_v2_result_handle count = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM t", &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainChangedRows(count) == 2);
	duckdb_v2_result_destroy(&count);
}

// ---------------------------------------------------------------------------
// Lifecycle: non-consuming, destroy semantics, result outlives prepared handle
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_statement_destroy is null-safe and idempotent", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT 1", false);
	REQUIRE(prepared != nullptr);
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared == nullptr);
	// Second destroy on the nulled slot is a no-op.
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
	// Null pointer is a no-op.
	REQUIRE(duckdb_v2_prepared_statement_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: a result outlives its prepared handle", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);
	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t ORDER BY x", false);
	REQUIRE(prepared != nullptr);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Destroy the prepared handle while its result is live; the result keeps the
	// context alive and stays drainable.
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainInt32Column(r) == std::vector<int32_t> {1, 2, 3, 4});
	duckdb_v2_result_destroy(&r);
}

// ---------------------------------------------------------------------------
// One live result per connection (shared busy slot)
// ---------------------------------------------------------------------------

TEST_CASE("V2: prepared_execute refuses while a live result exists", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);

	// Prepare before the live result (preparing while one is live is itself
	// refused; see the dedicated case below).
	auto prepared = PsPrepare(fx.conn, "SELECT 1", false);
	REQUIRE(prepared != nullptr);

	// A live statement_execute result holds the connection.
	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT x FROM t", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle blocked = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &blocked, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(blocked == nullptr);

	// Drain and destroy the live result: now prepared_execute succeeds.
	V2DrainRowCount(live);
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &blocked, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_destroy(&blocked);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: statement_prepare refuses while a live result exists", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);

	// A live result holds the connection; preparing would run InitialCleanup and
	// cancel it, so statement_prepare refuses instead of clobbering.
	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT x FROM t", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto stmt = PsParseOne(fx.conn, "SELECT 1");
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	REQUIRE(duckdb_v2_statement_prepare(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(prepared == nullptr);

	// After the live result is gone, preparing the same statement succeeds.
	V2DrainRowCount(live);
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_statement_prepare(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared != nullptr);
	duckdb_v2_prepared_statement_destroy(&prepared);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: a live prepared result blocks statement_execute", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	SeedTable(fx.conn);
	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t", false);
	REQUIRE(prepared != nullptr);
	duckdb_v2_result_handle live = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle blocked = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &blocked, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(blocked == nullptr);

	duckdb_v2_result_destroy(&live);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ---------------------------------------------------------------------------
// Busy-slot release on the throwing execute path: a failed execute must not
// leave the connection permanently busy.
// ---------------------------------------------------------------------------

TEST_CASE("V2: a prepared_execute that errors frees the connection", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(x INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (0)");

	// Division by zero errors during execution-prepare (constant-folded at bind).
	auto prepared = PsPrepare(fx.conn, "SELECT 1 / (x - x) FROM t", false);
	REQUIRE(prepared != nullptr);
	duckdb_v2_result_handle r = nullptr;
	auto rc = duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, &r, nullptr);
	if (rc == DUCKDB_V2_ERROR_NONE) {
		// The error may surface lazily while stepping; drain to force it.
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		while (duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE) {
			if (chunk) {
				duckdb_v2_data_chunk_destroy(&chunk);
			}
			if (status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED || status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED) {
				break;
			}
			if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
				duckdb_v2_result_wait(r, nullptr);
			}
		}
	}
	duckdb_v2_result_destroy(&r);
	duckdb_v2_prepared_statement_destroy(&prepared);

	// The slot must be free: a subsequent statement_execute on the same
	// connection succeeds.
	duckdb_v2_result_handle after = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(after);
	duckdb_v2_result_destroy(&after);
}

// ---------------------------------------------------------------------------
// Null-argument guards and prepare-error routing
// ---------------------------------------------------------------------------

TEST_CASE("V2: statement_prepare guards null arguments", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	auto stmt = PsParseOne(fx.conn, "SELECT 1");
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	REQUIRE(duckdb_v2_statement_prepare(nullptr, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(prepared == nullptr);
	REQUIRE(duckdb_v2_statement_prepare(fx.conn, nullptr, false, &prepared, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(prepared == nullptr);
	REQUIRE(duckdb_v2_statement_prepare(fx.conn, stmt, false, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: prepared_execute guards null arguments", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $1::INTEGER", false);
	REQUIRE(prepared != nullptr);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_execute(nullptr, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 0, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	// A positive count with a null value array is refused.
	REQUIRE(duckdb_v2_prepared_execute(prepared, nullptr, nullptr, 1, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: statement_prepare surfaces a catalog error", "[capi_v2][prepared_statement]") {
	V2EnvFixture fx;
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	auto prepared = PsPrepare(fx.conn, "SELECT * FROM no_such_table", false, &rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(prepared == nullptr);
}
