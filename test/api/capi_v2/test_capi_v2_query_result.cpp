#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// V2 query_result tests: streaming execution, schema, and metadata.
// Row-data decoding lives on the data_chunk + vector surfaces and is
// exercised separately.
//
// Tests drive the step primitive (result_step / result_wait, via the
// V2StepChunk / V2DrainRowCount helpers) wherever possible; the
// result_fetch_chunk convenience has its own focused tests.
//
// INVARIANTS THIS FILE RELIES ON:
//   - duckdb_v2_result_handle is a heap-allocated duckdb::ResultWrapperV2
//     cast to the handle type. The bridge's ToResult cast and destructor
//     depend on this layout.
//   - duckdb_v2_schema_handle returned by result_get_schema is owned (caller
//     destroys via schema_destroy). Its lifetime is independent of the result:
//     it is a snapshot, so it stays valid after the result is destroyed.
//   - Names and types borrowed via schema_get_field are valid until the
//     schema is destroyed.
//   - Chunks returned by step / fetch_chunk are owned and independent
//     of the producing result / connection / database.
// ---------------------------------------------------------------------------

namespace {} // namespace

// ===========================================================================
// Smoke: SELECT round-trip, the canonical "SELECT 1" check.
// ===========================================================================

TEST_CASE("V2: statement_execute SELECT returns QUERY_RESULT with INTEGER column", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1 AS one", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	REQUIRE(V2ColumnCount(r) == 1);
	V2RequireColumn(r, 0, "one", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	REQUIRE(V2DrainRowCount(r) == 1);

	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
}

// ===========================================================================
// Multi-column SELECT: column count, names, and types.
// ===========================================================================

TEST_CASE("V2: statement_execute multi-column SELECT", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1::INTEGER AS a, 'hi' AS b, 3.14::DOUBLE AS c", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	REQUIRE(V2ColumnCount(r) == 3);

	struct {
		const char *name;
		DUCKDB_V2_LOGICAL_TYPE_ID id;
	} expected[] = {
	    {"a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {"b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR},
	    {"c", DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE},
	};
	for (idx_t i = 0; i < 3; i++) {
		V2RequireColumn(r, i, expected[i].name, expected[i].id);
	}

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// The step loop over a multi-chunk SELECT: WAITING is transient, chunks
// are caller-owned, FINISHED is sticky, and out_chunk is written iff the
// status is CHUNK.
// ===========================================================================

TEST_CASE("V2: step loop drains a multi-chunk SELECT", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t total_rows = 0;
	idx_t chunk_count = 0;
	while (true) {
		// Poison the out-param to prove step nulls it on non-CHUNK
		// statuses.
		auto poison = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		duckdb_v2_data_chunk_handle chunk = poison;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
			REQUIRE(chunk != nullptr);
			REQUIRE(chunk != poison);
			idx_t size = 0;
			duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
			REQUIRE(size > 0);
			total_rows += size;
			chunk_count++;
			duckdb_v2_data_chunk_destroy(&chunk);
			continue;
		}
		REQUIRE(chunk == nullptr); // nulled on every non-CHUNK status
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			continue; // repeated stepping drives the query forward
		}
		REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED);
		break;
	}
	REQUIRE(total_rows == 100000);
	REQUIRE(chunk_count > 1); // genuinely multi-chunk

	// FINISHED is sticky and keeps nulling out_chunk.
	for (int i = 0; i < 3; i++) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED);
		REQUIRE(chunk == nullptr);
	}

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Metadata is valid before the first step, during consumption, and after
// the stream ends; it is prepare-time information.
// ===========================================================================

TEST_CASE("V2: metadata valid before, during, and after consumption", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i AS steady FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto check_metadata = [&](const char *phase) {
		INFO("phase: " << phase);
		DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
		REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
		DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
		REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);
		REQUIRE(V2ColumnCount(r) == 1);
		V2RequireColumn(r, 0, "steady", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	};

	// Before the first step: nothing has executed yet.
	check_metadata("before first step");

	// During: consume one chunk, then re-check.
	idx_t consumed = 0;
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	consumed += size;
	duckdb_v2_data_chunk_destroy(&chunk);
	check_metadata("mid-stream");

	// After FINISHED.
	consumed += V2DrainRowCount(r);
	REQUIRE(consumed == 100000);
	check_metadata("after FINISHED");

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// DDL: CREATE / DROP report NOTHING; the stream yields no rows.
// ===========================================================================

TEST_CASE("V2: DDL returns NOTHING and an empty stream", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "CREATE TABLE u (i INTEGER)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	duckdb_v2_result_get_result_type(r, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_NOTHING);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(r, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_CREATE);

	REQUIRE(V2DrainRowCount(r) == 0);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// DML: INSERT / UPDATE / DELETE report CHANGED_ROWS; the changed-row count
// arrives as the stream's single BIGINT "Count" chunk.
// ===========================================================================

TEST_CASE("V2: INSERT / UPDATE / DELETE stream the Count chunk", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	// INSERT three rows.
	duckdb_v2_result_handle ins = nullptr;
	REQUIRE(V2Query(fx.conn, "INSERT INTO t VALUES (1), (2), (3)", &ins, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(ins, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(ins, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_INSERT);

	REQUIRE(V2DrainChangedRows(ins) == 3);
	duckdb_v2_result_destroy(&ins);

	// UPDATE two rows.
	duckdb_v2_result_handle upd = nullptr;
	V2Query(fx.conn, "UPDATE t SET i = i + 10 WHERE i >= 2", &upd, nullptr);
	duckdb_v2_result_get_statement_type(upd, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_UPDATE);
	REQUIRE(V2DrainChangedRows(upd) == 2);
	duckdb_v2_result_destroy(&upd);

	// DELETE one row.
	duckdb_v2_result_handle del = nullptr;
	V2Query(fx.conn, "DELETE FROM t WHERE i = 1", &del, nullptr);
	duckdb_v2_result_get_statement_type(del, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_DELETE);
	REQUIRE(V2DrainChangedRows(del) == 1);
	duckdb_v2_result_destroy(&del);
}

TEST_CASE("V2: CHANGED_ROWS Count chunk reports 0 when WHERE matches nothing", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	V2Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &ins, nullptr);
	REQUIRE(V2DrainChangedRows(ins) == 2);
	duckdb_v2_result_destroy(&ins);

	duckdb_v2_result_handle upd = nullptr;
	REQUIRE(V2Query(fx.conn, "UPDATE t SET i = i WHERE 1=0", &upd, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(upd, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);
	REQUIRE(V2DrainChangedRows(upd) == 0);
	duckdb_v2_result_destroy(&upd);

	duckdb_v2_result_handle del = nullptr;
	V2Query(fx.conn, "DELETE FROM t WHERE 1=0", &del, nullptr);
	REQUIRE(V2DrainChangedRows(del) == 0);
	duckdb_v2_result_destroy(&del);
}

// ===========================================================================
// Result schema on CHANGED_ROWS / NOTHING: the synthetic Count column is
// visible via the column accessors, before any step.
// ===========================================================================

TEST_CASE("V2: CHANGED_ROWS exposes synthetic Count column via column accessors", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	V2Query(fx.conn, "INSERT INTO t VALUES (1)", &ins, nullptr);

	REQUIRE(V2ColumnCount(ins) == 1);
	// The synthetic column name is set by core; pin the current spelling
	// so a future rename surfaces here loud rather than silent.
	V2RequireColumn(ins, 0, "Count", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	duckdb_v2_result_destroy(&ins);
}

TEST_CASE("V2: NOTHING result exposes the synthetic Count column with zero rows", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	V2Query(fx.conn, "CREATE TABLE t (i INTEGER)", &r, nullptr);

	REQUIRE(V2ColumnCount(r) == 1);
	V2RequireColumn(r, 0, "Count", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	// The stream itself yields no rows.
	REQUIRE(V2DrainRowCount(r) == 0);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_fetch_chunk: the blocking convenience. Chunk, then NULL at
// end-of-stream, idempotently.
// ===========================================================================

TEST_CASE("V2: result_fetch_chunk drains and reports end-of-stream as NULL", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t total_rows = 0;
	idx_t chunk_count = 0;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_result_fetch_chunk(r, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (!chunk) {
			break; // end-of-stream
		}
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		total_rows += size;
		chunk_count++;
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	REQUIRE(total_rows == 100000);
	REQUIRE(chunk_count > 1);

	// End-of-stream is sticky.
	for (int i = 0; i < 3; i++) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_result_fetch_chunk(r, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(chunk == nullptr);
	}

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_wait: unblocks stepping; no-op on terminal states.
// ===========================================================================

TEST_CASE("V2: result_wait makes the step loop progress without busy-spinning", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// V2StepChunk waits between steps; draining the full stream through
	// it proves wait never wedges and stepping always becomes actionable.
	idx_t total_rows = 0;
	while (auto chunk = V2StepChunk(r)) {
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		total_rows += size;
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	REQUIRE(total_rows == 100000);

	// Terminal state: wait is a no-op, never an error.
	for (int i = 0; i < 3; i++) {
		REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_wait before the first step is safe", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Nothing has executed yet; wait must return once a task is executable
	// (which is immediately, right after the pending query is created).
	REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Error paths at statement_execute time: prepare runs eagerly, so parser
// and binder errors surface here, not from the steps.
// ===========================================================================

TEST_CASE("V2: statement_execute surfaces parser error and leaves out_result null", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(V2Query(fx.conn, "SELEKT 1", &r, &err) == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);

	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(msg.len > 0); // detail propagated from the parser
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: statement_execute binder error (unknown table)", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT * FROM no_such_table", &r, &err) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: statement_execute failure tolerates err == nullptr", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "BADSQL", &r, nullptr) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
}

// ===========================================================================
// statement_execute takes exactly one parsed statement: a NULL statement
// handle (the iterator's end marker, or a no-statement input) is
// rejected. Multi-statement strings are handled by the iterator; see the
// sql_statement tests.
// ===========================================================================

TEST_CASE("V2: querying with a NULL statement is rejected", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// No-statement input parses to an immediately exhausted iterator; the
	// NULL statement it yields is rejected by statement_execute.
	for (const char *sql : {"", "   ", ";"}) {
		INFO("sql: '" << sql << "'");
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(r == nullptr);
	}
}

// ===========================================================================
// Error mid-stream: an execution-time failure surfaces from the steps as
// a sticky error with nulled out-params.
// ===========================================================================

TEST_CASE("V2: execution error mid-stream is sticky", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// The cast only fails on the last row, so the failure is raised during
	// execution, after statement_execute has returned successfully.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn,
	                "SELECT (CASE WHEN i < 99999 THEN CAST(i AS VARCHAR) ELSE 'oops' END)::INT FROM range(100000) t(i)",
	                &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	// Step until the error surfaces; chunks may legitimately arrive first.
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_handle err = nullptr;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(r, &chunk, &status, &err);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			REQUIRE(chunk == nullptr); // nulled on failure
			break;
		}
		REQUIRE(status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED); // must fail before finishing
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);

	// Sticky: subsequent steps report the same code without touching the
	// (closed) internals; fetch_chunk reports it too.
	for (int i = 0; i < 2; i++) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
		REQUIRE(chunk == nullptr);
	}
	duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
	REQUIRE(duckdb_v2_result_fetch_chunk(r, &chunk, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(chunk == nullptr);

	// Metadata still works after the failure (prepare-time information).
	REQUIRE(V2ColumnCount(r) == 1);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Interrupt / cancellation. Decision 6: step reports the event as
// status CANCELLED; fetch_chunk reports it as ERROR_RUNTIME_INTERRUPT.
// ===========================================================================

TEST_CASE("V2: interrupt between steps surfaces as sticky CANCELLED status", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Consume the first chunk so the stream is genuinely mid-flight.
	auto first = V2StepChunk(r);
	REQUIRE(first != nullptr);
	duckdb_v2_data_chunk_destroy(&first);

	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The next step that touches the engine observes the interrupt.
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000 && status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);

	// Sticky, and out_chunk keeps being nulled.
	for (int i = 0; i < 3; i++) {
		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);
		REQUIRE(chunk == nullptr);
	}

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: interrupt during the pending phase maps to CANCELLED", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Interrupt lands before the first step, i.e. while the result is
	// still in the pending phase (where the engine reports the interrupt
	// as an execution error of type INTERRUPT; the bridge maps it to the
	// CANCELLED status for consistency).
	V2Result r;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000 && status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(status != DUCKDB_V2_RESULT_STEP_STATUS_CHUNK); // no chunk can precede the interrupt
	}
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: fetch_chunk reports cancellation as ERROR_RUNTIME_INTERRUPT", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_fetch_chunk(r, &chunk, &err) == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	REQUIRE(chunk == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Same event, two channels: the step channel reports the sticky
	// CANCELLED status, the fetch channel keeps reporting the error.
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);
	REQUIRE(duckdb_v2_result_fetch_chunk(r, &chunk, nullptr) == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	REQUIRE(chunk == nullptr);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: interrupt from a second thread cancels a blocked fetch", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// A heavy aggregation: a single result row that takes long enough to
	// compute that the interrupt lands mid-execution.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT sum(i) FROM range(2000000000) t(i) WHERE i % 3 <> 0", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	std::thread interrupter([&]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	});

	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_result_fetch_chunk(r, &chunk, &err);
	interrupter.join();

	// The interrupt must beat the multi-second aggregation. Capture the
	// error detail so a wrong code identifies the exception that escaped.
	duckdb_v2_str err_text = {nullptr, 0};
	if (err) {
		duckdb_v2_error_info_get_text(err, &err_text);
	}
	auto err_str = V2StrTo(err_text);
	INFO("fetch_chunk error detail: " << (!err_str.empty() ? err_str : "(none)"));
	REQUIRE(rc == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	REQUIRE(chunk == nullptr);

	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: interrupt with no active query is a no-op", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The stale interrupt flag must not poison the next query (the engine
	// resets it when a new query starts).
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 42", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// connection_query_progress.
// ===========================================================================

TEST_CASE("V2: query_progress reports idle values when no query is active", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	auto progress = V2ReadProgress(fx.conn);
	REQUIRE(progress.percentage == -1.0);
	REQUIRE(progress.rows_processed == 0);
	REQUIRE(progress.total_rows_to_process == 0);

	// The snapshot destructor is null-safe.
	REQUIRE(duckdb_v2_query_progress_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_query_progress_handle already_null = nullptr;
	REQUIRE(duckdb_v2_query_progress_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: query_progress advances while stepping a query", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Single-threaded so all execution happens in our steps, and the
	// progress bar enabled so the engine publishes progress at all.
	for (const char *setup_sql : {"SET threads=1", "CREATE TABLE tbl AS SELECT range a FROM range(1000000)",
	                              "SET enable_progress_bar=true", "SET enable_progress_bar_print=false"}) {
		duckdb_v2_result_handle setup = nullptr;
		REQUIRE(V2Query(fx.conn, setup_sql, &setup, nullptr) == DUCKDB_V2_ERROR_NONE);
		V2DrainRowCount(setup);
		duckdb_v2_result_destroy(&setup);
	}

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT sum(a) FROM tbl", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Progress restarts at 0 when the query begins and advances as steps
	// drive execution. Each snapshot is an independent owned object.
	V2Progress progress;
	bool saw_progress = false;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		progress = V2ReadProgress(fx.conn);
		if (progress.percentage > 0.0) {
			saw_progress = true;
		}
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			break;
		}
	}
	REQUIRE(saw_progress);
	REQUIRE(progress.rows_processed <= progress.total_rows_to_process);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Drain into a ColumnDataCollection: the deferred result_materialize
// convenience is fully expressible with the primitives.
// ===========================================================================

TEST_CASE("V2: drain a stream into a column data collection and scan it back", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// The collection needs a context to allocate through; borrow it via
	// execute_with_context. The collection outlives the callback.
	duckdb_v2_column_data_collection_handle cdc = nullptr;
	REQUIRE(duckdb_v2_connection_execute_with_context(
	            fx.conn,
	            [](duckdb_v2_context_handle ctx, void *user_data, duckdb_v2_error_info_handle *err) {
		            auto out = static_cast<duckdb_v2_column_data_collection_handle *>(user_data);
		            duckdb_v2_logical_type_handle bigint = nullptr;
		            duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint, err);
		            duckdb_v2_column_data_collection_create(ctx, &bigint, 1, out, err);
		            duckdb_v2_logical_type_destroy(&bigint);
	            },
	            &cdc, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cdc != nullptr);

	duckdb_v2_column_data_collection_append_state_handle append_state = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_append_state_create(cdc, &append_state, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	while (auto chunk = V2StepChunk(r)) {
		REQUIRE(duckdb_v2_column_data_collection_append(cdc, append_state, chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	duckdb_v2_result_destroy(&r);
	duckdb_v2_column_data_collection_append_state_destroy(&append_state);

	idx_t row_count = 0;
	REQUIRE(duckdb_v2_column_data_collection_row_count(cdc, &row_count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(row_count == 10000);

	// Scan it back and re-count.
	duckdb_v2_column_data_collection_scan_state_handle scan_state = nullptr;
	REQUIRE(duckdb_v2_column_data_collection_scan_state_create(cdc, &scan_state, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle bigint = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint, nullptr);
	duckdb_v2_data_chunk_handle scan_chunk = nullptr;
	REQUIRE(duckdb_v2_data_chunk_create(&bigint, 1, &scan_chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&bigint);

	idx_t scanned_rows = 0;
	while (true) {
		bool did_produce = false;
		REQUIRE(duckdb_v2_column_data_collection_scan(cdc, scan_state, scan_chunk, &did_produce, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		if (!did_produce) {
			break;
		}
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(scan_chunk, &size, nullptr);
		scanned_rows += size;
	}
	REQUIRE(scanned_rows == 10000);

	duckdb_v2_data_chunk_destroy(&scan_chunk);
	duckdb_v2_column_data_collection_scan_state_destroy(&scan_state);
	duckdb_v2_column_data_collection_destroy(&cdc);
}

// ===========================================================================
// Teardown discipline.
// ===========================================================================

TEST_CASE("V2: destroying a never-stepped result is clean", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(1000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The connection remains fully usable.
	duckdb_v2_result_handle next = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}

TEST_CASE("V2: destroying a half-consumed result is clean", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(1000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle next = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}

TEST_CASE("V2: a fetched chunk outlives result, connection, and database", "[capi_v2][query_result]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, "SELECT i, 'row-' || i AS s FROM range(100) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);

	duckdb_v2_result_destroy(&r);
	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);

	// The chunk owns its data; producers are all gone.
	idx_t size = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 100);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	REQUIRE(reinterpret_cast<const int64_t *>(view.data)[99] == 99);

	duckdb_v2_data_chunk_destroy(&chunk);
}

// ===========================================================================
// Lifetime: result_get_schema is a self-contained snapshot that outlives the
// result it came from. (Borrow stability and out-of-range indices are covered
// by the schema-type tests in test_capi_v2_schema.cpp.)
// ===========================================================================

TEST_CASE("V2: result_get_schema outlives the result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	V2Query(fx.conn, "SELECT 1 AS only_column", &r, nullptr);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_destroy(&r);

	// The schema is self-contained; it stays valid after the result is gone.
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle lt = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2StrTo(name) == "only_column");
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(lt, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_schema_destroy(&schema);
}

// ===========================================================================
// Null-arg validation across the surface.
// ===========================================================================

TEST_CASE("V2: statement_execute null-arg rejection", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Exercise the real duckdb_v2_str signature. A {NULL, 0} sql is a valid
	// empty view; the malformed case is a null pointer with a nonzero length.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(nullptr, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(V2Query(fx.conn, nullptr, &r, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(V2Query(fx.conn, "SELECT 1", nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: result_destroy is null-safe", "[capi_v2][query_result]") {
	REQUIRE(duckdb_v2_result_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle already_null = nullptr;
	REQUIRE(duckdb_v2_result_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

TEST_CASE("V2: result accessors reject null handle and null out-params", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	DUCKDB_V2_RESULT_TYPE rt;
	DUCKDB_V2_STATEMENT_TYPE st;
	duckdb_v2_schema_handle schema = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status;

	REQUIRE(duckdb_v2_result_get_result_type(nullptr, &rt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_statement_type(nullptr, &st, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_schema(nullptr, &schema, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_step(nullptr, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_fetch_chunk(nullptr, &chunk, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_wait(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_interrupt(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	double pct;
	uint64_t rows, total;
	duckdb_v2_query_progress_handle progress = nullptr;
	REQUIRE(duckdb_v2_connection_query_progress(nullptr, &progress, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_query_progress_get_percentage(nullptr, &pct, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_query_progress_get_rows_processed(nullptr, &rows, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_query_progress_get_total_rows_to_process(nullptr, &total, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_result_handle r = nullptr;
	V2Query(fx.conn, "SELECT 1", &r, nullptr);
	REQUIRE(duckdb_v2_result_get_result_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_statement_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_get_schema(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_step(r, nullptr, &status, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_step(r, &chunk, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_result_fetch_chunk(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_query_progress(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_connection_query_progress(fx.conn, &progress, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_query_progress_get_percentage(progress, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_query_progress_get_rows_processed(progress, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_query_progress_get_total_rows_to_process(progress, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_query_progress_destroy(&progress);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// Belt-and-braces: a succeeding call leaves a pre-existing *err untouched.
// ===========================================================================

TEST_CASE("V2: statement_execute leaves pre-existing err untouched on success", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(V2Query(fx.conn, "BADSQL", &r, &err) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(err != nullptr);

	REQUIRE(V2Query(fx.conn, "SELECT 1", &r, &err) == DUCKDB_V2_ERROR_NONE);
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

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

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
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, c.sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
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
// instead return a string and this assertion will fire, signalling that
// DUCKDB_V2_STATEMENT_TYPE in api_spec/v2/query_result/query_result.yaml
// needs a matching id.
// ===========================================================================

TEST_CASE("V2: STATEMENT_TYPE has no gaps vs duckdb::StatementType", "[capi_v2][query_result]") {
	constexpr auto highest_known = static_cast<uint8_t>(duckdb::StatementType::DISCONNECT_STATEMENT);
	auto probe = static_cast<duckdb::StatementType>(highest_known + 1);
	REQUIRE_THROWS_AS(duckdb::EnumUtil::ToString(probe), duckdb::NotImplementedException);
}

// ===========================================================================
// Results don't alias each other's storage, and a terminal (drained)
// result keeps its metadata usable while later queries run.
// ===========================================================================

TEST_CASE("V2: results are independent; destroying one leaves the other usable", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle a = nullptr;
	duckdb_v2_result_handle b = nullptr;
	V2Query(fx.conn, "SELECT 1 AS aa", &a, nullptr);
	// a must reach a terminal state before the connection accepts b.
	REQUIRE(V2DrainRowCount(a) == 1);
	V2Query(fx.conn, "SELECT 'hi' AS bb", &b, nullptr);

	V2RequireColumn(a, 0, "aa", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	V2RequireColumn(b, 0, "bb", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	// Destroy a; b's schema is still fetchable.
	duckdb_v2_result_destroy(&a);
	V2RequireColumn(b, 0, "bb", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	duckdb_v2_result_destroy(&b);
}

// ===========================================================================
// One live result per connection: statement_execute refuses with
// RESOURCE_IN_USE while a live result exists, and the connection is
// freed by drain, destroy, interrupt-then-step, or a sticky error.
// ===========================================================================

TEST_CASE("V2: statement_execute refuses while a live result exists", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Refused while live, with detail; the live result is untouched.
	duckdb_v2_result_handle second = reinterpret_cast<duckdb_v2_result_handle>(uintptr_t(0xdead));
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &second, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(second == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(V2StrTo(msg).find("live result") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);

	// Consuming the live result still works after the refusal.
	auto chunk = V2StepChunk(live);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);

	// Draining to FINISHED frees the connection; the handle may still be
	// alive (metadata stays usable).
	while ((chunk = V2StepChunk(live))) {
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	REQUIRE(V2Query(fx.conn, "SELECT 2", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
	duckdb_v2_result_destroy(&live);
}

TEST_CASE("V2: destroying an undrained result frees the connection", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle second = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);

	duckdb_v2_result_destroy(&live);
	REQUIRE(V2Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
}

TEST_CASE("V2: a cancelled result frees the connection once the step observes it", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2Result live;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The slot is released on the terminal transition, i.e. when a step
	// observes the cancellation, not by the interrupt itself.
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000 && status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_step(live, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);

	V2Result second;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
	duckdb_v2_result_destroy(&live);
}

TEST_CASE("V2: a sticky execution error frees the connection", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 'oops'::INT FROM range(10) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(live, &chunk, &status, nullptr);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_TYPE_CONVERSION);

	duckdb_v2_result_handle second = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
	duckdb_v2_result_destroy(&live);
}

TEST_CASE("V2: a busy connection does not affect a second connection", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	duckdb_v2_connection_handle conn2 = nullptr;
	REQUIRE(duckdb_v2_connect(fx.db, &conn2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle other = nullptr;
	REQUIRE(V2Query(conn2, "SELECT 1", &other, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(other) == 1);
	duckdb_v2_result_destroy(&other);

	duckdb_v2_result_destroy(&live);
	duckdb_v2_disconnect(&conn2);
}

// ===========================================================================
// result_drain: run to completion, report the changed-row count.
// ===========================================================================

namespace {
// Query + drain + destroy, returning the drained changed-row count.
idx_t V2QueryAndDrain(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t rows_changed = 99;
	REQUIRE(duckdb_v2_result_drain(r, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_destroy(&r);
	return rows_changed;
}
} // namespace

TEST_CASE("V2: result_drain applies side effects and reports rows changed", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	REQUIRE(V2QueryAndDrain(fx.conn, "CREATE TABLE t (i INTEGER)") == 0); // NOTHING result
	REQUIRE(V2QueryAndDrain(fx.conn, "INSERT INTO t VALUES (1), (2), (3)") == 3);
	REQUIRE(V2QueryAndDrain(fx.conn, "UPDATE t SET i = i WHERE 1=0") == 0);

	// Rows of a row-producing result are discarded; 0 rows changed.
	REQUIRE(V2QueryAndDrain(fx.conn, "SELECT i FROM range(100000) t(i)") == 0);

	// The side effects are visible: the drained INSERT really ran.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 3);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_drain edge and error paths", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	idx_t rows_changed = 99;
	duckdb_v2_error_info_handle err = nullptr;

	// Execution-time error propagates with its type and is sticky.
	duckdb_v2_result_handle bad = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 'oops'::INT FROM range(10) t(i)", &bad, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_drain(bad, &rows_changed, &err) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(duckdb_v2_result_drain(bad, &rows_changed, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	duckdb_v2_result_destroy(&bad);

	// Draining an already drained (FINISHED) result succeeds with 0: the
	// Count chunk was consumed by the first drain.
	duckdb_v2_result_handle ins = nullptr;
	REQUIRE(V2Query(fx.conn, "CREATE TABLE t (i INTEGER)", &ins, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_drain(ins, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_destroy(&ins);
	REQUIRE(V2Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &ins, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_drain(ins, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows_changed == 2);
	REQUIRE(duckdb_v2_result_drain(ins, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows_changed == 0);
	duckdb_v2_result_destroy(&ins);

	// Cancellation surfaces as RUNTIME_INTERRUPT, like fetch_chunk.
	duckdb_v2_result_handle live = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_drain(live, &rows_changed, nullptr) == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	duckdb_v2_result_destroy(&live);

	// Null-arg rejection.
	REQUIRE(duckdb_v2_result_drain(nullptr, &rows_changed, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_drain(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_result_destroy(&r);

	// Draining released the connection: a fresh query still works.
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_wait is guarded against engine state the wrapper doesn't
// control: an interrupted-but-not-yet-observed result must not hang or
// surface an engine INTERNAL error.
// ===========================================================================

TEST_CASE("V2: result_wait after interrupt is clean", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2Result r;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Wait must return (the engine wakes waiters on interrupt) without an
	// INTERNAL error; the next steps then observe CANCELLED.
	duckdb_v2_error_info_handle err = nullptr;
	auto wait_rc = duckdb_v2_result_wait(r, &err);
	REQUIRE(wait_rc != DUCKDB_V2_ERROR_RUNTIME_INTERNAL);
	duckdb_v2_error_info_destroy(&err);

	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000 && status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);

	// And wait on the now-terminal result stays a no-op.
	REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_fetch_chunk on a CHANGED_ROWS result: the convenience path
// yields the Count chunk, then end-of-stream.
// ===========================================================================

TEST_CASE("V2: result_fetch_chunk drains a CHANGED_ROWS result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	REQUIRE(V2Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &ins, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_result_fetch_chunk(ins, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	REQUIRE(view.data != nullptr);
	REQUIRE(reinterpret_cast<const int64_t *>(view.data)[0] == 2);
	duckdb_v2_data_chunk_destroy(&chunk);

	REQUIRE(duckdb_v2_result_fetch_chunk(ins, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk == nullptr); // end-of-stream

	duckdb_v2_result_destroy(&ins);
}

// ===========================================================================
// Disconnecting while a result is alive but undrained: the result's
// internals keep the client context alive, so the handle stays safe to
// consume and destroy after the connection (and database) handles are
// gone.
// ===========================================================================

TEST_CASE("V2: an undrained result survives disconnect and close", "[capi_v2][query_result]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);

	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);

	// Metadata still reads off the wrapper.
	REQUIRE(V2ColumnCount(r) == 1);

	// Consumption keeps working (the stream holds the context alive) or
	// fails cleanly; either way destroy must be safe afterwards.
	idx_t drained = 0;
	while (true) {
		duckdb_v2_data_chunk_handle c = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		auto rc = duckdb_v2_result_step(r, &c, &status, nullptr);
		if (rc != DUCKDB_V2_ERROR_NONE || status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED) {
			break;
		}
		if (c) {
			idx_t size = 0;
			duckdb_v2_data_chunk_get_size(c, &size, nullptr);
			drained += size;
			duckdb_v2_data_chunk_destroy(&c);
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			break;
		}
	}
	(void)drained;
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_destroy_environment(&env);
}

// ===========================================================================
// Statement expansion: preprocessing can turn one user statement into a
// group of engine statements ("fragments"). The group executes as one
// result; selection mirrors ClientContext::Query (first row-producing
// fragment, else the last fragment), and metadata is deferred until the
// principal fragment is prepared.
// ===========================================================================

TEST_CASE("V2: PIVOT expands to a group and streams the pivoted rows", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE sales (city VARCHAR, year INT, amount INT)");
	V2ExecSQL(fx.conn,
	          "INSERT INTO sales VALUES ('ams', 2023, 10), ('ams', 2024, 20), ('rtm', 2023, 30), ('rtm', 2024, 40)");

	// Auto-PIVOT parses to one raw statement (a MultiStatement) and
	// expands at statement_execute into CREATE TYPE (enum) + SELECT.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "PIVOT sales ON year USING sum(amount)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Metadata is deferred until the row-producing fragment is prepared.
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	V2RequireSchemaDeferred(r);
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	// Only the pivoted SELECT's rows surface: one row per city.
	REQUIRE(V2DrainRowCount(r) == 2);

	// The principal fragment's metadata is now available.
	REQUIRE(V2ColumnCount(r) == 3); // city + one column per year
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: ALTER with a non-constant DEFAULT expands and executes fully", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(1000)");

	// Expands into BEGIN / ALTER ADD (DEFAULT NULL) / UPDATE / ALTER SET
	// DEFAULT / COMMIT, wrapped by the engine's preprocessor.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Metadata deferred while the group runs.
	V2RequireSchemaDeferred(r);

	// No fragment produces rows; the internal UPDATE's Count chunk is
	// discarded, exactly as ClientContext::Query discards it.
	REQUIRE(V2DrainRowCount(r) == 0);

	// Engine-mirrored principal: the last fragment (the injected COMMIT).
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_NOTHING);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_TRANSACTION);
	duckdb_v2_result_destroy(&r);

	// The whole group really ran: the column exists and is populated.
	REQUIRE(V2Query(fx.conn, "SELECT i FROM t WHERE c IS NOT NULL", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1000);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_drain runs an expanded group to completion", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(100)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT (1 + random())::INT", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Drain applies all fragments; the interior UPDATE's count is
	// suppressed (the principal COMMIT reports NOTHING).
	idx_t rows_changed = 99;
	REQUIRE(duckdb_v2_result_drain(r, &rows_changed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows_changed == 0);
	duckdb_v2_result_destroy(&r);

	REQUIRE(V2Query(fx.conn, "SELECT i FROM t WHERE c IS NOT NULL", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 100);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: query-backed PRAGMA reparses at statement_execute", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Reparses one-to-one into a row-producing statement, so metadata is
	// available immediately, before the first step.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "PRAGMA database_size", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(V2ColumnCount(r) > 0);

	REQUIRE(V2DrainRowCount(r) >= 1);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: an error inside an expanded group is sticky and rolls back", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(100)");

	// The materializing UPDATE fails at execution: '0.xxxx' || 'x' does
	// not convert to INT.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c INT DEFAULT ((random()::VARCHAR || 'x')::INT)", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_handle err = nullptr;
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(r, &chunk, &status, &err);
		REQUIRE(status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED); // must fail before finishing
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Sticky.
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	duckdb_v2_result_destroy(&r);

	// The wrapped transaction rolled back: no column, no partial data.
	REQUIRE(V2Query(fx.conn, "SELECT c FROM t", &r, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(r == nullptr);

	// The connection stays usable.
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: interrupt during an expanded group cancels and rolls back", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(2000000)");

	V2Result r;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Drive part of the group, then interrupt.
	for (int i = 0; i < 20; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000 && status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);
	duckdb_v2_result_destroy(&r);

	// Nothing of the group was committed.
	REQUIRE(V2Query(fx.conn, "SELECT c FROM t", &r, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);

	// The connection stays usable.
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: destroying a half-executed expanded group is clean", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(2000000)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	for (int i = 0; i < 10; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The abandoned group was not committed, and the connection works.
	REQUIRE(V2Query(fx.conn, "SELECT c FROM t", &r, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_get_schema: a result's output schema as one schema handle.
// ===========================================================================

TEST_CASE("V2: result_get_schema mirrors a SELECT's columns", "[capi_v2][query_result]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1 AS a, 'x' AS b", &r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name.ptr, name.len) == "a");
	REQUIRE(duckdb_v2_schema_get_field(schema, 1, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name.ptr, name.len) == "b");

	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_get_schema of a CHANGED_ROWS statement is a BIGINT count", "[capi_v2][query_result]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id;
	REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_get_schema null arguments", "[capi_v2][query_result]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = reinterpret_cast<duckdb_v2_schema_handle>(0x1);
	REQUIRE(duckdb_v2_result_get_schema(nullptr, &schema, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(schema == nullptr); // pointer out-param nulled on failure
	REQUIRE(duckdb_v2_result_get_schema(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_result_destroy(&r);
}

// ===========================================================================
// result_render_box: engine-side box rendering, consuming the result by
// transfer (the to_arrow_stream adopt-by-transfer pattern). These tests pin
// the raw transfer semantics; higher-level rendering assertions live in the
// [cpp_api] suite.
// ===========================================================================

TEST_CASE("V2: result_render_box renders, consumes the result, and frees the connection", "[capi_v2][query_result]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	char *text = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, &err) ==
	        DUCKDB_V2_ERROR_NONE);
	// Adopt-by-transfer: the slot is nulled on success.
	REQUIRE(r == nullptr);
	REQUIRE(text != nullptr);
	std::string rendered(text);
	free(text);
	// A box-drawing glyph and the column name are present.
	REQUIRE(rendered.find("\342\224\202") != std::string::npos);
	REQUIRE(rendered.find("one") != std::string::npos);

	// The one-live-result slot was released: a new query runs on the same connection.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 2 AS two", &r2) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r2) == 1);
	REQUIRE(duckdb_v2_result_destroy(&r2) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: result_render_box on an execution error consumes the result and reports the error",
          "[capi_v2][query_result]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	// Execution is lazy: error() (volatile, so never folded) throws only when the
	// stream is stepped, i.e. inside render_box's materialize loop.
	REQUIRE(V2Query(fx.conn, "SELECT error('boom render') FROM range(5) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	char *text = reinterpret_cast<char *>(0x1); // sentinel: the bridge must null it
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, &err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	// Consumed by transfer on failure too; out_text left NULL (no buffer to free).
	REQUIRE(r == nullptr);
	REQUIRE(text == nullptr);
	// The engine's error text propagated through the slot.
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(msg.ptr ? msg.ptr : "", msg.len).find("boom render") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);

	// The busy slot was released despite the error: a new query runs.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 7", &r2) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r2) == 1);
	REQUIRE(duckdb_v2_result_destroy(&r2) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: result_render_box rejects null arguments and leaves the result intact", "[capi_v2][query_result]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	// out_text NULL: rejected before adoption, so the result is NOT consumed.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, nullptr, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(r != nullptr); // intact
	duckdb_v2_error_info_destroy(&err);

	// A NULL result-slot pointer is rejected without a crash.
	char *text = nullptr;
	REQUIRE(duckdb_v2_result_render_box(nullptr, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(text == nullptr);

	// The still-intact result renders normally now.
	REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
	REQUIRE(text != nullptr);
	free(text);
}

TEST_CASE("V2: result_render_box null_value override and exact footer at the C boundary", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Custom null text: appears instead of "NULL".
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT CAST(NULL AS INTEGER) AS b", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, V2Str("<nil>"), 0, 0, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
		std::string rendered(text);
		free(text);
		REQUIRE(rendered.find("<nil>") != std::string::npos);
		REQUIRE(rendered.find("NULL") == std::string::npos);
	}

	// max_rows bounds display but the footer counts every materialized row.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 4, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		std::string rendered(text);
		free(text);
		REQUIRE(rendered.find("100 rows") != std::string::npos); // exact total
		REQUIRE(rendered.find("4 shown") != std::string::npos);  // display bounded to max_rows
	}
}

TEST_CASE("V2: result_render_box validates by-value arguments before consuming the result", "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// Malformed null_value (null pointer, nonzero length): rejected before
	// adopt-by-transfer, so the caller still owns the result.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT CAST(NULL AS INTEGER) AS b", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 5}, 0, 0, &text, &err) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(r != nullptr); // intact
		REQUIRE(text == nullptr);
		duckdb_v2_error_info_destroy(&err);
		REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	}

	// render_mode out of range: same rejection-before-adoption contract.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 2, 0, &text, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(r != nullptr); // intact
		REQUIRE(text == nullptr);
		REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	}

	// The valid empty null_value forms both render the default "NULL":
	// {NULL, 0} (canonical empty view) and {ptr, 0} (non-null pointer, zero length).
	for (duckdb_v2_str empty_null : {duckdb_v2_str {nullptr, 0}, duckdb_v2_str {"", 0}}) {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT CAST(NULL AS INTEGER) AS b", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, empty_null, 0, 0, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(r == nullptr);
		std::string rendered(text);
		free(text);
		REQUIRE(rendered.find("NULL") != std::string::npos);
	}
}

TEST_CASE("V2: result_render_box limit renders an approximate footer when the result fills the bound",
          "[capi_v2][query_result]") {
	V2EnvFixture fx;

	// The caller applied LIMIT 21 upstream and passes it as limit. The result
	// fills the bound exactly, so the true total is unknown: the footer renders
	// "? rows" instead of reporting the truncated count as exact.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT i FROM range(21) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 21, &text, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		std::string rendered(text);
		free(text);
		REQUIRE(rendered.find("? rows") != std::string::npos);  // count is unknown
		REQUIRE(rendered.find("21 rows") == std::string::npos); // never claims the bound as an exact total
	}

	// The same result rendered with limit 0: the count is known and exact, so no
	// "? rows" approximation appears.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(V2Query(fx.conn, "SELECT i FROM range(21) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		char *text = nullptr;
		REQUIRE(duckdb_v2_result_render_box(&r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, &text, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		std::string rendered(text);
		free(text);
		REQUIRE(rendered.find("? rows") == std::string::npos);
	}
}
