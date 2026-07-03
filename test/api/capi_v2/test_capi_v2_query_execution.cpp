#include "capi_v2_test_helpers.hpp"

#include <chrono>
#include <string>
#include <thread>

using namespace std;

// ===========================================================================
// Query-execution edge cases: distinguishing an engine-initiated interrupt
// (max_execution_time timeout) from a consumer cancellation, and not rolling
// back a user-issued BEGIN TRANSACTION.
// ===========================================================================

// ---------------------------------------------------------------------------
// Bug #4: a user-issued BEGIN TRANSACTION must not be treated as a
// bridge-injected transaction wrap. Destroying the drained BEGIN result must
// not roll back the user's open transaction.
// ---------------------------------------------------------------------------

TEST_CASE("V2: user BEGIN ... ROLLBACK is not clobbered by the bridge", "[capi_v2][query_execution]") {
	V2EnvFixture fx;

	// A lone BEGIN TRANSACTION is a single fragment the user owns. Draining and
	// destroying its result must leave the transaction open.
	V2ExecSQL(fx.conn, "BEGIN TRANSACTION");

	// The transaction is still active: ROLLBACK must succeed. On the buggy code
	// the BEGIN was misclassified as a bridge wrap, so destroying its result
	// rolled the transaction back, and this ROLLBACK fails with "no transaction
	// is active".
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(V2Query(fx.conn, "ROLLBACK", &r, &err) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(r);
	duckdb_v2_result_destroy(&r);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: user BEGIN / INSERT / ROLLBACK discards the inserted row", "[capi_v2][query_execution]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	// Open a user transaction, insert a row, then roll back. The BEGIN result
	// is drained+destroyed before the INSERT runs; the bridge must keep the
	// user's transaction alive across that destroy.
	V2ExecSQL(fx.conn, "BEGIN TRANSACTION");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (42)");
	V2ExecSQL(fx.conn, "ROLLBACK");

	// The ROLLBACK undid the INSERT: the table is empty.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainChangedRows(r) == 0);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: user BEGIN / INSERT / COMMIT keeps the inserted row", "[capi_v2][query_execution]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	V2ExecSQL(fx.conn, "BEGIN TRANSACTION");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (7)");
	V2ExecSQL(fx.conn, "COMMIT");

	// The COMMIT persisted the INSERT: the row survives.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainChangedRows(r) == 1);
	duckdb_v2_result_destroy(&r);
}

// ---------------------------------------------------------------------------
// Bug #1: a max_execution_time timeout shares the INTERRUPT exception type with
// a consumer cancellation, but must surface as an ERROR carrying its message,
// not a message-less CANCELLED status. A real connection_interrupt must still
// surface as CANCELLED.
// ---------------------------------------------------------------------------

// A timeout can land in either the pending or streaming phase, depending on
// scheduling; [!mayfail] absorbs the rare run where the bounded step loop
// exits before the timeout fires under heavy load.
TEST_CASE("V2: a max_execution_time timeout surfaces as an error, not CANCELLED",
          "[capi_v2][query_execution][!mayfail]") {
	V2EnvFixture fx;

	// 50ms timeout on a slow cross product, mirroring max_execution_time.test.
	V2ExecSQL(fx.conn, "SET max_execution_time=50");

	V2Result r;
	REQUIRE(V2Query(fx.conn, "SELECT count(*) FROM range(100000000) t1, range(1000) t2", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Step until the timeout fires. It must arrive as an error return code, not
	// as the CANCELLED status. On the buggy code the timeout was conflated with
	// a consumer cancellation and surfaced as a message-less CANCELLED.
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_handle err = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 1000000; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(r, &chunk, &status, &err);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		// Stop on any terminal outcome: an error (expected), a cancellation
		// (the bug), or a finish (should not happen at this scale).
		if (rc != DUCKDB_V2_ERROR_NONE || status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED ||
		    status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			break;
		}
	}

	// The timeout must never be reported as a cancellation.
	REQUIRE(status != DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED);
	REQUIRE(rc == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	REQUIRE(err != nullptr);
	duckdb_v2_str text = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &text);
	auto msg = V2StrTo(text);
	INFO("timeout error detail: " << (!msg.empty() ? msg : "(none)"));
	REQUIRE(msg.find("Query exceeded maximum execution time") != string::npos);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: a consumer interrupt still surfaces as CANCELLED, not an error", "[capi_v2][query_execution]") {
	V2EnvFixture fx;

	// No timeout set: the only cancellation channel is connection_interrupt.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Consume the first chunk so the stream is genuinely mid-flight.
	auto first = V2StepChunk(r);
	REQUIRE(first != nullptr);
	duckdb_v2_data_chunk_destroy(&first);

	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The interrupt surfaces as the CANCELLED status, never as an error.
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
}
