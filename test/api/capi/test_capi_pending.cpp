#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;

TEST_CASE("Test pending statements in C API", "[capi]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;
	duckdb::unique_ptr<CAPIResult> result;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(prepared.Prepare(tester, "SELECT SUM(i) FROM range(1000000) tbl(i)"));
	REQUIRE(pending.Pending(prepared));

	while (true) {
		auto state = pending.ExecuteTask();
		REQUIRE(state != DUCKDB_PENDING_ERROR);
		if (duckdb_pending_execution_is_finished(state)) {
			break;
		}
	}

	result = pending.Execute();
	REQUIRE(result);
	REQUIRE(!result->HasError());
	REQUIRE(result->Fetch<int64_t>(0, 0) == 499999500000LL);
}

// ---------------------------------------------------------------------------
// CANARY for duckdb/duckdb#23379.
//
// Bug: duckdb_pending_execute_task (ClientContext::ExecuteTaskInternal) does not
// observe a pending duckdb_interrupt when the calling thread runs no task that
// reaches InterruptCheck(). Reproduced deterministically below: a tiny streaming
// buffer parks the result collector at RESULT_READY, and from then on
// duckdb_pending_execute_task keeps returning RESULT_READY -- the interrupt is
// never surfaced as DUCKDB_PENDING_ERROR.
//
// This case is tagged [!shouldfail] on purpose: while the upstream bug is present
// the final REQUIRE fails, so Catch2 records the case as a (expected) PASS and the
// suite stays green. Once the fix lands via an upstream merge the case will PASS,
// which [!shouldfail] turns into a build FAILURE -- the canary's "peep".
//
// WHEN THIS CASE STARTS FAILING THE BUILD: the fix has landed. Delete this canary
// and remove the [!mayfail] tags from the V2 pending-phase interrupt cases in
// test/api/capi_v2/test_capi_v2_query_result.cpp (they flake for the same reason).
// ---------------------------------------------------------------------------
TEST_CASE("CANARY duckdb#23379: pending_execute_task must observe interrupt", "[capi][!shouldfail]") {
	CAPITester tester;
	CAPIPrepared prepared;
	CAPIPending pending;

	REQUIRE(tester.OpenDatabase(nullptr));
	// Deterministic repro: one executor thread (the caller is the only thread that
	// can run tasks) and a tiny streaming buffer so the collector parks after the
	// first chunk, reaching RESULT_READY long before the 10M-row scan finishes.
	REQUIRE(duckdb_query(tester.connection, "SET threads=1", nullptr) == DuckDBSuccess);
	REQUIRE(duckdb_query(tester.connection, "SET streaming_buffer_size='1KB'", nullptr) == DuckDBSuccess);

	REQUIRE(prepared.Prepare(tester, "SELECT * FROM range(10000000)"));
	REQUIRE(pending.PendingStreaming(prepared));

	// Drive until the buffer is full and the collector parks (RESULT_READY).
	duckdb_pending_state state = DUCKDB_PENDING_RESULT_NOT_READY;
	for (idx_t i = 0; i < 1000000; i++) {
		state = pending.ExecuteTask();
		if (state == DUCKDB_PENDING_RESULT_READY || state == DUCKDB_PENDING_ERROR) {
			break;
		}
	}
	REQUIRE(state == DUCKDB_PENDING_RESULT_READY);

	// Cancel the in-flight query; the next polls must surface it as ERROR.
	duckdb_interrupt(tester.connection);

	bool saw_error = false;
	for (idx_t j = 0; j < 1000; j++) {
		if (pending.ExecuteTask() == DUCKDB_PENDING_ERROR) {
			saw_error = true;
			break;
		}
	}
	REQUIRE(saw_error);
}
