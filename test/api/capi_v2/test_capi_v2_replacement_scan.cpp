#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 replacement scan tests.
//
// Replacement scans fire when a base-table lookup fails; the callback claims
// the unresolved name by naming a table function, declines by returning
// without claiming, or fails by setting a code on the err slot. Results are
// consumed through the streaming helpers; every result is drained or
// destroyed before the next query on the same connection (one live result
// per connection).
//
// All file-level symbols carry a ReplScan prefix: the test suite is built as
// a unity build, so file-static names must be unique across test files.
// ---------------------------------------------------------------------------

namespace {

struct ReplScanQueryFailure {
	DUCKDB_V2_ERROR code = DUCKDB_V2_ERROR_NONE;
	std::string message;
};

// Runs a query that is expected to fail at statement_execute time (binder
// errors surface there: prepare is eager, only execution is lazy). Returns
// the failure code + message.
ReplScanQueryFailure ReplScanExpectQueryFailure(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = V2Query(conn, sql, &result, &err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(result == nullptr);
	ReplScanQueryFailure failure;
	failure.code = rc;
	duckdb_v2_str text {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &text);
	failure.message = V2StrTo(text);
	duckdb_v2_error_info_destroy(&err);
	return failure;
}

// Runs a query expected to succeed and returns its total row count.
idx_t ReplScanQueryRowCount(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, sql, &result, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto rows = V2DrainRowCount(result);
	duckdb_v2_result_destroy(&result);
	return rows;
}

} // namespace

// ---------------------------------------------------------------------------
// Happy path: claim a builtin table function. Also pins value ownership: the
// duckdb_v2_value is destroyed inside the callback, immediately after
// add_parameter, and the copy must keep working through planning/execution.
// ---------------------------------------------------------------------------

static void ReplScanClaimRange(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                               duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("range"), err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle count = V2Int64Value(5);
	REQUIRE(duckdb_v2_replacement_scan_add_parameter(info, count, err) == DUCKDB_V2_ERROR_NONE);
	// borrowed and copied at the call: destroying the value immediately must
	// not affect the query
	duckdb_v2_value_destroy(&count);
}

TEST_CASE("V2 replacement scan: claim a builtin table function", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanClaimRange, {nullptr, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ReplScanQueryRowCount(fix.conn, "SELECT * FROM repl_scan_no_such_table") == 5);
}

// ---------------------------------------------------------------------------
// Named parameters: a V2 table function with a positional and a named
// parameter, targeted through add_parameter + add_named_parameter.
// repl_seq_fn(n, start := s) produces n BIGINT rows s, s+1, ..., s+n-1.
// ---------------------------------------------------------------------------

struct ReplScanSeqBindData {
	int64_t n = 0;
	int64_t start = 0;
};

struct ReplScanSeqState {
	int64_t next = 0;
	int64_t end = 0;
};

static void ReplScanSeqBind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                            duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle n_val = nullptr;
	REQUIRE(duckdb_v2_table_function_bind_get_parameter(info, 0, &n_val, err) == DUCKDB_V2_ERROR_NONE);
	int64_t n = V2LeafPayload<int64_t>(n_val);
	duckdb_v2_value_destroy(&n_val);

	int64_t start = 0;
	duckdb_v2_value_handle start_val = nullptr;
	duckdb_v2_error_info_handle local_err = nullptr;
	if (duckdb_v2_table_function_bind_get_named_parameter(info, V2Str("start"), &start_val, &local_err) ==
	    DUCKDB_V2_ERROR_NONE) {
		start = V2LeafPayload<int64_t>(start_val);
		duckdb_v2_value_destroy(&start_val);
	}
	duckdb_v2_error_info_destroy(&local_err);

	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("val"), bigint_type, err);
	duckdb_v2_logical_type_destroy(&bigint_type);

	auto *bd = new ReplScanSeqBindData {n, start};
	duckdb_v2_table_function_bind_set_bind_data(
	    info, {bd, [](void *p) { delete static_cast<ReplScanSeqBindData *>(p); }, nullptr}, err);
}

static void ReplScanSeqInitGlobal(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                                  duckdb_v2_error_info_handle *err) {
	void *raw_bd = nullptr;
	duckdb_v2_table_function_init_get_bind_data(info, &raw_bd, err);
	auto *bd = static_cast<ReplScanSeqBindData *>(raw_bd);

	auto *state = new ReplScanSeqState {bd->start, bd->start + bd->n};
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<ReplScanSeqState *>(p); }, nullptr}, err);
}

static void ReplScanSeqExec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                            duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);
	auto &state = *static_cast<ReplScanSeqState *>(global_state);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);

	idx_t count = 0;
	if (state.next < state.end) {
		int64_t *data = nullptr;
		duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
		while (state.next < state.end && count < 2048) {
			data[count] = state.next;
			state.next++;
			count++;
		}
	}
	// the table-function bridge derives the output cardinality from vector 0
	duckdb_v2_vector_set_size(vec, count, err);
}

static void ReplScanRegisterSeqFn(duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *err) {
	duckdb_v2_table_function_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("repl_seq_fn"), err) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	// "n" is a required positional parameter; "start" is an optional named
	// parameter defaulting to 0 (the bridge injects it when the call omits it).
	duckdb_v2_value_handle start_default = V2Int64Value(0);
	V2TableSignature(builder, [&](duckdb_v2_function_signature_handle sig) {
		V2SigParam(sig, "n", bigint_type);
		V2SigParamDefault(sig, "start", bigint_type, start_default);
	});
	duckdb_v2_value_destroy(&start_default);
	duckdb_v2_logical_type_destroy(&bigint_type);

	REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, ReplScanSeqBind, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, ReplScanSeqInitGlobal, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, ReplScanSeqExec, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_builder_destroy(&builder);
}

static void ReplScanClaimSeq(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                             duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("repl_seq_fn"), err) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_handle n = V2Int64Value(4);
	REQUIRE(duckdb_v2_replacement_scan_add_parameter(info, n, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&n);

	duckdb_v2_value_handle start = V2Int64Value(10);
	REQUIRE(duckdb_v2_replacement_scan_add_named_parameter(info, V2Str("start"), start, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&start);
}

TEST_CASE("V2 replacement scan: named parameter targets a V2 table function", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) { ReplScanRegisterSeqFn(ctx, err); },
	    nullptr, nullptr);

	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanClaimSeq, {nullptr, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT * FROM repl_scan_no_such_table", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 4);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	for (idx_t i = 0; i < 4; i++) {
		idx_t idx = view.sel ? view.sel[i] : i;
		REQUIRE(data[idx] == static_cast<int64_t>(10 + i));
	}

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// Name getters: what the callback observes for unqualified and qualified
// references, including the empty-view mapping for absent parts. The callback
// declines, so each probe query ends in the normal catalog error; that also
// pins the decline contract.
// ---------------------------------------------------------------------------

struct ReplScanNameProbe {
	int invocations = 0;
	bool catalog_is_null = false;
	idx_t catalog_length = 1234;
	std::string catalog_name;
	bool schema_is_null = false;
	idx_t schema_length = 1234;
	std::string schema_name;
	std::string table_name;
	idx_t table_length = 0;
};

static void ReplScanRecordNames(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_replacement_scan_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	auto &probe = *static_cast<ReplScanNameProbe *>(ud);
	probe.invocations++;

	duckdb_v2_str name {nullptr, 0};
	REQUIRE(duckdb_v2_replacement_scan_get_catalog_name(info, &name, err) == DUCKDB_V2_ERROR_NONE);
	probe.catalog_is_null = (name.ptr == nullptr);
	probe.catalog_length = name.len;
	probe.catalog_name = V2StrTo(name);

	name = duckdb_v2_str {nullptr, 0};
	REQUIRE(duckdb_v2_replacement_scan_get_schema_name(info, &name, err) == DUCKDB_V2_ERROR_NONE);
	probe.schema_is_null = (name.ptr == nullptr);
	probe.schema_length = name.len;
	probe.schema_name = V2StrTo(name);

	name = duckdb_v2_str {nullptr, 0};
	REQUIRE(duckdb_v2_replacement_scan_get_table_name(info, &name, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name.ptr != nullptr);
	probe.table_name = V2StrTo(name);
	probe.table_length = name.len;
	// decline: return without claiming
}

TEST_CASE("V2 replacement scan: name getters and decline", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	ReplScanNameProbe probe;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanRecordNames, {&probe, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("unqualified name: catalog and schema read as empty views, decline reaches catalog error") {
		auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
		REQUIRE(failure.code == DUCKDB_V2_ERROR_DATABASE_CATALOG);
		REQUIRE(probe.invocations == 1);
		REQUIRE(probe.catalog_is_null);
		REQUIRE(probe.catalog_length == 0);
		REQUIRE(probe.schema_is_null);
		REQUIRE(probe.schema_length == 0);
		REQUIRE(probe.table_name == "repl_scan_nosuch");
		REQUIRE(probe.table_length == probe.table_name.size());
	}

	SECTION("qualified name under an existing catalog: parts flow through") {
		// "memory" is the in-memory database's catalog; the schema does not
		// exist, so the lookup fails and the replacement scan fires with the
		// full qualification visible.
		auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM memory.nosuch_schema.nosuch_table");
		REQUIRE(failure.code == DUCKDB_V2_ERROR_DATABASE_CATALOG);
		REQUIRE(probe.invocations == 1);
		REQUIRE_FALSE(probe.catalog_is_null);
		REQUIRE(probe.catalog_name == "memory");
		REQUIRE(probe.catalog_length == probe.catalog_name.size());
		REQUIRE_FALSE(probe.schema_is_null);
		REQUIRE(probe.schema_name == "nosuch_schema");
		REQUIRE(probe.schema_length == probe.schema_name.size());
		REQUIRE(probe.table_name == "nosuch_table");
	}

	SECTION("qualified name under an unknown catalog: replacement still fires") {
		// The failed lookup consults the replacement scans even when the
		// catalog itself does not exist; after the decline, the binder's own
		// catalog-does-not-exist error surfaces (as a binder error, not a
		// catalog error). Pinned as documentation of engine behavior, not as
		// a V2 contract.
		auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM nosuch_catalog.nosuch_schema.nosuch_table");
		REQUIRE(failure.code == DUCKDB_V2_ERROR_QUERY_BINDER);
		REQUIRE(probe.invocations == 1);
		REQUIRE(probe.catalog_name == "nosuch_catalog");
		REQUIRE(probe.schema_name == "nosuch_schema");
		REQUIRE(probe.table_name == "nosuch_table");
	}
}

// ---------------------------------------------------------------------------
// Registration order: scans are consulted in registration order, the first
// claim wins, later scans are not invoked once one claims.
// ---------------------------------------------------------------------------

struct ReplScanOrderFlags {
	bool first_claims = false;
	int first_invocations = 0;
	int second_invocations = 0;
};

static void ReplScanOrderFirst(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                               duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_replacement_scan_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	auto &flags = *static_cast<ReplScanOrderFlags *>(ud);
	flags.first_invocations++;
	if (!flags.first_claims) {
		return;
	}
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("range"), err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle count = V2Int64Value(7);
	REQUIRE(duckdb_v2_replacement_scan_add_parameter(info, count, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&count);
}

static void ReplScanOrderSecond(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_replacement_scan_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	auto &flags = *static_cast<ReplScanOrderFlags *>(ud);
	flags.second_invocations++;
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("range"), err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle count = V2Int64Value(3);
	REQUIRE(duckdb_v2_replacement_scan_add_parameter(info, count, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&count);
}

TEST_CASE("V2 replacement scan: registration order, first claim wins", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	ReplScanOrderFlags flags;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanOrderFirst, {&flags, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanOrderSecond, {&flags, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("first declines, second claims") {
		flags.first_claims = false;
		REQUIRE(ReplScanQueryRowCount(fix.conn, "SELECT * FROM repl_scan_nosuch") == 3);
		REQUIRE(flags.first_invocations == 1);
		REQUIRE(flags.second_invocations == 1);
	}

	SECTION("first claims, second never invoked") {
		flags.first_claims = true;
		REQUIRE(ReplScanQueryRowCount(fix.conn, "SELECT * FROM repl_scan_nosuch") == 7);
		REQUIRE(flags.first_invocations == 1);
		REQUIRE(flags.second_invocations == 0);
	}
}

// ---------------------------------------------------------------------------
// Error paths: a mapped code round-trips exactly; the generic sentinel falls
// back to the binder error (the InvokeWithErrorSlot fallback, since the
// callback runs during binding).
// ---------------------------------------------------------------------------

static void ReplScanFailMapped(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                               duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_IO_GENERAL);
	duckdb_v2_error_info_set_text(*err, V2Str("replacement scan probe failure"));
}

static void ReplScanFailSentinel(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_API);
	duckdb_v2_error_info_set_text(*err, V2Str("unmapped replacement scan failure"));
}

TEST_CASE("V2 replacement scan: callback error codes round-trip", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;

	SECTION("mapped code surfaces exactly") {
		REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanFailMapped, {nullptr, nullptr, nullptr}, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
		REQUIRE(failure.code == DUCKDB_V2_ERROR_IO_GENERAL);
		REQUIRE(failure.message.find("replacement scan probe failure") != std::string::npos);
	}

	SECTION("generic sentinel falls back to the binder error") {
		REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanFailSentinel, {nullptr, nullptr, nullptr},
		                                            nullptr) == DUCKDB_V2_ERROR_NONE);
		auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
		REQUIRE(failure.code == DUCKDB_V2_ERROR_QUERY_BINDER);
		REQUIRE(failure.message.find("unmapped replacement scan failure") != std::string::npos);
	}
}

// ---------------------------------------------------------------------------
// Claiming an unknown function: set_function_name does not validate; the
// binder's own catalog error surfaces later, no crash.
// ---------------------------------------------------------------------------

static void ReplScanClaimUnknownFn(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                   duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("repl_scan_definitely_missing_fn"), err) ==
	        DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 replacement scan: unknown claimed function surfaces a catalog error", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanClaimUnknownFn, {nullptr, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto failure = ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
	REQUIRE(failure.code == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(failure.message.find("repl_scan_definitely_missing_fn") != std::string::npos);
}

// ---------------------------------------------------------------------------
// set_function_name called twice: the last call wins.
// ---------------------------------------------------------------------------

static void ReplScanClaimTwice(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                               duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("repl_scan_definitely_missing_fn"), err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_replacement_scan_set_function_name(info, V2Str("range"), err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle count = V2Int64Value(2);
	REQUIRE(duckdb_v2_replacement_scan_add_parameter(info, count, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&count);
}

TEST_CASE("V2 replacement scan: set_function_name twice, last wins", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanClaimTwice, {nullptr, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ReplScanQueryRowCount(fix.conn, "SELECT * FROM repl_scan_nosuch") == 2);
}

// ---------------------------------------------------------------------------
// User data: flows to the callback via get_user_data; the opaque's destroy
// callback runs exactly once, at database close (not before).
// ---------------------------------------------------------------------------

struct ReplScanUserDataProbe {
	int seen_in_callback = 0;
	int destroy_calls = 0;
};

static void ReplScanCountInvocation(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                    duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_replacement_scan_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ud != nullptr);
	static_cast<ReplScanUserDataProbe *>(ud)->seen_in_callback++;
	// decline
}

TEST_CASE("V2 replacement scan: user data flows, destructor runs once at close", "[capi_v2][replacement_scan]") {
	ReplScanUserDataProbe probe;
	{
		V2EnvFixture fix;
		REQUIRE(duckdb_v2_replacement_scan_register(
		            fix.db, ReplScanCountInvocation,
		            {&probe, [](void *p) { static_cast<ReplScanUserDataProbe *>(p)->destroy_calls++; }, nullptr},
		            nullptr) == DUCKDB_V2_ERROR_NONE);
		ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
		REQUIRE(probe.seen_in_callback == 1);
		// scans live until the database closes; the destructor has not run yet
		REQUIRE(probe.destroy_calls == 0);
	}
	REQUIRE(probe.destroy_calls == 1);
}

// ---------------------------------------------------------------------------
// No firing for names that resolve normally.
// ---------------------------------------------------------------------------

TEST_CASE("V2 replacement scan: not invoked for resolvable names", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;
	ReplScanUserDataProbe probe;
	REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanCountInvocation, {&probe, nullptr, nullptr}, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	V2ExecSQL(fix.conn, "CREATE TABLE repl_scan_real(i INTEGER)");
	V2ExecSQL(fix.conn, "INSERT INTO repl_scan_real VALUES (1), (2)");
	REQUIRE(ReplScanQueryRowCount(fix.conn, "SELECT * FROM repl_scan_real") == 2);
	REQUIRE(probe.seen_in_callback == 0);
}

// ---------------------------------------------------------------------------
// Null-argument rejection. Out-of-callback: null info / null db / null
// callback. In-callback: null out-params and malformed inputs on a valid info.
// A {NULL, 0} string view is a valid empty string; only {NULL, nonzero-len}
// is malformed. Pointer-typed out-params are nulled on INVALID_INPUT.
// ---------------------------------------------------------------------------

struct ReplScanNullArgProbe {
	DUCKDB_V2_ERROR null_out_name = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR null_out_data = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR bad_fn_name = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR null_param_value = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR bad_named_name = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR null_named_value = DUCKDB_V2_ERROR_NONE;
};

static void ReplScanProbeNullArgs(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_context_handle ctx,
                                  duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_replacement_scan_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	auto &probe = *static_cast<ReplScanNullArgProbe *>(ud);

	probe.null_out_name = duckdb_v2_replacement_scan_get_table_name(info, nullptr, nullptr);
	probe.null_out_data = duckdb_v2_replacement_scan_get_user_data(info, nullptr, nullptr);
	// a {NULL, nonzero-len} view is malformed and rejected
	probe.bad_fn_name = duckdb_v2_replacement_scan_set_function_name(info, duckdb_v2_str {nullptr, 5}, nullptr);
	probe.null_param_value = duckdb_v2_replacement_scan_add_parameter(info, nullptr, nullptr);

	duckdb_v2_value_handle value = V2Int64Value(1);
	probe.bad_named_name =
	    duckdb_v2_replacement_scan_add_named_parameter(info, duckdb_v2_str {nullptr, 5}, value, nullptr);
	duckdb_v2_value_destroy(&value);
	probe.null_named_value = duckdb_v2_replacement_scan_add_named_parameter(info, V2Str("start"), nullptr, nullptr);
	// decline
}

TEST_CASE("V2 replacement scan: null argument rejection", "[capi_v2][replacement_scan]") {
	V2EnvFixture fix;

	SECTION("register rejects null db and null callback") {
		REQUIRE(duckdb_v2_replacement_scan_register(nullptr, ReplScanClaimRange, {nullptr, nullptr, nullptr},
		                                            nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_replacement_scan_register(fix.db, nullptr, {nullptr, nullptr, nullptr}, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}

	SECTION("info accessors reject a null info and null the pointer out-params") {
		duckdb_v2_str name {reinterpret_cast<const char *>(0x1), 7};
		REQUIRE(duckdb_v2_replacement_scan_get_catalog_name(nullptr, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(name.ptr == nullptr);

		name = duckdb_v2_str {reinterpret_cast<const char *>(0x1), 7};
		REQUIRE(duckdb_v2_replacement_scan_get_schema_name(nullptr, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(name.ptr == nullptr);

		name = duckdb_v2_str {reinterpret_cast<const char *>(0x1), 7};
		REQUIRE(duckdb_v2_replacement_scan_get_table_name(nullptr, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(name.ptr == nullptr);

		void *data = reinterpret_cast<void *>(0x1);
		REQUIRE(duckdb_v2_replacement_scan_get_user_data(nullptr, &data, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(data == nullptr);

		REQUIRE(duckdb_v2_replacement_scan_set_function_name(nullptr, V2Str("range"), nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);

		duckdb_v2_value_handle value = V2Int64Value(1);
		REQUIRE(duckdb_v2_replacement_scan_add_parameter(nullptr, value, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_replacement_scan_add_named_parameter(nullptr, V2Str("start"), value, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_value_destroy(&value);
	}

	SECTION("in-callback null out-params and malformed inputs are rejected") {
		ReplScanNullArgProbe probe;
		REQUIRE(duckdb_v2_replacement_scan_register(fix.db, ReplScanProbeNullArgs, {&probe, nullptr, nullptr},
		                                            nullptr) == DUCKDB_V2_ERROR_NONE);
		ReplScanExpectQueryFailure(fix.conn, "SELECT * FROM repl_scan_nosuch");
		REQUIRE(probe.null_out_name == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(probe.null_out_data == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(probe.bad_fn_name == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(probe.null_param_value == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(probe.bad_named_name == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(probe.null_named_value == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
}
