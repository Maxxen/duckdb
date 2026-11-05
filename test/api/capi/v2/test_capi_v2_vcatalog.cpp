#include "test_capi_v2.hpp"

#include <string>

// ---------------------------------------------------------------------------
// V2 vcatalog tests: register a database type of our own, ATTACH databases of
// that type, and observe the attach and transaction callbacks.
//
// Callbacks avoid Catch assertions: they run inside DuckDB. They record what
// they see, and the test asserts on it afterwards.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

constexpr std::string_view TYPE_NAME = "vtest";

// What the callbacks saw, and how they should misbehave. Carried in the vcatalog's user data; declared before the
// fixture, so it outlives the instance and the states destroyed with it.
struct Recorder {
	idx_t attaches = 0;
	std::string name;
	std::string path;
	bool attach_read_only = false;
	std::string secret;
	idx_t begins = 0;
	bool begin_read_only = false;
	idx_t commits = 0;
	idx_t rollbacks = 0;
	//! Whether every callback saw the user data and the states set for its database and transaction
	bool states_seen = true;
	idx_t catalog_states_destroyed = 0;
	idx_t transaction_states_destroyed = 0;

	bool fail_attach = false;
	bool fail_begin = false;
	bool fail_commit = false;
	bool fail_rollback = false;
};

struct CatalogState {
	Recorder *recorder;
};

void DestroyCatalogState(void *ptr) {
	auto state = static_cast<CatalogState *>(ptr);
	state->recorder->catalog_states_destroyed++;
	delete state;
}

struct TransactionState {
	Recorder *recorder;
	CatalogState *catalog;
};

void DestroyTransactionState(void *ptr) {
	auto state = static_cast<TransactionState *>(ptr);
	state->recorder->transaction_states_destroyed++;
	delete state;
}

void Fail(duckdb_v2_error_info_handle *err, std::string_view text) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_set_text(*err, text.data(), text.size());
}

void Attach(duckdb_v2_vcatalog_attach_info_handle info, duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	if (duckdb_v2_vcatalog_attach_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.attaches++;
	const char *text = nullptr;
	idx_t text_length = 0;
	if (duckdb_v2_vcatalog_attach_get_name(info, &text, &text_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.name.assign(text, text_length);
	if (duckdb_v2_vcatalog_attach_get_path(info, &text, &text_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.path.assign(text, text_length);
	if (duckdb_v2_vcatalog_attach_is_read_only(info, &recorder.attach_read_only, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	std::string_view option_name = "SECRET";
	duckdb_v2_value_handle secret = nullptr;
	bool present = false;
	if (duckdb_v2_vcatalog_attach_get_option(info, option_name.data(), option_name.size(), &secret, &present, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder.secret.clear();
	if (present) {
		auto rc = duckdb_v2_value_get_varchar(secret, &text, &text_length, err);
		if (rc == DUCKDB_V2_ERROR_NONE) {
			recorder.secret.assign(text, text_length);
		}
		duckdb_v2_value_destroy(&secret);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// Set before failing, so a failed ATTACH still has a state to destroy
	duckdb_v2_opaque state = {new CatalogState {&recorder}, DestroyCatalogState, nullptr};
	if (duckdb_v2_vcatalog_attach_set_catalog_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyCatalogState(state.ptr);
		return;
	}
	if (recorder.fail_attach) {
		Fail(err, "attach refused");
	}
}

void Begin(duckdb_v2_vcatalog_transaction_begin_info_handle info, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_transaction_begin_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_begin_get_catalog_state(info, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.begins++;
	auto catalog = static_cast<CatalogState *>(catalog_state);
	if (!catalog || catalog->recorder != &recorder) {
		recorder.states_seen = false;
	}
	if (duckdb_v2_vcatalog_transaction_begin_is_read_only(info, &recorder.begin_read_only, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// Set before failing, so a failed begin still has a state to destroy
	duckdb_v2_opaque state = {new TransactionState {&recorder, catalog}, DestroyTransactionState, nullptr};
	if (duckdb_v2_vcatalog_transaction_begin_set_transaction_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyTransactionState(state.ptr);
		return;
	}
	if (recorder.fail_begin) {
		Fail(err, "begin refused");
	}
}

// Checks that a commit or rollback sees what its database and transaction set.
void CheckStates(Recorder &recorder, void *catalog_state, void *transaction_state) {
	auto transaction = static_cast<TransactionState *>(transaction_state);
	if (!catalog_state || !transaction || transaction->catalog != catalog_state || transaction->recorder != &recorder) {
		recorder.states_seen = false;
	}
}

void Commit(duckdb_v2_vcatalog_transaction_commit_info_handle info, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_transaction_commit_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_commit_get_catalog_state(info, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_commit_get_transaction_state(info, &transaction_state, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.commits++;
	CheckStates(recorder, catalog_state, transaction_state);
	if (recorder.fail_commit) {
		Fail(err, "commit refused");
	}
}

void Rollback(duckdb_v2_vcatalog_transaction_rollback_info_handle info, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_transaction_rollback_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_rollback_get_catalog_state(info, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_transaction_rollback_get_transaction_state(info, &transaction_state, err) !=
	        DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.rollbacks++;
	CheckStates(recorder, catalog_state, transaction_state);
	if (recorder.fail_rollback) {
		Fail(err, "rollback refused");
	}
}

// A vcatalog with every callback set, recording into `recorder`.
duckdb_v2_vcatalog_handle MakeVCatalog(duckdb_v2_factory_handle factory, std::string_view name, Recorder *recorder) {
	duckdb_v2_vcatalog_handle vcatalog = nullptr;
	REQUIRE(duckdb_v2_vcatalog_create(factory, &vcatalog, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (!name.empty()) {
		REQUIRE(duckdb_v2_vcatalog_set_name(vcatalog, name.data(), name.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	REQUIRE(duckdb_v2_vcatalog_set_attach_callback(vcatalog, Attach, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_begin_callback(vcatalog, Begin, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_commit_callback(vcatalog, Commit, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_transaction_rollback_callback(vcatalog, Rollback, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (recorder) {
		duckdb_v2_opaque data = {recorder, nullptr, nullptr};
		REQUIRE(duckdb_v2_vcatalog_set_user_data(vcatalog, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	return vcatalog;
}

DUCKDB_V2_ERROR RegisterVCatalog(duckdb_v2_connection_handle conn, duckdb_v2_vcatalog_handle vcatalog) {
	auto rc = duckdb_v2_connection_register_vcatalog(conn, vcatalog, nullptr);
	duckdb_v2_vcatalog_destroy(&vcatalog);
	return rc;
}

// Runs a statement expected to fail, returning the text of its error, or an empty string if it succeeded.
std::string ErrorText(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_error_info_handle err = nullptr;
	duckdb_v2_result_handle result = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	auto status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	while (rc == DUCKDB_V2_ERROR_NONE && status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_step(result, &chunk, &status, &err);
		duckdb_v2_data_chunk_destroy(&chunk);
		if (rc == DUCKDB_V2_ERROR_NONE && status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			rc = duckdb_v2_result_wait(result, &err);
		}
	}
	duckdb_v2_result_destroy(&result);
	std::string message;
	if (rc != DUCKDB_V2_ERROR_NONE && err) {
		const char *text = nullptr;
		idx_t text_length = 0;
		if (duckdb_v2_error_info_get_text(err, &text, &text_length) == DUCKDB_V2_ERROR_NONE) {
			message.assign(text, text_length);
		}
	}
	duckdb_v2_error_info_destroy(&err);
	return message;
}

bool Contains(const std::string &haystack, std::string_view needle) {
	return haystack.find(needle) != std::string::npos;
}

// A fixture with the vcatalog registered under TYPE_NAME, and every transaction touching every attached database.
struct VCatalogFixture {
	Recorder recorder;
	EnvFixture fx;

	VCatalogFixture() {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "SET immediate_transaction_mode = true") == DUCKDB_V2_ERROR_NONE);
	}

	void ResetCounts() {
		recorder.begins = 0;
		recorder.commits = 0;
		recorder.rollbacks = 0;
		recorder.transaction_states_destroyed = 0;
	}
};

} // namespace

TEST_CASE("V2 vcatalog: ATTACH invokes the attach callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;

	REQUIRE(RunSql(conn, "ATTACH 'some/path' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.attaches == 1);
	REQUIRE(recorder.name == "v");
	REQUIRE(recorder.path == "some/path");
	REQUIRE_FALSE(recorder.attach_read_only);
	REQUIRE(recorder.secret.empty());

	SECTION("the type can be given as a path prefix") {
		REQUIRE(RunSql(conn, "ATTACH 'vtest:other/path' AS w") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.attaches == 2);
		REQUIRE(recorder.name == "w");
		REQUIRE(recorder.path == "other/path");
	}
	SECTION("read-only attaches are reported") {
		REQUIRE(RunSql(conn, "ATTACH 'p' AS r (TYPE vtest, READ_ONLY)") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.attach_read_only);
	}
	SECTION("options are retrieved case-insensitively") {
		REQUIRE(RunSql(conn, "ATTACH 'p' AS o (TYPE vtest, secret 'hunter2')") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.secret == "hunter2");
	}
	SECTION("DETACH destroys the catalog state") {
		REQUIRE(recorder.catalog_states_destroyed == 0);
		REQUIRE(RunSql(conn, "DETACH v") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.catalog_states_destroyed == 1);
	}
}

TEST_CASE("V2 vcatalog: a failing ATTACH destroys the catalog state", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;

	SECTION("the attach callback reports an error") {
		recorder.fail_attach = true;
		REQUIRE(Contains(ErrorText(conn, "ATTACH 'p' AS v (TYPE vtest)"), "attach refused"));
	}
	SECTION("an option the attach callback did not retrieve") {
		REQUIRE(Contains(ErrorText(conn, "ATTACH 'p' AS v (TYPE vtest, bogus 1)"), "Unrecognized option"));
	}
	REQUIRE(recorder.attaches == 1);
	REQUIRE(recorder.catalog_states_destroyed == 1);
	// The name is free again
	recorder.fail_attach = false;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: transactions begin, commit and roll back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	SECTION("COMMIT") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE_FALSE(recorder.begin_read_only);
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.commits == 1);
		REQUIRE(recorder.rollbacks == 0);
	}
	SECTION("ROLLBACK") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE(recorder.commits == 0);
		REQUIRE(recorder.rollbacks == 1);
	}
	SECTION("BEGIN TRANSACTION READ ONLY") {
		REQUIRE(RunSql(conn, "BEGIN TRANSACTION READ ONLY") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begin_read_only);
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("a lookup in auto-commit mode begins a transaction") {
		REQUIRE(RunSql(conn, "SET immediate_transaction_mode = false") == DUCKDB_V2_ERROR_NONE);
		vfx.ResetCounts();
		// No schema exists yet, so the statement fails and its transaction rolls back
		REQUIRE(RunSql(conn, "SELECT * FROM v.main.t") != DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.begins == 1);
		REQUIRE(recorder.rollbacks == 1);
	}
	SECTION("a statement failing in an explicit transaction rolls back only when the transaction ends") {
		REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(conn, "SELECT error('boom')") != DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.rollbacks == 0);
		// COMMIT of the invalidated transaction silently rolls it back
		REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.commits == 0);
		REQUIRE(recorder.rollbacks == 1);
	}
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.transaction_states_destroyed == recorder.begins);
}

TEST_CASE("V2 vcatalog: transactions on read-only databases are read-only", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest, READ_ONLY)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.begin_read_only);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 vcatalog: a failed commit rolls the transaction back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_commit = true;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Contains(ErrorText(conn, "COMMIT"), "commit refused"));
	REQUIRE(recorder.commits == 1);
	REQUIRE(recorder.rollbacks == 1);
	REQUIRE(recorder.transaction_states_destroyed == 1);
	REQUIRE(recorder.states_seen);

	// The transaction has ended, and the connection is usable
	recorder.fail_commit = false;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.commits == 2);
}

TEST_CASE("V2 vcatalog: a failed rollback still ends the transaction", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_rollback = true;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Contains(ErrorText(conn, "ROLLBACK"), "rollback refused"));
	REQUIRE(recorder.rollbacks == 1);
	REQUIRE(recorder.transaction_states_destroyed == 1);

	recorder.fail_rollback = false;
	REQUIRE(RunSql(conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.commits == 1);
}

TEST_CASE("V2 vcatalog: a failed begin neither commits nor rolls back", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "SET immediate_transaction_mode = false") == DUCKDB_V2_ERROR_NONE);
	vfx.ResetCounts();

	recorder.fail_begin = true;
	REQUIRE(Contains(ErrorText(conn, "SELECT * FROM v.main.t"), "begin refused"));
	REQUIRE(recorder.begins == 1);
	REQUIRE(recorder.commits == 0);
	REQUIRE(recorder.rollbacks == 0);
	REQUIRE(recorder.transaction_states_destroyed == 1);
}

TEST_CASE("V2 vcatalog: registration validates the vcatalog", "[capi_v2][vcatalog]") {
	Recorder recorder;
	EnvFixture fx;

	SECTION("a name is required") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, std::string_view(), &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("an attach callback is required") {
		auto vcatalog = MakeVCatalog(fx.factory, TYPE_NAME, &recorder);
		REQUIRE(duckdb_v2_vcatalog_set_attach_callback(vcatalog, nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, vcatalog) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("a name can only be registered once") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, "VTEST", &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("DuckDB's own database type cannot be taken") {
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, "duckdb", &recorder)) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("registration is not transactional") {
		REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, MakeVCatalog(fx.factory, TYPE_NAME, &recorder)) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("null handles are rejected, and destroy is null-safe") {
		REQUIRE(duckdb_v2_connection_register_vcatalog(fx.conn, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vcatalog_create(fx.factory, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vcatalog_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vcatalog_handle vcatalog = nullptr;
		REQUIRE(duckdb_v2_vcatalog_destroy(&vcatalog) == DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 vcatalog: the user data lives as long as the instance", "[capi_v2][vcatalog]") {
	bool destroyed = false;
	{
		EnvFixture fx;
		auto vcatalog = MakeVCatalog(fx.factory, TYPE_NAME, nullptr);
		duckdb_v2_opaque data = {&destroyed, [](void *ptr) { *static_cast<bool *>(ptr) = true; }, nullptr};
		REQUIRE(duckdb_v2_vcatalog_set_user_data(vcatalog, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RegisterVCatalog(fx.conn, vcatalog) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(destroyed);
	}
	REQUIRE(destroyed);
}

} // namespace test_capi_v2
