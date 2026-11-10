#include "test_capi_v2.hpp"

#include <cctype>
#include <string>
#include <vector>

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
	//! The schemas every database has
	std::vector<std::string> schema_names = {"main", "other"};
	idx_t schema_lookups = 0;
	std::string last_schema_lookup;
	idx_t schema_scans = 0;
	idx_t schemas_destroyed = 0;
	//! Whether every catalog callback received a context
	bool contexts_seen = true;
	//! A schema of the first database whose schemas were created, while it is attached
	duckdb_v2_vcatalog_schema_handle first_schema = nullptr;

	bool fail_attach = false;
	bool fail_begin = false;
	bool fail_commit = false;
	bool fail_rollback = false;
	bool fail_schema_lookup = false;
	//! Resolve every schema lookup to `first_schema`
	bool resolve_to_first_schema = false;
};

struct CatalogState {
	Recorder *recorder;
	//! The schemas of the database, in the order of the recorder's names. Created on first use, destroyed on DETACH.
	std::vector<duckdb_v2_vcatalog_schema_handle> schemas;
};

void DestroyCatalogState(void *ptr) {
	auto state = static_cast<CatalogState *>(ptr);
	auto &recorder = *state->recorder;
	recorder.catalog_states_destroyed++;
	for (auto &schema : state->schemas) {
		if (recorder.first_schema == schema) {
			recorder.first_schema = nullptr;
		}
		duckdb_v2_vcatalog_schema_destroy(&schema);
		recorder.schemas_destroyed++;
	}
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

void Attach(duckdb_v2_vcatalog_attach_info_handle info, duckdb_v2_vcatalog_database_handle database,
            duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
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
	// The database has no catalog state until the callback sets one
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (catalog_state) {
		recorder.states_seen = false;
	}
	// Set before failing, so a failed ATTACH still has a state to destroy
	duckdb_v2_opaque state = {new CatalogState {&recorder}, DestroyCatalogState, nullptr};
	if (duckdb_v2_vcatalog_attach_set_catalog_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		DestroyCatalogState(state.ptr);
		return;
	}
	if (duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (catalog_state != state.ptr) {
		recorder.states_seen = false;
	}
	if (recorder.fail_attach) {
		Fail(err, "attach refused");
	}
}

void Begin(duckdb_v2_vcatalog_transaction_begin_info_handle info, duckdb_v2_vcatalog_database_handle database,
           duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
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

void Commit(duckdb_v2_vcatalog_transaction_commit_info_handle info, duckdb_v2_vcatalog_database_handle database,
            duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
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

void Rollback(duckdb_v2_vcatalog_transaction_rollback_info_handle info, duckdb_v2_vcatalog_database_handle database,
              duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	void *transaction_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE ||
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

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
			return false;
		}
	}
	return true;
}

// The recorder and catalog state of the database a catalog callback runs on.
bool GetStates(duckdb_v2_vcatalog_database_handle database, Recorder *&recorder, CatalogState *&state,
               duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *catalog_state = nullptr;
	if (duckdb_v2_vcatalog_database_get_user_data(database, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_database_get_catalog_state(database, &catalog_state, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	recorder = static_cast<Recorder *>(user_data);
	state = static_cast<CatalogState *>(catalog_state);
	if (state->recorder != recorder) {
		recorder->states_seen = false;
	}
	return true;
}

// Creates the schemas of the database on first use, each carrying the catalog state as its own state.
bool EnsureSchemas(duckdb_v2_vcatalog_database_handle database, CatalogState &state, duckdb_v2_error_info_handle *err) {
	if (!state.schemas.empty()) {
		return true;
	}
	for (auto &name : state.recorder->schema_names) {
		duckdb_v2_vcatalog_schema_handle schema = nullptr;
		if (duckdb_v2_vcatalog_schema_create(database, name.data(), name.size(), &schema, err) !=
		    DUCKDB_V2_ERROR_NONE) {
			return false;
		}
		state.schemas.push_back(schema);
		duckdb_v2_opaque schema_state = {&state, nullptr, nullptr};
		if (duckdb_v2_vcatalog_schema_set_state(schema, &schema_state, err) != DUCKDB_V2_ERROR_NONE) {
			return false;
		}
	}
	if (!state.recorder->first_schema) {
		state.recorder->first_schema = state.schemas[0];
	}
	return true;
}

// Checks that a schema reports the database, state and name it was created with.
void CheckSchema(duckdb_v2_vcatalog_schema_handle schema, duckdb_v2_vcatalog_database_handle database,
                 CatalogState &state, std::string_view name) {
	duckdb_v2_vcatalog_database_handle schema_database = nullptr;
	void *schema_state = nullptr;
	const char *schema_name = nullptr;
	idx_t schema_name_length = 0;
	if (duckdb_v2_vcatalog_schema_get_database(schema, &schema_database, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_get_state(schema, &schema_state, nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_get_name(schema, &schema_name, &schema_name_length, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    schema_database != database || schema_state != &state ||
	    std::string_view(schema_name, schema_name_length) != name) {
		state.recorder->states_seen = false;
	}
}

void SchemaLookup(duckdb_v2_vcatalog_schema_lookup_info_handle info, duckdb_v2_vcatalog_database_handle database,
                  duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	const char *name = nullptr;
	idx_t name_length = 0;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_schema_lookup_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vcatalog_schema_lookup_get_name(info, &name, &name_length, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->schema_lookups++;
	recorder->last_schema_lookup.assign(name, name_length);
	if (!context) {
		recorder->contexts_seen = false;
	}
	// The begin callback sets a transaction state for every transaction
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (recorder->fail_schema_lookup) {
		Fail(err, "schema lookup refused");
		return;
	}
	if (recorder->resolve_to_first_schema && recorder->first_schema) {
		duckdb_v2_vcatalog_schema_lookup_set_schema(info, recorder->first_schema, err);
		return;
	}
	if (!EnsureSchemas(database, *state, err)) {
		return;
	}
	for (idx_t i = 0; i < state->schemas.size(); i++) {
		if (EqualsIgnoreCase(recorder->schema_names[i], recorder->last_schema_lookup)) {
			CheckSchema(state->schemas[i], database, *state, recorder->schema_names[i]);
			duckdb_v2_vcatalog_schema_lookup_set_schema(info, state->schemas[i], err);
			return;
		}
	}
}

void SchemaScan(duckdb_v2_vcatalog_schema_scan_info_handle info, duckdb_v2_vcatalog_database_handle database,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	Recorder *recorder = nullptr;
	CatalogState *state = nullptr;
	void *transaction_state = nullptr;
	if (!GetStates(database, recorder, state, err) ||
	    duckdb_v2_vcatalog_schema_scan_get_transaction_state(info, &transaction_state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	recorder->schema_scans++;
	if (!context) {
		recorder->contexts_seen = false;
	}
	if (!transaction_state || static_cast<TransactionState *>(transaction_state)->catalog != state) {
		recorder->states_seen = false;
	}
	if (!EnsureSchemas(database, *state, err)) {
		return;
	}
	for (idx_t i = 0; i < state->schemas.size(); i++) {
		CheckSchema(state->schemas[i], database, *state, recorder->schema_names[i]);
		if (duckdb_v2_vcatalog_schema_scan_add_schema(info, state->schemas[i], err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// A schema added twice is listed once
	duckdb_v2_vcatalog_schema_scan_add_schema(info, state->schemas[0], err);
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
	REQUIRE(duckdb_v2_vcatalog_set_schema_lookup_callback(vcatalog, SchemaLookup, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_set_schema_scan_callback(vcatalog, SchemaScan, nullptr) == DUCKDB_V2_ERROR_NONE);
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

idx_t CountRows(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto count = DrainRowCount(result);
	duckdb_v2_result_destroy(&result);
	return count;
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
	REQUIRE(recorder.states_seen);

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

TEST_CASE("V2 vcatalog: schemas resolve through the lookup callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	SECTION("a schema the lookup resolves") {
		REQUIRE(RunSql(conn, "USE v.other") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_lookup == "other");
	}
	SECTION("the default schema") {
		REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_lookup == "main");
	}
	SECTION("the lookup decides how names match") {
		REQUIRE(RunSql(conn, "USE v.OTHER") == DUCKDB_V2_ERROR_NONE);
		REQUIRE(recorder.last_schema_lookup == "OTHER");
	}
	SECTION("a name the lookup does not resolve") {
		REQUIRE(Contains(ErrorText(conn, "USE v.nope"), "nope"));
	}
	SECTION("an error the lookup reports fails the statement") {
		recorder.fail_schema_lookup = true;
		REQUIRE(Contains(ErrorText(conn, "USE v.other"), "schema lookup refused"));
	}
	REQUIRE(recorder.schema_lookups > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: schemas are listed through the scan callback", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);

	// "main" is added twice, and listed once
	REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v'") == 2);
	REQUIRE(CountRows(conn, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'v' AND schema_name = "
	                        "'other'") == 1);
	REQUIRE(recorder.schema_scans > 0);
	REQUIRE(recorder.states_seen);
	REQUIRE(recorder.contexts_seen);
}

TEST_CASE("V2 vcatalog: the implementation owns its schemas", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE memory") == DUCKDB_V2_ERROR_NONE);
	// Kept in the catalog state across transactions, and destroyed with it
	REQUIRE(recorder.schemas_destroyed == 0);
	REQUIRE(RunSql(conn, "DETACH v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.schemas_destroyed == 2);
}

TEST_CASE("V2 vcatalog: a lookup cannot resolve to a schema of another database", "[capi_v2][vcatalog]") {
	VCatalogFixture vfx;
	auto &recorder = vfx.recorder;
	auto conn = vfx.fx.conn;
	REQUIRE(RunSql(conn, "ATTACH 'p' AS v (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "ATTACH 'q' AS w (TYPE vtest)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE v") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(conn, "USE memory") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder.first_schema);

	recorder.resolve_to_first_schema = true;
	auto error = ErrorText(conn, "USE w");
	CAPTURE(error, recorder.last_schema_lookup);
	REQUIRE(Contains(error, "different database"));
}

TEST_CASE("V2 vcatalog: schema arguments are checked", "[capi_v2][vcatalog]") {
	REQUIRE(duckdb_v2_vcatalog_schema_create(nullptr, "s", 1, nullptr, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vcatalog_schema_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vcatalog_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_vcatalog_schema_destroy(&schema) == DUCKDB_V2_ERROR_NONE);
	void *state = nullptr;
	REQUIRE(duckdb_v2_vcatalog_schema_get_state(nullptr, &state, nullptr) != DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
