#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"
#include "test_helpers.hpp"

#include <cstring>

// ---------------------------------------------------------------------------
// V2 smoke / lifecycle tests for env, open/close, connect/disconnect.
// Option set/get against database/connection lands in a follow-up — this
// file currently exercises the foundation only.
// ---------------------------------------------------------------------------

TEST_CASE("V2: env create / destroy", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env != nullptr);
	idx_t count = 99;
	REQUIRE(duckdb_v2_environment_database_count(env, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env == nullptr);
}

TEST_CASE("V2: open / close in-memory database", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db != nullptr);

	idx_t count = 0;
	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 1);

	REQUIRE(duckdb_v2_close(&db) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db == nullptr);

	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 0);

	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: destroy_environment refuses while databases are open", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(env != nullptr); // refusal leaves env intact

	duckdb_v2_close(&db);
	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: open with pre-open option handles", "[capi_v2][db][option]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &opt, nullptr);
	duckdb_v2_option_handle opts[] = {opt};

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_close(&db);
	duckdb_v2_option_destroy(&opt);
	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: file-based open rejects second open of same file", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	auto path = duckdb::TestCreatePath("v2_test_open.db");

	duckdb_v2_database_handle db_a = nullptr;
	REQUIRE(duckdb_v2_open(env, V2Str(path.c_str()), nullptr, 0, &db_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_database_handle db_b = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_open(env, V2Str(path.c_str()), nullptr, 0, &db_b, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(db_b == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_close(&db_a);

	// After close, reopen succeeds (the path slot is freed).
	REQUIRE(duckdb_v2_open(env, V2Str(path.c_str()), nullptr, 0, &db_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_close(&db_b);

	duckdb_v2_destroy_environment(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: connect / disconnect", "[capi_v2][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);
	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);

	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: null-arg validation on env / db / conn entrypoints", "[capi_v2][env][db][conn]") {
	SECTION("create_environment rejects null out_env") {
		REQUIRE(duckdb_v2_create_environment(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("destroy_environment with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_destroy_environment(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("open rejects null env") {
		duckdb_v2_database_handle db = nullptr;
		REQUIRE(duckdb_v2_open(nullptr, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("open rejects null out_db") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_destroy_environment(&env);
	}
	SECTION("open rejects option_count > 0 with null options") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_database_handle db = nullptr;
		REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 1, &db, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_destroy_environment(&env);
	}
	SECTION("close with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_close(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connect rejects null db") {
		duckdb_v2_connection_handle conn = nullptr;
		REQUIRE(duckdb_v2_connect(nullptr, &conn, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("disconnect with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_disconnect(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
}

// ---------------------------------------------------------------------------
// duckdb_v2_database_option_*  /  duckdb_v2_connection_option_*
//
// Phase 2 tests: option set/get against database (GLOBAL) and connection
// (AUTOMATIC / GLOBAL / LOCAL). Scope enforcement reuses
// PhysicalSet::GetSettingScope so error messages are DuckDB's own.
// ---------------------------------------------------------------------------

namespace {

// Tiny RAII helper to keep the env+db+conn lifecycle out of every test
// body. Construct once per test case; destruction tears down in reverse.
struct V2Fixture {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	V2Fixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~V2Fixture() {
		duckdb_v2_disconnect(&conn);
		duckdb_v2_close(&db);
		duckdb_v2_destroy_environment(&env);
	}
};

} // namespace

TEST_CASE("V2 db option: set + get round-trip", "[capi_v2][db][option]") {
	V2Fixture fx;

	// Read the default before mutating so we can compare against it.
	duckdb_v2_option_handle before = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, V2Str("memory_limit"), &before, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str before_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(before, &before_setting, nullptr);
	std::string default_value = V2StrTo(before_setting);
	duckdb_v2_option_destroy(&before);

	duckdb_v2_option_handle in_opt = nullptr;
	duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &in_opt, nullptr);
	REQUIRE(duckdb_v2_database_option_set(fx.db, in_opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&in_opt);

	duckdb_v2_option_handle after = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, V2Str("memory_limit"), &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str after_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(after, &after_setting, nullptr);
	REQUIRE(after_setting.ptr != nullptr);
	REQUIRE(after_setting != default_value); // mutation visible
	duckdb_v2_option_destroy(&after);
}

TEST_CASE("V2 db option: get populates description and aliases", "[capi_v2][db][option]") {
	V2Fixture fx;

	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, V2Str("memory_limit"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_option_get_name(opt, &name, nullptr);
	// "memory_limit" is an alias; the canonical name is something else
	// (e.g. "max_memory"). Either way, the alias list should contain
	// "memory_limit".
	idx_t alias_count = 0;
	duckdb_v2_option_get_alias_count(opt, &alias_count, nullptr);
	bool has_memory_limit = false;
	for (idx_t i = 0; i < alias_count; i++) {
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_option_get_alias(opt, i, &alias, nullptr);
		if (alias == "memory_limit") {
			has_memory_limit = true;
			break;
		}
	}
	REQUIRE(has_memory_limit);

	duckdb_v2_str desc = {nullptr, 0};
	duckdb_v2_option_get_description(opt, &desc, nullptr);
	REQUIRE(desc.ptr != nullptr);
	REQUIRE(desc.len != 0);

	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 db option: get unknown name errors", "[capi_v2][db][option]") {
	V2Fixture fx;
	duckdb_v2_option_handle out = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, V2Str("this_option_does_not_exist"), &out, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 db option: get_count and get_by_index", "[capi_v2][db][option]") {
	V2Fixture fx;
	idx_t count = 0;
	REQUIRE(duckdb_v2_database_option_get_count(fx.db, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count > 0);

	// Walk the first few entries — each should produce a populated handle.
	idx_t to_check = count < 5 ? count : 5;
	for (idx_t i = 0; i < to_check; i++) {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, i, &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &name, nullptr);
		REQUIRE(name.ptr != nullptr);
		REQUIRE(name.len != 0);
		duckdb_v2_option_destroy(&opt);
	}

	duckdb_v2_option_handle out_of_range = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, count + 100, &out_of_range, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 conn option: set LOCAL is invisible to other connections", "[capi_v2][conn][option]") {
	V2Fixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	// max_execution_time is LOCAL_DEFAULT, so a LOCAL-scope write stays
	// session-local — perfect for this test.
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(V2Str("max_execution_time"), V2Str("5000"), &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_LOCAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt);

	duckdb_v2_option_handle on_fx = nullptr;
	duckdb_v2_connection_option_get(fx.conn, V2Str("max_execution_time"), &on_fx, nullptr);
	duckdb_v2_str fx_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(on_fx, &fx_setting, nullptr);
	REQUIRE(fx_setting == "5000");
	duckdb_v2_option_destroy(&on_fx);

	duckdb_v2_option_handle on_other = nullptr;
	duckdb_v2_connection_option_get(other, V2Str("max_execution_time"), &on_other, nullptr);
	duckdb_v2_str other_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(on_other, &other_setting, nullptr);
	// The other connection sees the static default ("0"), not "5000".
	REQUIRE(other_setting != "5000");
	duckdb_v2_option_destroy(&on_other);

	duckdb_v2_disconnect(&other);
}

TEST_CASE("V2 conn option: set GLOBAL is visible everywhere", "[capi_v2][conn][option]") {
	V2Fixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(V2Str("memory_limit"), V2Str("2GB"), &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_GLOBAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt);

	std::string fx_setting, other_setting;
	for (auto target : {fx.conn, other}) {
		duckdb_v2_option_handle seen = nullptr;
		duckdb_v2_connection_option_get(target, V2Str("memory_limit"), &seen, nullptr);
		duckdb_v2_str setting = {nullptr, 0};
		duckdb_v2_option_get_setting(seen, &setting, nullptr);
		(target == fx.conn ? fx_setting : other_setting) = V2StrTo(setting);
		duckdb_v2_option_destroy(&seen);
	}
	REQUIRE(!fx_setting.empty());
	REQUIRE(fx_setting == other_setting); // GLOBAL write seen identically by both

	duckdb_v2_disconnect(&other);
}

TEST_CASE("V2 conn option: scope enforcement matches SQL", "[capi_v2][conn][option]") {
	V2Fixture fx;
	duckdb_v2_error_info_handle err = nullptr;

	// GLOBAL_ONLY × LOCAL: rejected. allow_community_extensions is GLOBAL_ONLY.
	duckdb_v2_option_handle global_only = nullptr;
	duckdb_v2_option_create(V2Str("allow_community_extensions"), V2Str("false"), &global_only, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, global_only, DUCKDB_V2_SETTING_SCOPE_LOCAL, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_option_destroy(&global_only);
}

TEST_CASE("V2 conn option: AUTOMATIC scope mirrors bare SQL `SET`", "[capi_v2][conn][option]") {
	V2Fixture fx;
	// max_execution_time is LOCAL_DEFAULT → AUTOMATIC resolves to SESSION
	// → write succeeds.
	duckdb_v2_option_handle local = nullptr;
	duckdb_v2_option_create(V2Str("max_execution_time"), V2Str("5000"), &local, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, local, DUCKDB_V2_SETTING_SCOPE_AUTOMATIC, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&local);
}

TEST_CASE("V2 db/conn option: open with options applies them at GLOBAL scope", "[capi_v2][db][option]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_handle o1 = nullptr;
	duckdb_v2_option_create(V2Str("memory_limit"), V2Str("2GB"), &o1, nullptr);
	duckdb_v2_option_handle opts[] = {o1};

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_option_handle seen = nullptr;
	duckdb_v2_database_option_get(db, V2Str("memory_limit"), &seen, nullptr);
	duckdb_v2_str setting = {nullptr, 0};
	duckdb_v2_option_get_setting(seen, &setting, nullptr);
	REQUIRE(setting.ptr != nullptr);
	REQUIRE(setting.len > 0);
	// Compare against the un-set baseline by opening a second db.
	duckdb_v2_database_handle db_default = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db_default, nullptr);
	duckdb_v2_option_handle def_opt = nullptr;
	duckdb_v2_database_option_get(db_default, V2Str("memory_limit"), &def_opt, nullptr);
	duckdb_v2_str def_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(def_opt, &def_setting, nullptr);
	REQUIRE(V2StrTo(setting) != def_setting);
	duckdb_v2_option_destroy(&def_opt);
	duckdb_v2_close(&db_default);
	duckdb_v2_option_destroy(&seen);

	duckdb_v2_option_destroy(&o1);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

// ---------------------------------------------------------------------------
// Internal helper that bridge implementations use to report failures:
//
//   - The return value always carries the error code (authoritative).
//   - If `err` is non-null, `SetErrorInfo` writes the info into the slot
//     (lazy-allocating on first use, overwriting in place thereafter).
//   - There is no success-path helper: successful calls leave the slot
//     untouched. Callers that reuse a slot must clear it themselves.
//   - SetErrorInfo is safe to call with err == nullptr.
// ---------------------------------------------------------------------------

TEST_CASE("V2 error: SetErrorInfo helper", "[capi_v2][error]") {
	SECTION("SetErrorInfo allocates an info and returns the code") {
		duckdb_v2_error_info_handle err = nullptr;
		auto rc = duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, "bad input");
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);

		duckdb_v2_str msg = {nullptr, 0};
		REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(msg.ptr != nullptr);
		REQUIRE(msg == "bad input");

		duckdb_v2_error_info_destroy(&err);
		REQUIRE(err == nullptr);
	}

	SECTION("SetErrorInfo with null message produces an empty message") {
		duckdb_v2_error_info_handle err = nullptr;
		auto rc = duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, nullptr);
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);

		duckdb_v2_str msg = {nullptr, 0};
		REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(msg.ptr != nullptr);
		REQUIRE(msg.len == 0);

		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("SetErrorInfo preserves arbitrarily long messages") {
		duckdb_v2_error_info_handle err = nullptr;
		std::string long_msg(4096, 'x');
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, long_msg.c_str());

		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(msg.len == long_msg.size());

		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("SetErrorInfo with nullptr err returns the code and allocates nothing") {
		auto rc = duckdb::SetErrorInfo(nullptr, DUCKDB_V2_ERROR_INVALID_INPUT, "ignored");
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	SECTION("SetErrorInfo replaces a pre-existing info's message") {
		duckdb_v2_error_info_handle err = nullptr;
		duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, "first");
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, "second");
		REQUIRE(err != nullptr);

		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(msg == "second");

		duckdb_v2_error_info_destroy(&err);
	}
}

TEST_CASE("V2 error: error_info_destroy is null-safe", "[capi_v2][error]") {
	SECTION("destroying a null handle is a no-op") {
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_error_info_destroy(&err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(err == nullptr);
	}

	SECTION("destroying via a null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_error_info_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("detach + destroy preserves info independently of the original slot") {
		duckdb_v2_error_info_handle err = nullptr;
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, "boom");

		// Transfer ownership out of `err` — the original slot is now detached.
		duckdb_v2_error_info_handle saved = err;
		err = nullptr;

		duckdb_v2_str msg = {nullptr, 0};
		REQUIRE(duckdb_v2_error_info_get_text(saved, &msg) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(msg == "boom");
		duckdb_v2_error_info_destroy(&saved);
		REQUIRE(saved == nullptr);
	}
}

// ---------------------------------------------------------------------------
// WithErrorHandler is the universal err-translation primitive used at every
// V2 bridge entry point. It writes the slot only on failure (lazy-allocating
// on first use, overwriting in place thereafter); a successful call leaves
// the slot untouched. End of slot lifetime is the caller's job
// (`error_info_destroy`).
// ---------------------------------------------------------------------------

TEST_CASE("V2 error: WithErrorHandler success leaves the err slot untouched", "[capi_v2][error]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connect(db, &conn, nullptr);

	// A successful call with a fresh (null) slot does not allocate: the return
	// code is authoritative, so the library never touches the slot on success.
	duckdb_v2_error_info_handle err = nullptr;
	duckdb_v2_file_system_handle fs = nullptr;
	REQUIRE(duckdb_v2_file_system_get_from_connection(conn, &fs, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err == nullptr);
	REQUIRE(fs != nullptr);

	// Seed the slot with a failure, then make a successful call reusing the
	// same slot. The stale info is NOT cleared — success leaves it as-is, and
	// it is the caller's responsibility to clear before relying on it again.
	REQUIRE(duckdb_v2_file_system_get_from_connection(nullptr, &fs, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);

	REQUIRE(duckdb_v2_file_system_get_from_connection(conn, &fs, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr); // untouched: the failure's info still sits in the slot
	duckdb_v2_error_code_t code = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_get_code(err, &code);
	REQUIRE(code == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_error_info_destroy(&err);
	REQUIRE(err == nullptr);

	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2 error: WithErrorHandler failure overwrites the prior message in the slot", "[capi_v2][error]") {
	duckdb_v2_error_info_handle err = nullptr;

	// First failing call: null out_file_system yields
	// "Output file system pointer cannot be null."
	duckdb_v2_file_system_handle *no_out = nullptr;
	REQUIRE(duckdb_v2_file_system_get_from_connection(nullptr, no_out, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	{
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(V2StrTo(msg).find("Output file system pointer") != std::string::npos);
	}

	// Second failing call: out_file_system is valid but connection is null,
	// yielding "Connection pointer cannot be null." The slot is reused; the
	// message is overwritten in place. No reallocation, no destroy.
	duckdb_v2_file_system_handle fs = nullptr;
	REQUIRE(duckdb_v2_file_system_get_from_connection(nullptr, &fs, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	{
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(V2StrTo(msg).find("Connection pointer") != std::string::npos);
		REQUIRE(V2StrTo(msg).find("Output file system pointer") == std::string::npos);
	}

	duckdb_v2_error_info_destroy(&err);
	REQUIRE(err == nullptr);
}

// ---------------------------------------------------------------------------
// duckdb_v2_option: lifecycle + accessors. The option handle is a pure data
// container in this round — database/connection get paths land in a follow-up.
// Until then, options carry only the (name, setting) pair the user supplied;
// description, default_setting, target_scope, and aliases stay empty/UNKNOWN.
// ---------------------------------------------------------------------------

TEST_CASE("V2 option: create / destroy", "[capi_v2][option]") {
	SECTION("create succeeds and destroy nulls the slot") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt != nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt == nullptr);
	}

	SECTION("create with empty name and setting succeeds (strings are just copied)") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(V2Str(""), V2Str(""), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_str setting = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &name, nullptr);
		duckdb_v2_option_get_setting(opt, &setting, nullptr);
		REQUIRE(name.len == 0);
		REQUIRE(setting.len == 0);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("create rejects a malformed name view (null ptr, nonzero len)") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, V2Str("x"), &opt, &err) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(opt == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("create rejects a malformed setting view (null ptr, nonzero len)") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(V2Str("x"), duckdb_v2_str {nullptr, 1}, &opt, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(opt == nullptr);
	}

	SECTION("create with an empty (null, 0) name and setting succeeds") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 0}, duckdb_v2_str {nullptr, 0}, &opt, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt != nullptr);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("create rejects null out_option") {
		REQUIRE(duckdb_v2_option_create(V2Str("x"), V2Str("y"), nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	SECTION("destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_option_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("destroy on already-null slot is a no-op") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("double destroy is safe (slot was nulled by first destroy)") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &opt, nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 option: accessors round-trip user-supplied values", "[capi_v2][option]") {
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_option_create(V2Str("memory_limit"), V2Str("2GB"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("get_name returns the user-supplied name") {
		duckdb_v2_str name = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_name(opt, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(name == "memory_limit");
	}

	SECTION("get_setting returns the user-supplied setting") {
		duckdb_v2_str setting = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_setting(opt, &setting, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(setting == "2GB");
	}

	SECTION("get_default_setting returns empty string until populated by a get") {
		duckdb_v2_str def = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_default_setting(opt, &def, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(def.len == 0);
	}

	SECTION("get_description returns empty string until populated by a get") {
		duckdb_v2_str desc = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_description(opt, &desc, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(desc.len == 0);
	}

	SECTION("get_target_scope returns UNKNOWN until populated by a get") {
		DUCKDB_V2_OPTION_TARGET_SCOPE scope = DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY;
		REQUIRE(duckdb_v2_option_get_target_scope(opt, &scope, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(scope == DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN);
	}

	SECTION("get_alias_count returns 0 until populated by a get") {
		idx_t count = 99;
		REQUIRE(duckdb_v2_option_get_alias_count(opt, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == 0);
	}

	SECTION("get_alias on empty alias list returns INVALID_INPUT") {
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 0, &alias, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(alias.ptr == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}

	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 option: accessor null-arg validation", "[capi_v2][option]") {
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(V2Str("k"), V2Str("v"), &opt, nullptr);

	SECTION("get_name rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_name(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_name rejects null out_name") {
		REQUIRE(duckdb_v2_option_get_name(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_setting rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_setting rejects null out_setting") {
		REQUIRE(duckdb_v2_option_get_setting(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_default_setting rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_default_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_description rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_description(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_target_scope rejects null option") {
		DUCKDB_V2_OPTION_TARGET_SCOPE s;
		REQUIRE(duckdb_v2_option_get_target_scope(nullptr, &s, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_target_scope rejects null out_target_scope") {
		REQUIRE(duckdb_v2_option_get_target_scope(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_alias_count rejects null option") {
		idx_t c;
		REQUIRE(duckdb_v2_option_get_alias_count(nullptr, &c, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_alias rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_alias(nullptr, 0, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 option: handles are independent", "[capi_v2][option]") {
	// Two options created back-to-back must not alias each other's storage,
	// and destroying one must not affect the other.
	duckdb_v2_option_handle a = nullptr;
	duckdb_v2_option_handle b = nullptr;
	duckdb_v2_option_create(V2Str("memory_limit"), V2Str("1GB"), &a, nullptr);
	duckdb_v2_option_create(V2Str("threads"), V2Str("4"), &b, nullptr);

	duckdb_v2_str name_a = {nullptr, 0};
	duckdb_v2_str name_b = {nullptr, 0};
	duckdb_v2_option_get_name(a, &name_a, nullptr);
	duckdb_v2_option_get_name(b, &name_b, nullptr);
	REQUIRE(name_a == "memory_limit");
	REQUIRE(name_b == "threads");

	duckdb_v2_option_destroy(&a);
	REQUIRE(a == nullptr);

	// b's accessors still work after a's destruction.
	duckdb_v2_str still_b = {nullptr, 0};
	REQUIRE(duckdb_v2_option_get_name(b, &still_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(still_b == "threads");

	duckdb_v2_option_destroy(&b);
}

TEST_CASE("V2 option: borrowed pointers stay valid until destroy", "[capi_v2][option]") {
	// Per the contract, accessors return borrowed pointers valid until the
	// option is destroyed. Repeated reads must return stable pointers
	// (the strings are owned by the wrapper and don't move on access).
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(V2Str("foo"), V2Str("bar"), &opt, nullptr);

	duckdb_v2_str first_name = {nullptr, 0};
	duckdb_v2_str second_name = {nullptr, 0};
	duckdb_v2_option_get_name(opt, &first_name, nullptr);
	duckdb_v2_option_get_name(opt, &second_name, nullptr);
	REQUIRE(first_name.ptr == second_name.ptr); // borrowed pointer is stable across reads
	REQUIRE(first_name == "foo");

	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 option: error info is populated on failure paths", "[capi_v2][option]") {
	SECTION("create with a malformed name view surfaces a descriptive error") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, V2Str("v"), &opt, &err) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(V2StrTo(msg).find("duckdb_v2_option_create") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("get_alias out-of-range surfaces a descriptive error") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_option_create(V2Str("k"), V2Str("v"), &opt, nullptr);
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 5, &alias, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(V2StrTo(msg).find("out of range") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("err == nullptr is tolerated on every failure path") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, V2Str("v"), &opt, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_option_create(V2Str("k"), V2Str("v"), &opt, nullptr);
		duckdb_v2_str alias = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_alias(opt, 99, &alias, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_option_destroy(&opt);
	}
}

// ---------------------------------------------------------------------------
// Scalar Function API
// ---------------------------------------------------------------------------
TEST_CASE("V2 scalar: create / destroy", "[capi_v2][scalar]") {
	// Setup a minimal environment + database so we can get a connection handle for the
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);

	// Function callbacks
	static auto bind_callback = [](duckdb_v2_scalar_function_bind_args *args, duckdb_v2_error_info_handle *err) {
		/* TODO */
	};

	static auto init_callback = [](duckdb_v2_scalar_function_init_args *args, duckdb_v2_error_info_handle *err) {
		/* TODO */
	};

	static auto exec_callback = [](duckdb_v2_scalar_function_exec_args *args, duckdb_v2_error_info_handle *err) {
		duckdb_v2_data_chunk_handle chunk = args->input;

		duckdb_v2_vector_handle lhs_vec = nullptr;
		duckdb_v2_vector_handle rhs_vec = nullptr;
		duckdb_v2_vector_handle out_vec = args->result;

		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &lhs_vec, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &rhs_vec, err) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_view lhs_view;
		duckdb_v2_vector_view rhs_view;

		REQUIRE(duckdb_v2_vector_get_view(lhs_vec, &lhs_view, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_get_view(rhs_vec, &rhs_view, err) == DUCKDB_V2_ERROR_NONE);

		const auto lhs_data = static_cast<const int32_t *>(lhs_view.data);
		const auto rhs_data = static_cast<const int32_t *>(rhs_view.data);

		int32_t *out_data = nullptr;
		REQUIRE(duckdb_v2_vector_get_data_mutable(out_vec, (void **)&out_data, err) == DUCKDB_V2_ERROR_NONE);

		for (idx_t i = 0; i < lhs_view.count; i++) {
			const auto lhs_idx = lhs_view.sel ? lhs_view.sel[i] : i;
			const auto rhs_idx = rhs_view.sel ? rhs_view.sel[i] : i;

			out_data[i] = lhs_data[lhs_idx] + rhs_data[rhs_idx];
		}
	};

	SECTION("Basic registration and cleanup") {
		// Run in transaction to get a context we can register within

		duckdb_v2_connection_execute_with_context(
		    conn,
		    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
			    duckdb_v2_scalar_function_builder_handle builder = nullptr;
			    REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Make type
			    duckdb_v2_logical_type_handle type = nullptr;
			    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Setup parameters
			    REQUIRE(duckdb_v2_scalar_function_builder_add_parameter(builder, V2Str("a"), type, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_add_parameter(builder, V2Str("b"), type, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Also add return type
			    REQUIRE(duckdb_v2_scalar_function_builder_set_return_type(builder, type, err) == DUCKDB_V2_ERROR_NONE);

			    // Setup function callbacks
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(builder, V2Str("my_func"), err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(builder, bind_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(builder, init_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(builder, exec_callback, err) ==
			            DUCKDB_V2_ERROR_NONE);

			    // Expected-failure probes opt out of detail (nullptr err): writing
			    // their codes into the callback's slot would fail the whole scope,
			    // since a non-NONE code left on the slot at return is the callback's
			    // failure signal.

			    // Empty name not supported
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(builder, V2Str(""), nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);

			    // Does not work with NULL
			    REQUIRE(duckdb_v2_scalar_function_builder_set_name(nullptr, V2Str("my_func"), nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(nullptr, bind_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(nullptr, init_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);
			    REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(nullptr, exec_callback, nullptr) ==
			            DUCKDB_V2_ERROR_INVALID_INPUT);

			    // Register
			    REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Default is ALTER_ON_CONFLICT, and so we can register twice
			    REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);

			    // Destroy should null out the builder handle and be safe to call twice.
			    REQUIRE(duckdb_v2_scalar_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
			    REQUIRE(builder == nullptr);
			    REQUIRE(duckdb_v2_scalar_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);

			    duckdb_v2_logical_type_destroy(&type);
		    },
		    nullptr, nullptr);
	}

	// Now get the result
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, "SELECT my_func(1, 2)", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	duckdb_v2_vector_handle result_vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &result_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view result_view;
	REQUIRE(duckdb_v2_vector_get_view(result_vec, &result_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_view.count == 1);

	REQUIRE((static_cast<const int32_t *>(result_view.data))[0] == 3);

	// Cleanup
	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}
