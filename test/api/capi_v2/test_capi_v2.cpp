#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "test_helpers.hpp"

#include <cstring>

// ---------------------------------------------------------------------------
// V2 smoke / lifecycle tests for env, open/close, connect/disconnect.
// Option set/get against database/connection lands in a follow-up — this
// file currently exercises the foundation only.
// ---------------------------------------------------------------------------

TEST_CASE("V2: env create / destroy", "[capi_v2][env]") {
	duckdb_v2_environment_ptr env = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env != nullptr);
	idx_t count = 99;
	REQUIRE(duckdb_v2_environment_database_count(env, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	REQUIRE(duckdb_v2_destroy_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env == nullptr);
}

TEST_CASE("V2: open / close in-memory database", "[capi_v2][db]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_ptr db = nullptr;
	REQUIRE(duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db != nullptr);

	idx_t count = 0;
	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 1);

	REQUIRE(duckdb_v2_close(&db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db == nullptr);

	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 0);

	duckdb_v2_destroy_environment(&env, nullptr);
}

TEST_CASE("V2: destroy_environment refuses while databases are open", "[capi_v2][env]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);

	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_destroy_environment(&env, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(env != nullptr); // refusal leaves env intact
	REQUIRE(err != nullptr);
	const char *msg = nullptr;
	duckdb_v2_error_info_get_message(err, &msg, nullptr);
	REQUIRE(std::string(msg).find("still open") != std::string::npos);
	duckdb_v2_error_info_destroy(&err, nullptr);

	duckdb_v2_close(&db, nullptr);
	REQUIRE(duckdb_v2_destroy_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: open with pre-open option handles", "[capi_v2][db][option]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("memory_limit", "1GB", &opt, nullptr);
	duckdb_v2_option_ptr opts[] = {opt};

	duckdb_v2_database_ptr db = nullptr;
	REQUIRE(duckdb_v2_open(env, nullptr, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_close(&db, nullptr);
	duckdb_v2_option_destroy(&opt, nullptr);
	duckdb_v2_destroy_environment(&env, nullptr);
}

TEST_CASE("V2: file-based open rejects second open of same file", "[capi_v2][db]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	auto path = duckdb::TestCreatePath("v2_test_open.db");

	duckdb_v2_database_ptr db_a = nullptr;
	REQUIRE(duckdb_v2_open(env, path.c_str(), nullptr, 0, &db_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_database_ptr db_b = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_open(env, path.c_str(), nullptr, 0, &db_b, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(db_b == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err, nullptr);

	duckdb_v2_close(&db_a, nullptr);

	// After close, reopen succeeds (the path slot is freed).
	REQUIRE(duckdb_v2_open(env, path.c_str(), nullptr, 0, &db_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_close(&db_b, nullptr);

	duckdb_v2_destroy_environment(&env, nullptr);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: connect / disconnect", "[capi_v2][conn]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_ptr conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);
	REQUIRE(duckdb_v2_disconnect(&conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);

	duckdb_v2_close(&db, nullptr);
	duckdb_v2_destroy_environment(&env, nullptr);
}

TEST_CASE("V2: null-arg validation on env / db / conn entrypoints", "[capi_v2][env][db][conn]") {
	SECTION("create_environment rejects null out_env") {
		REQUIRE(duckdb_v2_create_environment(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("destroy_environment with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_destroy_environment(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("open rejects null env") {
		duckdb_v2_database_ptr db = nullptr;
		REQUIRE(duckdb_v2_open(nullptr, nullptr, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("open rejects null out_db") {
		duckdb_v2_environment_ptr env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		REQUIRE(duckdb_v2_open(env, nullptr, nullptr, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_destroy_environment(&env, nullptr);
	}
	SECTION("open rejects option_count > 0 with null options") {
		duckdb_v2_environment_ptr env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_database_ptr db = nullptr;
		REQUIRE(duckdb_v2_open(env, nullptr, nullptr, 1, &db, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_destroy_environment(&env, nullptr);
	}
	SECTION("close with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_close(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connect rejects null db") {
		duckdb_v2_connection_ptr conn = nullptr;
		REQUIRE(duckdb_v2_connect(nullptr, &conn, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("disconnect with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_disconnect(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
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
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_connection_ptr conn = nullptr;
	V2Fixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~V2Fixture() {
		duckdb_v2_disconnect(&conn, nullptr);
		duckdb_v2_close(&db, nullptr);
		duckdb_v2_destroy_environment(&env, nullptr);
	}
};

} // namespace

TEST_CASE("V2 db option: set + get round-trip", "[capi_v2][db][option]") {
	V2Fixture fx;

	// Read the default before mutating so we can compare against it.
	duckdb_v2_option_ptr before = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, "memory_limit", &before, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *before_setting = nullptr;
	duckdb_v2_option_get_setting(before, &before_setting, nullptr);
	std::string default_value = before_setting ? before_setting : "";
	duckdb_v2_option_destroy(&before, nullptr);

	duckdb_v2_option_ptr in_opt = nullptr;
	duckdb_v2_option_create("memory_limit", "1GB", &in_opt, nullptr);
	REQUIRE(duckdb_v2_database_option_set(fx.db, in_opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&in_opt, nullptr);

	duckdb_v2_option_ptr after = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, "memory_limit", &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *after_setting = nullptr;
	duckdb_v2_option_get_setting(after, &after_setting, nullptr);
	REQUIRE(after_setting != nullptr);
	REQUIRE(std::string(after_setting) != default_value); // mutation visible
	duckdb_v2_option_destroy(&after, nullptr);
}

TEST_CASE("V2 db option: get populates description and aliases", "[capi_v2][db][option]") {
	V2Fixture fx;

	duckdb_v2_option_ptr opt = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, "memory_limit", &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char *name = nullptr;
	duckdb_v2_option_get_name(opt, &name, nullptr);
	// "memory_limit" is an alias; the canonical name is something else
	// (e.g. "max_memory"). Either way, the alias list should contain
	// "memory_limit".
	idx_t alias_count = 0;
	duckdb_v2_option_get_alias_count(opt, &alias_count, nullptr);
	bool has_memory_limit = false;
	for (idx_t i = 0; i < alias_count; i++) {
		const char *alias = nullptr;
		duckdb_v2_option_get_alias(opt, i, &alias, nullptr);
		if (alias && std::string(alias) == "memory_limit") {
			has_memory_limit = true;
			break;
		}
	}
	REQUIRE(has_memory_limit);

	const char *desc = nullptr;
	duckdb_v2_option_get_description(opt, &desc, nullptr);
	REQUIRE(desc != nullptr);
	REQUIRE(desc[0] != '\0');

	duckdb_v2_option_destroy(&opt, nullptr);
}

TEST_CASE("V2 db option: set rejects LOCAL_ONLY at GLOBAL scope", "[capi_v2][db][option]") {
	V2Fixture fx;
	// max_execution_time is declared LOCAL_ONLY (generic option).
	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("max_execution_time", "5000", &opt, nullptr);
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_database_option_set(fx.db, opt, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err, nullptr);
	duckdb_v2_option_destroy(&opt, nullptr);
}

TEST_CASE("V2 db option: get unknown name errors", "[capi_v2][db][option]") {
	V2Fixture fx;
	duckdb_v2_option_ptr out = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, "this_option_does_not_exist", &out, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err, nullptr);
}

TEST_CASE("V2 db option: get_count and get_by_index", "[capi_v2][db][option]") {
	V2Fixture fx;
	idx_t count = 0;
	REQUIRE(duckdb_v2_database_option_get_count(fx.db, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count > 0);

	// Walk the first few entries — each should produce a populated handle.
	idx_t to_check = count < 5 ? count : 5;
	for (idx_t i = 0; i < to_check; i++) {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, i, &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		const char *name = nullptr;
		duckdb_v2_option_get_name(opt, &name, nullptr);
		REQUIRE(name != nullptr);
		REQUIRE(name[0] != '\0');
		duckdb_v2_option_destroy(&opt, nullptr);
	}

	duckdb_v2_option_ptr out_of_range = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, count + 100, &out_of_range, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_destroy(&err, nullptr);
}

TEST_CASE("V2 conn option: set LOCAL is invisible to other connections", "[capi_v2][conn][option]") {
	V2Fixture fx;
	duckdb_v2_connection_ptr other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	// max_execution_time is LOCAL_ONLY — perfect for this test.
	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("max_execution_time", "5000", &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_LOCAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt, nullptr);

	duckdb_v2_option_ptr on_fx = nullptr;
	duckdb_v2_connection_option_get(fx.conn, "max_execution_time", &on_fx, nullptr);
	const char *fx_setting = nullptr;
	duckdb_v2_option_get_setting(on_fx, &fx_setting, nullptr);
	REQUIRE(std::string(fx_setting) == "5000");
	duckdb_v2_option_destroy(&on_fx, nullptr);

	duckdb_v2_option_ptr on_other = nullptr;
	duckdb_v2_connection_option_get(other, "max_execution_time", &on_other, nullptr);
	const char *other_setting = nullptr;
	duckdb_v2_option_get_setting(on_other, &other_setting, nullptr);
	// The other connection sees the static default ("0"), not "5000".
	REQUIRE(std::string(other_setting) != "5000");
	duckdb_v2_option_destroy(&on_other, nullptr);

	duckdb_v2_disconnect(&other, nullptr);
}

TEST_CASE("V2 conn option: set GLOBAL is visible everywhere", "[capi_v2][conn][option]") {
	V2Fixture fx;
	duckdb_v2_connection_ptr other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("memory_limit", "2GB", &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_GLOBAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt, nullptr);

	std::string fx_setting, other_setting;
	for (auto target : {fx.conn, other}) {
		duckdb_v2_option_ptr seen = nullptr;
		duckdb_v2_connection_option_get(target, "memory_limit", &seen, nullptr);
		const char *setting = nullptr;
		duckdb_v2_option_get_setting(seen, &setting, nullptr);
		(target == fx.conn ? fx_setting : other_setting) = setting ? setting : "";
		duckdb_v2_option_destroy(&seen, nullptr);
	}
	REQUIRE(!fx_setting.empty());
	REQUIRE(fx_setting == other_setting); // GLOBAL write seen identically by both

	duckdb_v2_disconnect(&other, nullptr);
}

TEST_CASE("V2 conn option: scope enforcement matches SQL", "[capi_v2][conn][option]") {
	V2Fixture fx;

	// LOCAL_ONLY × GLOBAL: rejected.
	duckdb_v2_option_ptr local_only = nullptr;
	duckdb_v2_option_create("max_execution_time", "5000", &local_only, nullptr);
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, local_only, DUCKDB_V2_SETTING_SCOPE_GLOBAL, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err, nullptr);
	duckdb_v2_option_destroy(&local_only, nullptr);

	// GLOBAL_ONLY × LOCAL: rejected. allow_community_extensions is GLOBAL_ONLY.
	duckdb_v2_option_ptr global_only = nullptr;
	duckdb_v2_option_create("allow_community_extensions", "false", &global_only, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, global_only, DUCKDB_V2_SETTING_SCOPE_LOCAL, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_destroy(&err, nullptr);
	duckdb_v2_option_destroy(&global_only, nullptr);
}

TEST_CASE("V2 conn option: AUTOMATIC scope mirrors bare SQL `SET`", "[capi_v2][conn][option]") {
	V2Fixture fx;
	// max_execution_time is LOCAL_ONLY → AUTOMATIC resolves to SESSION
	// → write succeeds.
	duckdb_v2_option_ptr local = nullptr;
	duckdb_v2_option_create("max_execution_time", "5000", &local, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, local, DUCKDB_V2_SETTING_SCOPE_AUTOMATIC, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&local, nullptr);
}

TEST_CASE("V2 db/conn option: open with options applies them at GLOBAL scope", "[capi_v2][db][option]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_ptr o1 = nullptr;
	duckdb_v2_option_create("memory_limit", "2GB", &o1, nullptr);
	duckdb_v2_option_ptr opts[] = {o1};

	duckdb_v2_database_ptr db = nullptr;
	REQUIRE(duckdb_v2_open(env, nullptr, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_option_ptr seen = nullptr;
	duckdb_v2_database_option_get(db, "memory_limit", &seen, nullptr);
	const char *setting = nullptr;
	duckdb_v2_option_get_setting(seen, &setting, nullptr);
	REQUIRE(setting != nullptr);
	REQUIRE(std::string(setting).length() > 0);
	// Compare against the un-set baseline by opening a second db.
	duckdb_v2_database_ptr db_default = nullptr;
	duckdb_v2_open(env, nullptr, nullptr, 0, &db_default, nullptr);
	duckdb_v2_option_ptr def_opt = nullptr;
	duckdb_v2_database_option_get(db_default, "memory_limit", &def_opt, nullptr);
	const char *def_setting = nullptr;
	duckdb_v2_option_get_setting(def_opt, &def_setting, nullptr);
	REQUIRE(std::string(setting) != std::string(def_setting ? def_setting : ""));
	duckdb_v2_option_destroy(&def_opt, nullptr);
	duckdb_v2_close(&db_default, nullptr);
	duckdb_v2_option_destroy(&seen, nullptr);

	duckdb_v2_option_destroy(&o1, nullptr);
	duckdb_v2_close(&db, nullptr);
	duckdb_v2_destroy_environment(&env, nullptr);
}

// ---------------------------------------------------------------------------
// Internal helpers that bridge implementations use to honor the error-info
// contract:
//
//   - The return value always carries the error code (authoritative).
//   - If `err` is non-null, `SetErrorInfo` allocates a fresh info on failure
//     (destroying any previous one first) and `ClearErrorInfo` frees any
//     previous info on success, leaving *err == nullptr.
//   - Both helpers are safe to call with err == nullptr.
// ---------------------------------------------------------------------------

TEST_CASE("V2 error: SetErrorInfo / ClearErrorInfo helpers", "[capi_v2][error]") {
	SECTION("SetErrorInfo allocates an info and returns the code") {
		duckdb_v2_error_info_ptr err = nullptr;
		auto rc = duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, "bad input");
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);

		const char *msg = nullptr;
		REQUIRE(duckdb_v2_error_info_get_message(err, &msg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(msg != nullptr);
		REQUIRE(std::string(msg) == "bad input");

		duckdb_v2_error_info_destroy(&err, nullptr);
		REQUIRE(err == nullptr);
	}

	SECTION("SetErrorInfo with null message produces an empty message") {
		duckdb_v2_error_info_ptr err = nullptr;
		auto rc = duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, nullptr);
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);

		const char *msg = nullptr;
		REQUIRE(duckdb_v2_error_info_get_message(err, &msg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(msg != nullptr);
		REQUIRE(msg[0] == '\0');

		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	SECTION("SetErrorInfo preserves arbitrarily long messages") {
		duckdb_v2_error_info_ptr err = nullptr;
		std::string long_msg(4096, 'x');
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, long_msg.c_str());

		const char *msg = nullptr;
		duckdb_v2_error_info_get_message(err, &msg, nullptr);
		REQUIRE(std::strlen(msg) == long_msg.size());

		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	SECTION("SetErrorInfo with nullptr err returns the code and allocates nothing") {
		auto rc = duckdb::SetErrorInfo(nullptr, DUCKDB_V2_ERROR_INVALID_INPUT, "ignored");
		REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	SECTION("SetErrorInfo replaces a pre-existing info's message") {
		duckdb_v2_error_info_ptr err = nullptr;
		duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, "first");
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, "second");
		REQUIRE(err != nullptr);

		const char *msg = nullptr;
		duckdb_v2_error_info_get_message(err, &msg, nullptr);
		REQUIRE(std::string(msg) == "second");

		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	SECTION("ClearErrorInfo frees a pre-existing info and returns NONE") {
		duckdb_v2_error_info_ptr err = nullptr;
		duckdb::SetErrorInfo(&err, DUCKDB_V2_ERROR_INVALID_INPUT, "stale");
		REQUIRE(err != nullptr);

		auto rc = duckdb::ClearErrorInfo(&err);
		REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(err == nullptr);
	}

	SECTION("ClearErrorInfo with nullptr err returns NONE and does not crash") {
		auto rc = duckdb::ClearErrorInfo(nullptr);
		REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 error: destroy_error_info is null-safe", "[capi_v2][error]") {
	SECTION("destroying a null handle is a no-op") {
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_error_info_destroy(&err, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(err == nullptr);
	}

	SECTION("destroying via a null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_error_info_destroy(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("detach + destroy preserves info independently of the original slot") {
		duckdb_v2_error_info_ptr err = nullptr;
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, "boom");

		// Transfer ownership out of `err` — the original slot is now detached.
		duckdb_v2_error_info_ptr saved = err;
		err = nullptr;

		const char *msg = nullptr;
		REQUIRE(duckdb_v2_error_info_get_message(saved, &msg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(msg) == "boom");
		duckdb_v2_error_info_destroy(&saved, nullptr);
		REQUIRE(saved == nullptr);
	}
}

// ---------------------------------------------------------------------------
// duckdb_v2_option: lifecycle + accessors. The option handle is a pure data
// container in this round — database/connection get paths land in a follow-up.
// Until then, options carry only the (name, setting) pair the user supplied;
// description, default_setting, target_scope, and aliases stay empty/UNKNOWN.
// ---------------------------------------------------------------------------

TEST_CASE("V2 option: create / destroy", "[capi_v2][option]") {
	SECTION("create succeeds and destroy nulls the slot") {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_option_create("memory_limit", "1GB", &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt != nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt == nullptr);
	}

	SECTION("create with empty name and setting succeeds (strings are just copied)") {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_option_create("", "", &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		const char *name = nullptr;
		const char *setting = nullptr;
		duckdb_v2_option_get_name(opt, &name, nullptr);
		duckdb_v2_option_get_setting(opt, &setting, nullptr);
		REQUIRE(name != nullptr);
		REQUIRE(setting != nullptr);
		REQUIRE(name[0] == '\0');
		REQUIRE(setting[0] == '\0');
		duckdb_v2_option_destroy(&opt, nullptr);
	}

	SECTION("create rejects null name") {
		duckdb_v2_option_ptr opt = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_option_create(nullptr, "x", &opt, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(opt == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	SECTION("create rejects null setting") {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_option_create("x", nullptr, &opt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(opt == nullptr);
	}

	SECTION("create rejects null out_option") {
		REQUIRE(duckdb_v2_option_create("x", "y", nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	SECTION("destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_option_destroy(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("destroy on already-null slot is a no-op") {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_option_destroy(&opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("double destroy is safe (slot was nulled by first destroy)") {
		duckdb_v2_option_ptr opt = nullptr;
		duckdb_v2_option_create("memory_limit", "1GB", &opt, nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_option_destroy(&opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 option: accessors round-trip user-supplied values", "[capi_v2][option]") {
	duckdb_v2_option_ptr opt = nullptr;
	REQUIRE(duckdb_v2_option_create("memory_limit", "2GB", &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("get_name returns the user-supplied name") {
		const char *name = nullptr;
		REQUIRE(duckdb_v2_option_get_name(opt, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(name) == "memory_limit");
	}

	SECTION("get_setting returns the user-supplied setting") {
		const char *setting = nullptr;
		REQUIRE(duckdb_v2_option_get_setting(opt, &setting, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(setting) == "2GB");
	}

	SECTION("get_default_setting returns empty string until populated by a get") {
		const char *def = nullptr;
		REQUIRE(duckdb_v2_option_get_default_setting(opt, &def, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(def != nullptr);
		REQUIRE(def[0] == '\0');
	}

	SECTION("get_description returns empty string until populated by a get") {
		const char *desc = nullptr;
		REQUIRE(duckdb_v2_option_get_description(opt, &desc, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(desc != nullptr);
		REQUIRE(desc[0] == '\0');
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
		const char *alias = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 0, &alias, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(alias == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	duckdb_v2_option_destroy(&opt, nullptr);
}

TEST_CASE("V2 option: accessor null-arg validation", "[capi_v2][option]") {
	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("k", "v", &opt, nullptr);

	SECTION("get_name rejects null option") {
		const char *out = nullptr;
		REQUIRE(duckdb_v2_option_get_name(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_name rejects null out_name") {
		REQUIRE(duckdb_v2_option_get_name(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_setting rejects null option") {
		const char *out = nullptr;
		REQUIRE(duckdb_v2_option_get_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_setting rejects null out_setting") {
		REQUIRE(duckdb_v2_option_get_setting(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_default_setting rejects null option") {
		const char *out = nullptr;
		REQUIRE(duckdb_v2_option_get_default_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_description rejects null option") {
		const char *out = nullptr;
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
		const char *out = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(nullptr, 0, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}

	duckdb_v2_option_destroy(&opt, nullptr);
}

TEST_CASE("V2 option: handles are independent", "[capi_v2][option]") {
	// Two options created back-to-back must not alias each other's storage,
	// and destroying one must not affect the other.
	duckdb_v2_option_ptr a = nullptr;
	duckdb_v2_option_ptr b = nullptr;
	duckdb_v2_option_create("memory_limit", "1GB", &a, nullptr);
	duckdb_v2_option_create("threads", "4", &b, nullptr);

	const char *name_a = nullptr;
	const char *name_b = nullptr;
	duckdb_v2_option_get_name(a, &name_a, nullptr);
	duckdb_v2_option_get_name(b, &name_b, nullptr);
	REQUIRE(std::string(name_a) == "memory_limit");
	REQUIRE(std::string(name_b) == "threads");

	duckdb_v2_option_destroy(&a, nullptr);
	REQUIRE(a == nullptr);

	// b's accessors still work after a's destruction.
	const char *still_b = nullptr;
	REQUIRE(duckdb_v2_option_get_name(b, &still_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(still_b) == "threads");

	duckdb_v2_option_destroy(&b, nullptr);
}

TEST_CASE("V2 option: borrowed pointers stay valid until destroy", "[capi_v2][option]") {
	// Per the contract, accessors return borrowed pointers valid until the
	// option is destroyed. Repeated reads must return stable pointers
	// (the strings are owned by the wrapper and don't move on access).
	duckdb_v2_option_ptr opt = nullptr;
	duckdb_v2_option_create("foo", "bar", &opt, nullptr);

	const char *first_name = nullptr;
	const char *second_name = nullptr;
	duckdb_v2_option_get_name(opt, &first_name, nullptr);
	duckdb_v2_option_get_name(opt, &second_name, nullptr);
	REQUIRE(first_name == second_name);
	REQUIRE(std::string(first_name) == "foo");

	duckdb_v2_option_destroy(&opt, nullptr);
}

TEST_CASE("V2 option: error info is populated on failure paths", "[capi_v2][option]") {
	SECTION("create with null name surfaces a descriptive error") {
		duckdb_v2_option_ptr opt = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_option_create(nullptr, "v", &opt, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);
		const char *msg = nullptr;
		duckdb_v2_error_info_get_message(err, &msg, nullptr);
		REQUIRE(std::string(msg).find("duckdb_v2_option_create") != std::string::npos);
		duckdb_v2_error_info_destroy(&err, nullptr);
	}

	SECTION("get_alias out-of-range surfaces a descriptive error") {
		duckdb_v2_option_ptr opt = nullptr;
		duckdb_v2_option_create("k", "v", &opt, nullptr);
		const char *alias = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 5, &alias, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);
		const char *msg = nullptr;
		duckdb_v2_error_info_get_message(err, &msg, nullptr);
		REQUIRE(std::string(msg).find("out of range") != std::string::npos);
		duckdb_v2_error_info_destroy(&err, nullptr);
		duckdb_v2_option_destroy(&opt, nullptr);
	}

	SECTION("err == nullptr is tolerated on every failure path") {
		duckdb_v2_option_ptr opt = nullptr;
		REQUIRE(duckdb_v2_option_create(nullptr, "v", &opt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_option_create("k", "v", &opt, nullptr);
		const char *alias = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 99, &alias, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_option_destroy(&opt, nullptr);
	}
}
