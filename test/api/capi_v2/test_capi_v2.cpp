#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include <cstring>

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
