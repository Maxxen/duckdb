#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include <cstring>

TEST_CASE("V2: smoke test - open and close database", "[capi_v2]") {
	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	auto rc = duckdb_v2_open(nullptr, &db, &err);
	// Stubs return DUCKDB_V2_API_ERROR (not yet implemented)
	REQUIRE(rc == DUCKDB_V2_API_ERROR);
	// Stubs do not allocate an info; nothing to destroy.
	REQUIRE(err == nullptr);
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

		duckdb_v2_destroy_error_info(&err, nullptr);
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

		duckdb_v2_destroy_error_info(&err, nullptr);
	}

	SECTION("SetErrorInfo preserves arbitrarily long messages") {
		duckdb_v2_error_info_ptr err = nullptr;
		std::string long_msg(4096, 'x');
		duckdb::SetErrorInfo(&err, DUCKDB_V2_API_ERROR, long_msg.c_str());

		const char *msg = nullptr;
		duckdb_v2_error_info_get_message(err, &msg, nullptr);
		REQUIRE(std::strlen(msg) == long_msg.size());

		duckdb_v2_destroy_error_info(&err, nullptr);
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

		duckdb_v2_destroy_error_info(&err, nullptr);
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
		REQUIRE(duckdb_v2_destroy_error_info(&err, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(err == nullptr);
	}

	SECTION("destroying via a null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_destroy_error_info(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
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
		duckdb_v2_destroy_error_info(&saved, nullptr);
		REQUIRE(saved == nullptr);
	}
}
