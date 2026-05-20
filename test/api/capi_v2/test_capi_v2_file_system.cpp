#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "test_helpers.hpp"

#include <cstdio>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 file system tests: handle retrieval and the open/read/write/seek/tell/
// size/sync/close lifecycle of duckdb_v2_file_handle. File-open behavior is
// controlled by a `uint64_t` bitset of `DUCKDB_V2_FILE_FLAG_*` values passed
// directly to `duckdb_v2_file_system_open`.
// ---------------------------------------------------------------------------

namespace {

// RAII helper to keep env+db+conn setup out of every test body.
struct FsFixture {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_connection_ptr conn = nullptr;
	FsFixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~FsFixture() {
		duckdb_v2_disconnect(&conn, nullptr);
		duckdb_v2_close(&db, nullptr);
		duckdb_v2_destroy_environment(&env, nullptr);
	}
};

} // namespace

// ---------------------------------------------------------------------------
// File system handle retrieval
// ---------------------------------------------------------------------------

TEST_CASE("V2 fs: get_from_connection returns a non-null handle", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	REQUIRE(duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(fs != nullptr);
}

TEST_CASE("V2 fs: get_from_context returns a non-null handle", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr captured = nullptr;
	duckdb_v2_connection_execute_with_context(
	    fx.conn,
	    [](duckdb_v2_context_ptr ctx, void *ud, duckdb_v2_error_info_ptr *err) {
		    auto &out = *static_cast<duckdb_v2_file_system_ptr *>(ud);
		    REQUIRE(duckdb_v2_file_system_get_from_context(ctx, &out, err) == DUCKDB_V2_ERROR_NONE);
	    },
	    &captured, nullptr);
	REQUIRE(captured != nullptr);
}

TEST_CASE("V2 fs: get_from_* null-arg validation", "[capi_v2][file_system]") {
	FsFixture fx;
	SECTION("get_from_connection rejects null out_file_system") {
		REQUIRE(duckdb_v2_file_system_get_from_connection(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("get_from_connection rejects null connection") {
		duckdb_v2_file_system_ptr fs = nullptr;
		REQUIRE(duckdb_v2_file_system_get_from_connection(nullptr, &fs, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(fs == nullptr);
	}
	SECTION("get_from_context rejects null context") {
		duckdb_v2_file_system_ptr fs = nullptr;
		REQUIRE(duckdb_v2_file_system_get_from_context(nullptr, &fs, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(fs == nullptr);
	}
}

// ---------------------------------------------------------------------------
// File handle: end-to-end write/read round-trip
// ---------------------------------------------------------------------------

TEST_CASE("V2 file_handle: write then read round-trip", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);
	REQUIRE(fs != nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_roundtrip.bin");
	const std::string payload = "duckdb v2 file system round-trip";

	// --- Write phase ---
	{
		const uint64_t flags = DUCKDB_V2_FILE_FLAG_WRITE | DUCKDB_V2_FILE_FLAG_CREATE;
		duckdb_v2_file_handle_ptr h = nullptr;
		REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), flags, &h, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(h != nullptr);

		int64_t written = 0;
		REQUIRE(duckdb_v2_file_handle_write(h, (void *)payload.data(), (int64_t)payload.size(), &written, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(written == (int64_t)payload.size());

		REQUIRE(duckdb_v2_file_handle_sync(h, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_file_handle_close(h, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_file_handle_destroy(&h) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(h == nullptr);
	}

	// --- Read phase ---
	{
		duckdb_v2_file_handle_ptr h = nullptr;
		REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), DUCKDB_V2_FILE_FLAG_READ, &h, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);

		int64_t size = -1;
		REQUIRE(duckdb_v2_file_handle_size(h, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(size == (int64_t)payload.size());

		std::vector<char> buf(payload.size(), '\0');
		int64_t read = 0;
		REQUIRE(duckdb_v2_file_handle_read(h, buf.data(), (int64_t)buf.size(), &read, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(read == (int64_t)payload.size());
		REQUIRE(std::string(buf.begin(), buf.end()) == payload);

		duckdb_v2_file_handle_destroy(&h);
	}
}

TEST_CASE("V2 file_handle: seek / tell / read-after-seek", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_seek.bin");
	const std::string payload = "0123456789";

	// Write a known payload.
	{
		duckdb_v2_file_handle_ptr h = nullptr;
		duckdb_v2_file_system_open(fs, path.c_str(), DUCKDB_V2_FILE_FLAG_WRITE | DUCKDB_V2_FILE_FLAG_CREATE, &h,
		                           nullptr);
		int64_t written = 0;
		duckdb_v2_file_handle_write(h, (void *)payload.data(), (int64_t)payload.size(), &written, nullptr);
		duckdb_v2_file_handle_destroy(&h);
	}

	duckdb_v2_file_handle_ptr h = nullptr;
	REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), DUCKDB_V2_FILE_FLAG_READ, &h, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("seek forwards then read returns the suffix") {
		REQUIRE(duckdb_v2_file_handle_seek(h, 4, nullptr) == DUCKDB_V2_ERROR_NONE);
		int64_t pos = 0;
		REQUIRE(duckdb_v2_file_handle_tell(h, &pos, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(pos == 4);

		char buf[6] = {0};
		int64_t read = 0;
		REQUIRE(duckdb_v2_file_handle_read(h, buf, 6, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(read == 6);
		REQUIRE(std::string(buf, 6) == "456789");
	}

	SECTION("seek rejects negative offsets") {
		duckdb_v2_error_info_ptr err = nullptr;
		REQUIRE(duckdb_v2_file_handle_seek(h, -1, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}

	duckdb_v2_file_handle_destroy(&h);
}

TEST_CASE("V2 file_handle: combined flags open in the expected mode", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_combined.bin");

	// READ | WRITE | CREATE opens a fresh file with both capabilities.
	const uint64_t flags = DUCKDB_V2_FILE_FLAG_READ | DUCKDB_V2_FILE_FLAG_WRITE | DUCKDB_V2_FILE_FLAG_CREATE;
	duckdb_v2_file_handle_ptr h = nullptr;
	REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), flags, &h, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char msg[] = "hello";
	int64_t written = 0;
	REQUIRE(duckdb_v2_file_handle_write(h, (void *)msg, 5, &written, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_handle_seek(h, 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	char buf[5] = {0};
	int64_t read = 0;
	REQUIRE(duckdb_v2_file_handle_read(h, buf, 5, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(read == 5);
	REQUIRE(std::string(buf, 5) == "hello");

	duckdb_v2_file_handle_destroy(&h);
}

TEST_CASE("V2 file_handle: opening a non-existent file (without CREATE) fails", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_missing.bin");
	// Ensure the file isn't lying around from a previous run.
	std::remove(path.c_str());

	duckdb_v2_file_handle_ptr h = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), DUCKDB_V2_FILE_FLAG_READ, &h, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(h == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 file_handle: CREATE_NEW fails when the file already exists", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_create_new.bin");

	// First create the file (WRITE | CREATE) — should succeed.
	{
		duckdb_v2_file_handle_ptr h = nullptr;
		REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), DUCKDB_V2_FILE_FLAG_WRITE | DUCKDB_V2_FILE_FLAG_CREATE, &h,
		                                   nullptr) == DUCKDB_V2_ERROR_NONE);

		// Also issue a SYNC so we can be extra sure the file is visible to the next open
		REQUIRE(duckdb_v2_file_handle_sync(h, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_file_handle_destroy(&h);
	}

	// Now CREATE_NEW must refuse — the file is there.
	duckdb_v2_file_handle_ptr h = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(),
	                                   DUCKDB_V2_FILE_FLAG_WRITE | DUCKDB_V2_FILE_FLAG_CREATE |
	                                       DUCKDB_V2_FILE_FLAG_CREATE_NEW,
	                                   &h, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(h == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 file_handle: open rejects an empty flag bitset", "[capi_v2][file_system]") {
	FsFixture fx;
	duckdb_v2_file_system_ptr fs = nullptr;
	duckdb_v2_file_system_get_from_connection(fx.conn, &fs, nullptr);

	auto path = duckdb::TestCreatePath("v2_fs_no_flags.bin");

	duckdb_v2_file_handle_ptr h = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	// 0 == DUCKDB_V2_FILE_FLAG_INVALID; no capabilities is meaningless.
	REQUIRE(duckdb_v2_file_system_open(fs, path.c_str(), 0, &h, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(h == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

// ---------------------------------------------------------------------------
// Null-arg validation for the file_handle entrypoints
// ---------------------------------------------------------------------------

TEST_CASE("V2 file_handle: null-arg validation", "[capi_v2][file_system]") {
	SECTION("file_system_open rejects null file_system") {
		duckdb_v2_file_handle_ptr h = nullptr;
		char path[] = "/tmp/anything";
		REQUIRE(duckdb_v2_file_system_open(nullptr, path, DUCKDB_V2_FILE_FLAG_READ, &h, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("file_system_open rejects null out_file_handle") {
		char path[] = "/tmp/anything";
		REQUIRE(duckdb_v2_file_system_open(nullptr, path, DUCKDB_V2_FILE_FLAG_READ, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("read rejects null file_handle") {
		char buf[1] = {0};
		int64_t bytes_read = 0;
		REQUIRE(duckdb_v2_file_handle_read(nullptr, buf, 1, &bytes_read, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("write rejects null file_handle") {
		char buf[1] = {0};
		int64_t bytes_written = 0;
		REQUIRE(duckdb_v2_file_handle_write(nullptr, buf, 1, &bytes_written, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("tell rejects null file_handle") {
		int64_t pos = 0;
		REQUIRE(duckdb_v2_file_handle_tell(nullptr, &pos, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("size rejects null file_handle") {
		int64_t sz = 0;
		REQUIRE(duckdb_v2_file_handle_size(nullptr, &sz, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("seek rejects null file_handle") {
		REQUIRE(duckdb_v2_file_handle_seek(nullptr, 0, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("sync rejects null file_handle") {
		REQUIRE(duckdb_v2_file_handle_sync(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("close rejects null file_handle") {
		REQUIRE(duckdb_v2_file_handle_close(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	}
	SECTION("destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_file_handle_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("destroy on already-null slot is a no-op") {
		duckdb_v2_file_handle_ptr h = nullptr;
		REQUIRE(duckdb_v2_file_handle_destroy(&h) == DUCKDB_V2_ERROR_NONE);
	}
}
