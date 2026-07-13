#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api_helpers.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C++ API tests: environment, database options, filesystem, logging,
// exceptions, replacement scans.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Database GetOption by name and option target scope", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");

	// Database-resolved options carry their declared scope.
	auto option = db.GetOption("allow_community_extensions");
	REQUIRE(option.GetName() == "allow_community_extensions");
	REQUIRE(option.GetTargetScope() == OptionTargetScope::GlobalOnly);

	// Constructor-built options are unresolved: scope reports Unknown.
	DatabaseOption fresh("memory_limit", "1GB");
	REQUIRE(fresh.GetTargetScope() == OptionTargetScope::Unknown);

	// An alias resolves to its canonical option.
	REQUIRE(db.GetOption("memory_limit").GetName() == "max_memory");

	REQUIRE_THROWS_MATCHES(db.GetOption("no_such_option"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: LibraryVersion reports the engine version", "[cpp_api]") {
	const auto version = duckdb_api::LibraryVersion();
	REQUIRE_FALSE(version.empty());

	// The engine agrees; two calls return equal owned copies.
	char *raw = nullptr;
	REQUIRE(duckdb_v2_library_version(&raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(version == raw);
	free(raw);
}
TEST_CASE("Stable C++API: File System", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Get file system handle
		const auto fs = ctx.GetFileSystem();

		const auto test_path = duckdb::TestCreatePath("test_file_system.txt");

		// Write a file
		{
			auto handle = fs.OpenFile(test_path, FileFlags::WRITE | FileFlags::FILE_CREATE);

			// Write some data to the file
			const std::string data = "Hello, DuckDB!";

			REQUIRE(handle.Write(data.data(), (int64_t)data.size()) == (int64_t)data.size());
		}

		// Now read it back
		{
			auto handle = fs.OpenFile(test_path, FileFlags::READ);

			char buffer[64] = {0};
			int64_t bytes_read = handle.Read(buffer, sizeof(buffer));

			REQUIRE(bytes_read == 14);
			REQUIRE(std::string(buffer, bytes_read) == "Hello, DuckDB!");
		}
	});
}
TEST_CASE("Stable C++API: Logging", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.Execute("CALL enable_logging(storage = 'memory')").Drain();

	conn.Log(LogLevel::LOG_INFO, "This is an informational message from a connection");

	conn.WithTransaction([](const Context &ctx) {
		ctx.Log(LogLevel::LOG_INFO, "This is an informational message.");
		ctx.Log(LogLevel::LOG_WARN, "This is a warning message.");
		ctx.Log(LogLevel::LOG_ERROR, "This is an error message.");
	});

	auto res =
	    conn.Execute("SELECT case when log_level = 'INFO' then 1 when log_level = 'WARNING' then 2 when log_level "
	                 "= 'ERROR' then 3 else -1 end as level FROM duckdb_logs");
	auto chunk = res.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	auto data = view.Data<int32_t>();

	REQUIRE(data[view.SelAt(0)] == 1);
	REQUIRE(data[view.SelAt(1)] == 1);
	REQUIRE(data[view.SelAt(2)] == 2);
	REQUIRE(data[view.SelAt(3)] == 3);
}
namespace {

// Sink that the custom log storage callback writes captured entries into.
// A pointer to an instance is handed to the storage as user data, so the test
// can inspect what the callback observed after logging.
struct LogSink {
	std::vector<std::string> messages;
	std::vector<std::string> types;
	std::vector<duckdb_api::LogLevel> levels;
	std::vector<int64_t> timestamps;
};

} // namespace
TEST_CASE("Stable C++API: Log Storage", "[cpp_api]") {
	using namespace duckdb_api;

	// Declared before the database so it outlives the registered storage (the
	// storage, and the user data pointing here, are torn down with the db).
	LogSink sink;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// A Context is required to build and register a log storage, so do it inside
	// a transaction.
	conn.WithTransaction([&](const Context &ctx) {
		LogStorage storage(ctx);
		storage.SetName("cpp_custom_storage");

		// The callback must be a plain function pointer (no captures), so it
		// communicates with the test exclusively through the user data.
		storage.SetUserData<LogSink *>(&sink);
		storage.SetLogCallback([](LogStorage::LogEntry &entry) {
			auto &captured = *entry.GetUserData<LogSink *>();

			// Only capture entries emitted by this test; ignore the QueryLog
			// entries (the SQL text) that logging also produces.
			if (std::strcmp(entry.GetLogType(), "cpp_api_test") != 0) {
				return;
			}

			captured.messages.emplace_back(entry.GetLogMessage());
			captured.types.emplace_back(entry.GetLogType());
			captured.levels.push_back(entry.GetLogLevel());
			captured.timestamps.push_back(entry.GetLogTimestamp());
		});

		storage.Register(ctx);
	});

	// Activate logging and route it to the storage we just registered.
	conn.Execute("SET enable_logging = true;").Drain();
	conn.Execute("SET logging_storage = 'cpp_custom_storage';").Drain();

	// Emit a couple of log entries with a known type/level/message.
	conn.Execute("SELECT write_log('first message', log_type := 'cpp_api_test', level := 'WARNING');").Drain();
	conn.Execute("SELECT write_log('second message', log_type := 'cpp_api_test', level := 'ERROR');").Drain();

	// The callback should have captured exactly our two entries, in order.
	REQUIRE(sink.messages.size() == 2);

	REQUIRE(sink.messages[0] == "first message");
	REQUIRE(sink.types[0] == "cpp_api_test");
	REQUIRE(sink.levels[0] == LogLevel::LOG_WARN);

	REQUIRE(sink.messages[1] == "second message");
	REQUIRE(sink.types[1] == "cpp_api_test");
	REQUIRE(sink.levels[1] == LogLevel::LOG_ERROR);

	// Registering a second storage under the same name should fail.
	conn.WithTransaction([&](const Context &ctx) {
		LogStorage storage(ctx);
		storage.SetName("cpp_custom_storage");
		storage.SetLogCallback([](LogStorage::LogEntry &) {});
		REQUIRE_THROWS_AS(storage.Register(ctx), Exception);
	});
}
TEST_CASE("Stable C++API: Exception carries the code and message body", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Binder error: GetCode() is the identity, GetRawMessage() the unprefixed body.
	try {
		conn.Execute("SELECT * FROM no_such_table");
		FAIL("expected a Catalog error");
	} catch (const Exception &ex) {
		REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_DATABASE_CATALOG);
		REQUIRE(std::string(ex.GetRawMessage()).find("no_such_table") != std::string::npos);
		REQUIRE(std::string(ex.GetRawMessage()).rfind("Catalog Error:", 0) != 0);
		// what() is the full prefixed message and contains the body.
		REQUIRE(std::string(ex.what()).rfind("Catalog Error:", 0) == 0);
		REQUIRE(std::string(ex.what()).find(ex.GetRawMessage()) != std::string::npos);
	}

	// Parse error via ParseSQL has the same shape: Parser code, unprefixed body.
	try {
		conn.ParseSQL("SELECT 1; SELEKT 2");
		FAIL("expected a Parser error");
	} catch (const Exception &ex) {
		REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_QUERY_PARSER);
		REQUIRE(std::string(ex.GetRawMessage()).rfind("Parser Error:", 0) != 0);
	}
}
TEST_CASE("Stable C++API: Replacement Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Sums the row counts of every chunk a result produces.
	auto count_rows = [](QueryResult result) -> idx_t {
		idx_t total = 0;
		while (auto chunk = result.FetchChunk()) {
			total += chunk.GetRowCount();
		}
		return total;
	};

	SECTION("claim a builtin table function") {
		db.AddReplacementScan([](Database::ReplacementScanInput &input) {
			input.SetFunctionName("range");
			input.AddParameter(Value::Bigint(5));
		});
		auto result = conn.Execute("SELECT * FROM cpp_repl_no_such_table");
		REQUIRE(result.GetSchema().GetFieldCount() == 1);
		auto chunk = result.FetchChunk();
		REQUIRE(chunk);
		REQUIRE(chunk.GetRowCount() == 5);
		// range(5) yields the BIGINT sequence 0..4 in order.
		auto view = chunk.GetVector(0).GetView();
		auto data = view.Data<int64_t>();
		for (idx_t i = 0; i < 5; i++) {
			REQUIRE(view.IsValid(i));
			REQUIRE(data[view.SelAt(i)] == static_cast<int64_t>(i));
		}
	}

	SECTION("decline falls through to the catalog error") {
		db.AddReplacementScan([](Database::ReplacementScanInput &input) {
			// Inspect the name but do not claim it.
			REQUIRE(input.GetTableName() == "cpp_repl_no_such_table");
			REQUIRE(input.GetCatalogName().empty());
			REQUIRE(input.GetSchemaName().empty());
		});
		REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_repl_no_such_table"), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
	}

	SECTION("callback failure: a mapped code round-trips exactly") {
		db.AddReplacementScan([](Database::ReplacementScanInput &input) {
			throw Exception(DUCKDB_V2_ERROR_IO_GENERAL, "cpp replacement scan failure");
		});
		REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_repl_no_such_table"), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_IO_GENERAL));
	}

	SECTION("callback failure: a generic exception becomes a binder error") {
		db.AddReplacementScan([](Database::ReplacementScanInput &input) { throw std::runtime_error("plain failure"); });
		REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_repl_no_such_table"), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_QUERY_BINDER));
	}

	SECTION("user data and a named parameter, end to end via read_csv") {
		const auto csv_path = duckdb::TestCreatePath("cpp_api_replacement_scan.csv");
		{
			std::ofstream out(csv_path);
			out << "100,apple\n200,banana\n300,cherry\n";
		}

		// User data carries the CSV path; claim read_csv(path, sep => ',').
		db.AddReplacementScan<std::string>(
		    [](Database::ReplacementScanInput &input) {
			    const auto &path = input.GetUserData<std::string>();
			    input.SetFunctionName("read_csv");
			    input.AddParameter(Value::Varchar(path));
			    input.AddNamedParameter("sep", Value::Varchar(","));
		    },
		    csv_path);

		auto result = conn.Execute("SELECT * FROM cpp_repl_csv_placeholder");
		// `sep => ','` split each line into two columns; a wrong separator would
		// collapse them into one.
		REQUIRE(result.GetSchema().GetFieldCount() == 2);
		REQUIRE(count_rows(std::move(result)) == 3);
	}

	SECTION("the callback reaches the filesystem through the context") {
		const auto csv_path = duckdb::TestCreatePath("cpp_api_replacement_scan_ctx.csv");
		{
			std::ofstream out(csv_path);
			out << "7,seven\n8,eight\n";
		}

		// Probe the file through the binding context's filesystem before claiming.
		db.AddReplacementScan<std::string>(
		    [](Database::ReplacementScanInput &input) {
			    const auto &path = input.GetUserData<std::string>();
			    auto fs = input.GetContext().GetFileSystem();
			    auto file = fs.OpenFile(path, FileFlags::READ);
			    char head[1] = {0};
			    REQUIRE(file.Read(head, 1) == 1);
			    input.SetFunctionName("read_csv");
			    input.AddParameter(Value::Varchar(path));
			    input.AddNamedParameter("sep", Value::Varchar(","));
		    },
		    csv_path);

		auto result = conn.Execute("SELECT * FROM cpp_repl_ctx_placeholder");
		REQUIRE(result.GetSchema().GetFieldCount() == 2);
		REQUIRE(count_rows(std::move(result)) == 2);
	}
}
TEST_CASE("Stable C++API: Connection::SetOption scope split is visible correctly across connections", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn_a = db.Connect();
	auto conn_b = db.Connect();

	// max_execution_time is LOCAL_DEFAULT: a LOCAL write on conn_a stays
	// invisible to conn_b.
	conn_a.SetOption(DatabaseOption("max_execution_time", "5000"), SettingScope::Local);
	REQUIRE(conn_a.GetOption("max_execution_time").GetValue() == "5000");
	REQUIRE(conn_b.GetOption("max_execution_time").GetValue() != "5000");

	// A GLOBAL write on conn_a is visible identically on conn_b.
	conn_a.SetOption(DatabaseOption("memory_limit", "987MB"), SettingScope::Global);
	auto seen_a = conn_a.GetOption("memory_limit").GetValue();
	auto seen_b = conn_b.GetOption("memory_limit").GetValue();
	REQUIRE_FALSE(seen_a.empty());
	REQUIRE(std::string(seen_a) == std::string(seen_b));

	// A GLOBAL_ONLY option rejects a LOCAL scope.
	REQUIRE_THROWS_MATCHES(conn_a.SetOption(DatabaseOption("allow_community_extensions", "false"), SettingScope::Local),
	                       Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: Connection::GetOption by name and the scopeless SetOption default", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto option = conn.GetOption("allow_community_extensions");
	REQUIRE(option.GetName() == "allow_community_extensions");

	// The scopeless overload uses Automatic scope (SQL `SET` semantics).
	conn.SetOption(DatabaseOption("max_execution_time", "4242"));
	REQUIRE(conn.GetOption("max_execution_time").GetValue() == "4242");

	REQUIRE_THROWS_MATCHES(conn.GetOption("no_such_option_xyz"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: Environment::Open with pre-open options enforces read-only", "[cpp_api]") {
	using namespace duckdb_api;

	auto path = duckdb::TestCreatePath("cpp_api_readonly.duckdb");
	duckdb::DeleteDatabase(path);

	Environment env;
	{
		// Seed the database, then close (scope exit) to free the exclusive-open
		// slot for the read-only reopen.
		auto db = env.Open(path);
		auto conn = db.Connect();
		conn.Execute("CREATE TABLE t(i INTEGER)").Drain();
		conn.Execute("INSERT INTO t VALUES (1), (2)").Drain();
	}

	{
		std::vector<DatabaseOption> options;
		options.push_back(DatabaseOption("access_mode", "READ_ONLY"));
		auto ro_db = env.Open(path, options);
		auto ro_conn = ro_db.Connect();

		// Reads see the seeded data. Scoped so the live result is released
		// before the write attempts below.
		{
			auto result = ro_conn.Execute("SELECT count(*) FROM t");
			auto chunk = result.FetchChunk();
			REQUIRE(chunk.GetVector(0).GetValue(0).AsBigint() == 2);
		}

		// Writes are rejected: both DML and DDL.
		REQUIRE_THROWS_AS(ro_conn.Execute("INSERT INTO t VALUES (3)"), Exception);
		REQUIRE_THROWS_AS(ro_conn.Execute("CREATE TABLE u(i INTEGER)"), Exception);

		// The data is unchanged after the rejected write attempts.
		auto after = ro_conn.Execute("SELECT count(*) FROM t");
		REQUIRE(after.FetchChunk().GetVector(0).GetValue(0).AsBigint() == 2);
	}

	duckdb::DeleteDatabase(path);
}
TEST_CASE("Stable C++API: typed exceptions carry their error code", "[cpp_api]") {
	using namespace duckdb_api;

	// Each typed exception fixes its code in the implementation; throwing one
	// is how a callback names its error class without any code vocabulary.
	REQUIRE(InvalidInputException("boom").GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE(InterruptException("stop").GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT));

	// They are catchable through the Exception base, preserving the code.
	try {
		throw InvalidInputException("bad arg");
	} catch (const Exception &caught) {
		REQUIRE(caught.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_INVALID_INPUT));
	}

	// The base Exception with a raw code still works.
	Exception raw(static_cast<uint32_t>(DUCKDB_V2_ERROR_QUERY_BINDER), "parse boom");
	REQUIRE(raw.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_QUERY_BINDER));

	// A thrown-and-caught engine error classifies back correctly end to end.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	try {
		conn.Execute("SELECT * FROM no_such_table_xyz");
		FAIL("expected a Catalog error");
	} catch (const Exception &caught) {
		REQUIRE(caught.GetCode() == static_cast<uint32_t>(DUCKDB_V2_ERROR_DATABASE_CATALOG));
	}
}
