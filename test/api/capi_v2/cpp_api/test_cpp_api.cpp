#include "catch.hpp"
#include "cpp_api.hpp"
#include "test_helpers.hpp"

#include <cstring>

// ---------------------------------------------------------------------------
// Stable C-API testing
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++-API: Basic", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	REQUIRE(env.GetOpenDatabaseCount() == 0);

	auto db = env.Open(":memory:");

	REQUIRE(env.GetOpenDatabaseCount() == 1);

	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		ScalarFunction function(ctx);

		function.SetName("MyFunction")
		    .AddParameter("a", LogicalType::INTEGER())
		    .AddParameter("b", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetBindCallback([](ScalarFunction::BindInput &input) {
			    // Initialize bind data
			    input.SetBindData<std::string>("foobar");
		    })
		    .SetInitCallback([](ScalarFunction::InitInput &input) {
			    // Set worker state
			    input.SetWorkerState<std::pair<int, int>>(42, 1337);
		    })
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    const auto &bind_data = input.GetBindData<std::string>();
			    auto &[min, max] = input.GetWorkerState<std::pair<int, int>>();

			    REQUIRE(bind_data == "foobar");
			    REQUIRE(min == 42);
			    REQUIRE(max == 1337);

			    auto chunk = input.GetInputChunk();
			    auto lhs_vec = chunk.GetVector(0);
			    auto rhs_vec = chunk.GetVector(1);
			    auto out_vec = input.GetResultVector();

			    auto lhs_data = lhs_vec.GetDataMutable<const int32_t>();
			    auto rhs_data = rhs_vec.GetDataMutable<const int32_t>();
			    auto out_data = out_vec.GetDataMutable<int32_t>();

			    for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
				    out_data[i] = lhs_data[i] + rhs_data[i];
			    }
		    })
		    .Register(ctx);
	});

	const auto result = conn.Query("SELECT MyFunction(1, 2) AS result");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetChunkCount() == 1);

	REQUIRE(result.GetColumnName(0) == "result");

	auto chunk = result.GetChunk(0);

	auto vec = chunk.GetVector(0);
	auto data = vec.GetDataMutable<const int32_t>();

	REQUIRE(data[0] == 3);
}

TEST_CASE("Stable C++API: Aggregate Function", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		AggregateFunction aggregate(ctx);

		aggregate.SetName("MyAggregate")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) { input.Reserve<int32_t>(); })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) { input.Initialize<int32_t>(0); })
		    .SetUpdateCallback([](AggregateFunction::UpdateInput &input) {
			    auto &chunk = input.GetInputChunk();
			    auto vector = chunk.GetVector(0);

			    vector.Flatten();

			    const auto count = input.GetStateCount();
			    const auto array = input.GetStateArray<int32_t>();
			    const auto vdata = vector.GetDataMutable<const int32_t>();

			    for (idx_t i = 0; i < count; i++) {
				    *array[i] += vdata[i];
			    }
		    })
		    .SetCombineCallback([](AggregateFunction::CombineInput &input) {
			    const auto count = input.GetStateCount();

			    const auto source = input.GetSourceStateArray<int32_t>();
			    const auto target = input.GetTargetStateArray<int32_t>();

			    for (idx_t i = 0; i < count; i++) {
				    *target[i] += *source[i];
			    }
		    })
		    .SetFinalizeCallback([](AggregateFunction::FinalizeInput &input) {
			    const auto count = input.GetStateCount();
			    const auto array = input.GetStateArray<int32_t>();
			    const auto offset = input.GetResultOffset();

			    auto &vector = input.GetResultVector();
			    const auto result = vector.GetDataMutable<int32_t>();

			    for (idx_t i = 0; i < count; i++) {
				    result[offset + i] = *array[i] * 2; // Just to make it a bit more interesting than a plain sum
			    }
		    })
		    .Register(ctx);
	});

	const auto result = conn.Query("SELECT MyAggregate(i) AS result FROM (VALUES (1), (2), (3)) AS t(i)");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetChunkCount() == 1);

	REQUIRE(result.GetColumnName(0) == "result");

	auto chunk = result.GetChunk(0);

	auto vec = chunk.GetVector(0);
	auto data = vec.GetDataMutable<const int32_t>();

	REQUIRE(data[0] == 12); // (1 + 2 + 3) * 2
}

TEST_CASE("Stable C++API: Table Function", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction table_function(ctx);

		table_function.SetName("MyRangeFunction")
		    .AddParameter(LogicalType::INTEGER())              // Start
		    .AddParameter(LogicalType::INTEGER())              // Stop
		    .AddNamedParameter("step", LogicalType::INTEGER()) // Optional step, defaults to 1
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // We will emit one column named "i" of type INTEGER
			    input.AddResultColumn("i", LogicalType::INTEGER());

			    // Get parameters
			    const auto start = input.GetParameter(0).AsI32();
			    const auto stop = input.GetParameter(1).AsI32();

			    // "Step" is optional and named, so we try to get it and default to 1 if it's not provided
			    int32_t step = 1;
			    if (const auto step_arg = input.TryGetNamedParameter("step")) {
				    step = step_arg->AsI32();
			    }

			    // Store the parameters in the bind data for use in exec
			    input.SetBindData<std::tuple<int32_t, int32_t, int32_t>>(start, step, stop);
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) {
			    auto &[start, step, stop] = input.GetBindData<std::tuple<int32_t, int32_t, int32_t>>();

			    // Initialize global state
			    input.SetGlobalState<int32_t>(start);
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &[start, step, stop] = input.GetBindData<std::tuple<int32_t, int32_t, int32_t>>();

			    auto &state = input.GetGlobalState<int32_t>();

			    auto &chunk = input.GetResultChunk();
			    auto vec = chunk.GetVector(0);
			    const auto data = vec.GetDataMutable<int32_t>();

			    auto emitted = 0;
			    auto capacity = 2048; // TODO: Get capacity from vector

			    while (state < stop && emitted < capacity) {
				    data[emitted++] = state;
				    state += step;
			    }

			    vec.SetSize(emitted);
		    })
		    .Register(ctx);
	});

	const auto result = conn.Query("SELECT * FROM MyRangeFunction(0, 10, step => 2)");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetChunkCount() == 1);
	REQUIRE(result.GetColumnName(0) == "i");

	auto chunk = result.GetChunk(0);
	auto vec = chunk.GetVector(0);

	auto data = vec.GetDataMutable<const int32_t>();
	REQUIRE(chunk.GetRowCount() == 5);
	REQUIRE(data[0] == 0);
	REQUIRE(data[1] == 2);
	REQUIRE(data[2] == 4);
	REQUIRE(data[3] == 6);
	REQUIRE(data[4] == 8);
}

TEST_CASE("Stable C++-API: File System", "[cpp_api]") {
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
			auto handle = fs.OpenFile(test_path, FileFlags::WRITE | FileFlags::CREATE);

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

TEST_CASE("Stable C++-API: Column Data Single Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Produce 4100 rows of (a, b) where b == a * 10. This spans several internal
	// chunks (> STANDARD_VECTOR_SIZE), so both append and scan iterate multiple times.
	constexpr int64_t ROW_COUNT = 4100;
	auto result = conn.Query("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetColumnCount() == 2);

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT()); // range() produces BIGINT
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);
		REQUIRE(collection.GetRowCount() == 0);

		// Append every chunk of the query result into the collection.
		{
			auto append_state = collection.GetAppendState();
			for (idx_t i = 0; i < result.GetChunkCount(); i++) {
				auto chunk = result.GetChunk(i);
				collection.Append(append_state, chunk);
			}
		}
		REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT));

		// Scan the collection back single-threaded and verify the values round-trip.
		// The scan target is a fresh standalone chunk; Scan sets its cardinality.
		{
			auto scan_state = collection.GetSingleScanState();
			DataChunk out(ctx, types);

			idx_t scanned = 0;
			int64_t sum_a = 0;
			while (collection.Scan(scan_state, out)) {
				const auto rows = out.GetRowCount();
				const auto a = out.GetVector(0).GetDataMutable<const int64_t>();
				const auto b = out.GetVector(1).GetDataMutable<const int64_t>();
				for (idx_t r = 0; r < rows; r++) {
					REQUIRE(b[r] == a[r] * 10);
					sum_a += a[r];
				}
				scanned += rows;
			}

			REQUIRE(scanned == idx_t(ROW_COUNT));
			REQUIRE(sum_a == (ROW_COUNT - 1) * ROW_COUNT / 2); // sum of 0..4099
		}

		// Combine consumes another collection and merges its rows into this one.
		{
			ColumnDataCollection other(ctx, types);
			auto append_state = other.GetAppendState();
			for (idx_t i = 0; i < result.GetChunkCount(); i++) {
				auto chunk = result.GetChunk(i);
				other.Append(append_state, chunk);
			}
			REQUIRE(other.GetRowCount() == idx_t(ROW_COUNT));

			collection.Combine(std::move(other));
			REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT * 2));
		}
	});
}

TEST_CASE("Stable C++-API: Column Data Multi Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	constexpr int64_t ROW_COUNT = 4100;
	auto result = conn.Query("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetColumnCount() == 2);

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT());
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);

		{
			auto append_state = collection.GetAppendState();
			for (idx_t i = 0; i < result.GetChunkCount(); i++) {
				auto chunk = result.GetChunk(i);
				collection.Append(append_state, chunk);
			}
		}
		REQUIRE(collection.GetRowCount() == idx_t(ROW_COUNT));

		// Drive the parallel scan API single-threaded: a shared state coordinates
		// the scan, while a per-worker state tracks the current worker's position.
		auto shared_state = collection.GetSharedScanState();
		auto worker_state = collection.GetWorkerScanState();
		DataChunk out(ctx, types);

		idx_t scanned = 0;
		int64_t sum_a = 0;
		while (collection.Scan(shared_state, worker_state, out)) {
			const auto rows = out.GetRowCount();
			const auto a = out.GetVector(0).GetDataMutable<const int64_t>();
			const auto b = out.GetVector(1).GetDataMutable<const int64_t>();
			for (idx_t r = 0; r < rows; r++) {
				REQUIRE(b[r] == a[r] * 10);
				sum_a += a[r];
			}
			scanned += rows;
		}

		REQUIRE(scanned == idx_t(ROW_COUNT));
		REQUIRE(sum_a == (ROW_COUNT - 1) * ROW_COUNT / 2);
	});
}

TEST_CASE("Stable C++-API: Logging", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.Query("CALL enable_logging(storage = 'memory')");

	conn.Log(LogLevel::LOG_INFO, "This is an informational message from a connection");

	conn.WithTransaction([](const Context &ctx) {
		ctx.Log(LogLevel::LOG_INFO, "This is an informational message.");
		ctx.Log(LogLevel::LOG_WARN, "This is a warning message.");
		ctx.Log(LogLevel::LOG_ERROR, "This is an error message.");
	});

	// We don't have a good way to inspect error messages, cause we cant consume VARCHARs yet.
	auto res = conn.Query("SELECT case when log_level = 'INFO' then 1 when log_level = 'WARNING' then 2 when log_level "
	                      "= 'ERROR' then 3 else -1 end as level FROM duckdb_logs");
	auto chunk = res.GetChunk(0);
	auto vec = chunk.GetVector(0);
	auto data = vec.GetDataMutable<const int32_t>();

	REQUIRE(data[0] == 1);
	REQUIRE(data[1] == 1);
	REQUIRE(data[2] == 2);
	REQUIRE(data[3] == 3);
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

TEST_CASE("Stable C++-API: Log Storage", "[cpp_api]") {
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
	conn.Query("SET enable_logging = true;");
	conn.Query("SET logging_storage = 'cpp_custom_storage';");

	// Emit a couple of log entries with a known type/level/message.
	conn.Query("SELECT write_log('first message', log_type := 'cpp_api_test', level := 'WARNING');");
	conn.Query("SELECT write_log('second message', log_type := 'cpp_api_test', level := 'ERROR');");

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
