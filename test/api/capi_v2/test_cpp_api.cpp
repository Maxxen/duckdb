#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "test_helpers.hpp"

// For the DUCKDB_V2_ERROR_* codes asserted against Exception::GetCode().
#include "duckdb_v2.h"

#include <atomic>
#include <cstring>
#include <sstream>

// ---------------------------------------------------------------------------
// Stable C-API testing
// ---------------------------------------------------------------------------

namespace {

// Matcher for REQUIRE_THROWS_MATCHES: the thrown duckdb_api::Exception
// carries the expected V2 error code.
class HasErrorCode : public Catch::MatcherBase<duckdb_api::Exception> {
public:
	explicit HasErrorCode(int32_t code) : code(code) {
	}
	bool match(const duckdb_api::Exception &ex) const override {
		return ex.GetCode() == code;
	}
	std::string describe() const override {
		return "has error code " + std::to_string(code);
	}

private:
	int32_t code;
};

// Counts how many rows the scalar-property exec callback below is invoked over,
// used to observe whether the optimizer constant-folded the function.
std::atomic<duckdb_api::idx_t> g_property_exec_rows {0};

} // namespace

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

	auto result = conn.Query("SELECT MyFunction(1, 2) AS result");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetColumnName(0) == "result");

	auto chunk = result.FetchChunk();
	REQUIRE(chunk);

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

	auto result = conn.Query("SELECT MyAggregate(i) AS result FROM (VALUES (1), (2), (3)) AS t(i)");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetColumnName(0) == "result");

	auto chunk = result.FetchChunk();
	REQUIRE(chunk);

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

	auto result = conn.Query("SELECT * FROM MyRangeFunction(0, 10, step => 2)");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetColumnName(0) == "i");

	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto vec = chunk.GetVector(0);

	auto data = vec.GetDataMutable<const int32_t>();
	REQUIRE(chunk.GetRowCount() == 5);
	REQUIRE(data[0] == 0);
	REQUIRE(data[1] == 2);
	REQUIRE(data[2] == 4);
	REQUIRE(data[3] == 6);
	REQUIRE(data[4] == 8);
}

TEST_CASE("Stable C++API: Copy Function", "[cpp_api]") {
	using namespace duckdb_api;

	// State threaded through the callbacks. The bind data records the column count; the global (init) state owns the
	// output path and accumulates the total number of rows seen; each batch reports its own row count.
	struct GlobalState {
		std::string path;
		idx_t columns = 0;
		idx_t total_rows = 0;
	};

	const auto out_path = duckdb::TestCreatePath("cpp_api_copy_function.txt");

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		CopyFunction copy_function(ctx);

		copy_function.SetName("my_copy")
		    .SetBindCallback([](CopyFunction::BindInput &input) {
			    // Verify that the type is what we expect
			    const auto column_count = input.GetColumnCount();
			    REQUIRE(column_count == 1);
			    const auto column_name = input.GetColumnName(0);
			    REQUIRE(column_name == "i");
			    const auto column_type = input.GetColumnType(0);
			    REQUIRE(column_type == LogicalType::BIGINT());

			    // Remember how many columns are being copied.
			    input.SetBindData<idx_t>(input.GetColumnCount());
		    })
		    .SetInitCallback([](CopyFunction::InitInput &input) {
			    // Open a per-file global state, seeded with the destination path and the bound column count.
			    input.SetInitData<GlobalState>(
			        GlobalState {std::string(input.GetFilePath()), input.GetBindData<idx_t>(), 0});
		    })
		    .SetBatchCallback([](CopyFunction::BatchInput &input) {
			    // The batch is handed to us owning; reading its row count proves it round-trips correctly.
			    input.SetBatchData<idx_t>(input.GetBatch().GetRowCount());
		    })
		    .SetFlushCallback([](CopyFunction::FlushInput &input) {
			    input.GetInitData<GlobalState>().total_rows += input.GetBatchData<idx_t>();
		    })
		    .SetFinalizeCallback([](CopyFunction::FinalizeInput &input) {
			    // Write the accumulated summary out (via DuckDB's file system) so the test can verify the chain ran.
			    const auto &state = input.GetInitData<GlobalState>();
			    const auto contents = std::to_string(state.columns) + " " + std::to_string(state.total_rows);

			    auto fs = input.GetContext().GetFileSystem();
			    auto file = fs.OpenFile(state.path, FileFlags::WRITE | FileFlags::CREATE);
			    file.Write(contents.data(), static_cast<int64_t>(contents.size()));
		    })
		    .Register(ctx);
	});

	conn.Query("COPY (SELECT i FROM range(5) t(i)) TO '" + out_path + "' (FORMAT my_copy, USE_TMP_FILE FALSE)").Drain();

	// Read the summary back through the file system API and verify the full bind -> init -> batch -> flush ->
	// finalize chain observed 1 column and 5 rows.

	conn.WithTransaction([&](const Context &ctx) {
		auto fs = ctx.GetFileSystem();
		auto file = fs.OpenFile(out_path, FileFlags::READ);
		char buffer[64] = {0};
		const auto bytes_read = file.Read(buffer, sizeof(buffer) - 1);

		auto contents = std::string(buffer, bytes_read);
		auto split = contents.find(' ');
		auto columns = std::stoul(contents.substr(0, split));
		auto rows = std::stoul(contents.substr(split + 1));

		REQUIRE(columns == 1);
		REQUIRE(rows == 5);
	});
}

TEST_CASE("Stable C++API: Cast Function", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Register the custom type TEMPERATURE (an alias of INTEGER) via the C++ CustomType wrapper.
		CustomType custom_type(ctx);
		custom_type.SetName("TEMPERATURE").SetBaseType(LogicalType::INTEGER()).Register(ctx);

		// A LogicalType for the registered custom type, used as the cast's source type.
		auto temperature = LogicalType::INTEGER().WithAlias("TEMPERATURE");

		// Register a TEMPERATURE -> BIGINT cast that adds 1000 (a distinctive transform we can assert on).
		CastFunction cast(ctx);
		cast.SetSourceType(temperature)
		    .SetTargetType(LogicalType::BIGINT())
		    .SetImplicitCastCost(0)
		    .SetExecCallback([](CastFunction::ExecInput &input) {
			    auto in_vec = input.GetInput();
			    auto out_vec = input.GetOutput();
			    // The bridge passes the input vector as-is (it is not flattened for us); flatten it so we can
			    // index its data directly regardless of whether it arrived constant / dictionary-encoded.
			    in_vec.Flatten();
			    const auto in = in_vec.GetDataMutable<const int32_t>();
			    const auto out = out_vec.GetDataMutable<int64_t>();
			    for (idx_t i = 0; i < input.GetCount(); i++) {
				    out[i] = static_cast<int64_t>(in[i]) + 1000;
			    }
		    })
		    .Register(ctx);
	});

	auto result = conn.Query("SELECT CAST(CAST(42 AS TEMPERATURE) AS BIGINT) AS result");
	REQUIRE(result.GetColumnCount() == 1);
	REQUIRE(result.GetColumnName(0) == "result");
	REQUIRE(result.GetColumnType(0) == LogicalType::BIGINT());

	auto chunk = result.FetchChunk();
	auto vec = chunk.GetVector(0);
	const auto data = vec.GetDataMutable<const int64_t>();
	REQUIRE(chunk.GetRowCount() == 1);
	REQUIRE(data[0] == 1042);
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

	// Drain the stream up front: chunks are appended twice below, and the
	// result must be consumed outside WithTransaction (the transaction
	// callback holds the context lock the stream needs).
	std::vector<DataChunk> chunks;
	while (auto chunk = result.FetchChunk()) {
		chunks.push_back(std::move(chunk));
	}

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT()); // range() produces BIGINT
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);
		REQUIRE(collection.GetRowCount() == 0);

		// Append every chunk of the query result into the collection.
		{
			auto append_state = collection.GetAppendState();
			for (auto &chunk : chunks) {
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
			for (auto &chunk : chunks) {
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

	// Drain the stream before entering the transaction callback (which
	// holds the context lock the stream needs).
	std::vector<DataChunk> chunks;
	while (auto chunk = result.FetchChunk()) {
		chunks.push_back(std::move(chunk));
	}

	conn.WithTransaction([&](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT());
		types.push_back(LogicalType::BIGINT());

		ColumnDataCollection collection(ctx, types);

		{
			auto append_state = collection.GetAppendState();
			for (auto &chunk : chunks) {
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

	conn.Query("CALL enable_logging(storage = 'memory')").Drain();

	conn.Log(LogLevel::LOG_INFO, "This is an informational message from a connection");

	conn.WithTransaction([](const Context &ctx) {
		ctx.Log(LogLevel::LOG_INFO, "This is an informational message.");
		ctx.Log(LogLevel::LOG_WARN, "This is a warning message.");
		ctx.Log(LogLevel::LOG_ERROR, "This is an error message.");
	});

	// We don't have a good way to inspect error messages, cause we cant consume VARCHARs yet.
	auto res = conn.Query("SELECT case when log_level = 'INFO' then 1 when log_level = 'WARNING' then 2 when log_level "
	                      "= 'ERROR' then 3 else -1 end as level FROM duckdb_logs");
	auto chunk = res.FetchChunk();
	REQUIRE(chunk);
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
	conn.Query("SET enable_logging = true;").Drain();
	conn.Query("SET logging_storage = 'cpp_custom_storage';").Drain();

	// Emit a couple of log entries with a known type/level/message.
	conn.Query("SELECT write_log('first message', log_type := 'cpp_api_test', level := 'WARNING');").Drain();
	conn.Query("SELECT write_log('second message', log_type := 'cpp_api_test', level := 'ERROR');").Drain();

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

//----------------------------------------------------------------------------------------------------------------------
// Streaming result surface (step primitive + conveniences)
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("Stable C++-API: step loop drains a multi-chunk result", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Query("SELECT i FROM range(100000) t(i)");

	idx_t total_rows = 0;
	idx_t chunk_count = 0;
	while (true) {
		auto step = result.Step();
		if (step.status == QueryResult::StepStatus::CHUNK) {
			REQUIRE(step.chunk);
			total_rows += step.chunk.GetRowCount();
			chunk_count++;
			continue;
		}
		REQUIRE(!step.chunk); // non-empty iff status is CHUNK
		if (step.status == QueryResult::StepStatus::WAITING) {
			result.Wait();
			continue;
		}
		REQUIRE(step.status == QueryResult::StepStatus::FINISHED);
		break;
	}
	REQUIRE(total_rows == 100000);
	REQUIRE(chunk_count > 1);

	// FINISHED is sticky, and waiting on a terminal result is a no-op.
	auto step = result.Step();
	REQUIRE(step.status == QueryResult::StepStatus::FINISHED);
	REQUIRE(!step.chunk);
	result.Wait();
}

TEST_CASE("Stable C++-API: Drain applies side effects and reports rows changed", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(conn.Query("CREATE TABLE t (i INTEGER)").Drain() == 0);
	REQUIRE(conn.Query("INSERT INTO t VALUES (1), (2), (3)").Drain() == 3);
	REQUIRE(conn.Query("DELETE FROM t WHERE i = 1").Drain() == 1);
	REQUIRE(conn.Query("SELECT i FROM range(1000) t(i)").Drain() == 0); // rows drained and discarded

	auto result = conn.Query("SELECT i FROM t");
	idx_t rows = 0;
	while (auto chunk = result.FetchChunk()) {
		rows += chunk.GetRowCount();
	}
	REQUIRE(rows == 2);
}

TEST_CASE("Stable C++-API: a busy connection refuses new work with RESOURCE_IN_USE", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto live = conn.Query("SELECT i FROM range(100000) t(i)");

	REQUIRE_THROWS_MATCHES(conn.Query("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));

	// Draining the live result frees the connection.
	while (live.FetchChunk()) {
	}
	auto second = conn.Query("SELECT 1");
	REQUIRE(second.FetchChunk());
}

TEST_CASE("Stable C++-API: Interrupt cancels a running query", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Query("SELECT i FROM range(10000000) t(i)");
	conn.Interrupt();

	// Steps observe the cancellation as the sticky CANCELLED status.
	auto status = QueryResult::StepStatus::WAITING;
	for (int i = 0; i < 1000 && status != QueryResult::StepStatus::CANCELLED; i++) {
		status = result.Step().status;
	}
	REQUIRE(status == QueryResult::StepStatus::CANCELLED);

	// FetchChunk reports the same event on the error channel.
	REQUIRE_THROWS_MATCHES(result.FetchChunk(), Exception, HasErrorCode(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT));

	// The cancelled result freed the connection.
	REQUIRE(conn.Query("SELECT 1").Drain() == 0);
}

TEST_CASE("Stable C++-API: GetQueryProgress reports idle values when no query is active", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto progress = conn.GetQueryProgress();
	REQUIRE(progress.percentage == -1.0);
	REQUIRE(progress.rows_processed == 0);
	REQUIRE(progress.total_rows_to_process == 0);
}

TEST_CASE("Stable C++-API: ParseSQL iterates statements into Query", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto statements = conn.ParseSQL("SELECT 42; SELECT 84; SELECT 126");
	int statement_count = 0;
	while (auto statement = statements.Next()) {
		auto result = conn.Query(std::move(statement));
		REQUIRE(result.FetchChunk());
		result.Drain();
		statement_count++;
	}
	REQUIRE(statement_count == 3);

	// Exhaustion is idempotent.
	REQUIRE(!statements.Next());

	// The string-taking Query is single-statement sugar.
	REQUIRE_THROWS_MATCHES(conn.Query("SELECT 1; SELECT 2"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE_THROWS_MATCHES(conn.Query(""), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}

TEST_CASE("Stable C++-API: Exception carries the code and message body", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Binder error: GetCode() is the identity, GetRawMessage() the unprefixed body.
	try {
		conn.Query("SELECT * FROM no_such_table");
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

TEST_CASE("Stable C++API: Function properties", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Scalar: common properties default and round-trip.
		ScalarFunction scalar(ctx);
		REQUIRE(scalar.GetStability() == FunctionStability::Consistent);
		REQUIRE(scalar.GetNullHandling() == FunctionNullHandling::Default);
		REQUIRE(scalar.GetFallibility() == FunctionFallibility::Infallible);
		REQUIRE(scalar.GetCollationHandling() == FunctionCollationHandling::Propagate);

		scalar.SetStability(FunctionStability::Volatile)
		    .SetNullHandling(FunctionNullHandling::Special)
		    .SetFallibility(FunctionFallibility::Fallible)
		    .SetCollationHandling(FunctionCollationHandling::Ignore);

		REQUIRE(scalar.GetStability() == FunctionStability::Volatile);
		REQUIRE(scalar.GetNullHandling() == FunctionNullHandling::Special);
		REQUIRE(scalar.GetFallibility() == FunctionFallibility::Fallible);
		REQUIRE(scalar.GetCollationHandling() == FunctionCollationHandling::Ignore);

		// Aggregate: shares the common properties and adds its own.
		AggregateFunction aggregate(ctx);
		REQUIRE(aggregate.GetStability() == FunctionStability::Consistent);
		REQUIRE(aggregate.GetOrderDependence() == AggregateFunction::OrderDependence::Dependent);
		REQUIRE(aggregate.GetDistinctDependence() == AggregateFunction::DistinctDependence::Dependent);

		aggregate.SetStability(FunctionStability::ConsistentWithinQuery)
		    .SetOrderDependence(AggregateFunction::OrderDependence::Independent)
		    .SetDistinctDependence(AggregateFunction::DistinctDependence::Independent);

		REQUIRE(aggregate.GetStability() == FunctionStability::ConsistentWithinQuery);
		REQUIRE(aggregate.GetOrderDependence() == AggregateFunction::OrderDependence::Independent);
		REQUIRE(aggregate.GetDistinctDependence() == AggregateFunction::DistinctDependence::Independent);
	});
}

TEST_CASE("Stable C++API: Volatility affects constant folding", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// An exec callback that counts the rows it processes and writes a constant.
	auto exec = [](ScalarFunction::ExecInput &input) {
		auto chunk = input.GetInputChunk();
		auto out = input.GetResultVector().GetDataMutable<int32_t>();
		auto count = chunk.GetRowCount();
		for (idx_t i = 0; i < count; i++) {
			out[i] = 0;
		}
		g_property_exec_rows += count;
	};

	conn.WithTransaction([&](const Context &ctx) {
		// Default stability (CONSISTENT): foldable when its argument is constant.
		ScalarFunction consistent(ctx);
		consistent.SetName("prop_consistent")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback(exec)
		    .Register(ctx);

		// Same function, but VOLATILE: must be evaluated for every row.
		ScalarFunction vol(ctx);
		vol.SetName("prop_volatile")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetStability(FunctionStability::Volatile)
		    .SetExecCallback(exec)
		    .Register(ctx);
	});

	auto drain = [&](const char *sql) -> idx_t {
		g_property_exec_rows = 0;
		auto result = conn.Query(sql);
		while (auto chunk = result.FetchChunk()) {
		}
		return g_property_exec_rows.load();
	};

	// With a constant argument the consistent function is folded to a single
	// evaluation, while the volatile one runs for all 1000 rows.
	auto consistent_rows = drain("SELECT prop_consistent(42) FROM range(1000)");
	auto volatile_rows = drain("SELECT prop_volatile(42) FROM range(1000)");

	REQUIRE(volatile_rows == 1000);
	REQUIRE(consistent_rows < volatile_rows);
}
