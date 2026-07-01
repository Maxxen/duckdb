#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_helpers.hpp"

// For the DUCKDB_V2_ERROR_* codes asserted against Exception::GetCode().
#include "duckdb_v2.h"

#include <atomic>
#include <cstring>
#include <fstream>
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

	auto result = conn.Execute("SELECT MyFunction(1, 2) AS result");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "result");

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

	auto result = conn.Execute("SELECT MyAggregate(i) AS result FROM (VALUES (1), (2), (3)) AS t(i)");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "result");

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

	auto result = conn.Execute("SELECT * FROM MyRangeFunction(0, 10, step => 2)");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "i");

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
			    auto file = fs.OpenFile(state.path, FileFlags::WRITE | FileFlags::FILE_CREATE);
			    file.Write(contents.data(), static_cast<int64_t>(contents.size()));
		    })
		    .Register(ctx);
	});

	conn.Execute("COPY (SELECT i FROM range(5) t(i)) TO '" + out_path + "' (FORMAT my_copy, USE_TMP_FILE FALSE)")
	    .Drain();

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

	auto result = conn.Execute("SELECT CAST(CAST(42 AS TEMPERATURE) AS BIGINT) AS result");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "result");
	REQUIRE(result.GetSchema().GetFieldType(0) == LogicalType::BIGINT());

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

TEST_CASE("Stable C++-API: Column Data Single Scan", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Produce 4100 rows of (a, b) where b == a * 10. This spans several internal
	// chunks (> STANDARD_VECTOR_SIZE), so both append and scan iterate multiple times.
	constexpr int64_t ROW_COUNT = 4100;
	auto result = conn.Execute("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetSchema().GetFieldCount() == 2);

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
	auto result = conn.Execute("SELECT i AS a, i * 10 AS b FROM range(4100) t(i)");
	REQUIRE(result.GetSchema().GetFieldCount() == 2);

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

	conn.Execute("CALL enable_logging(storage = 'memory')").Drain();

	conn.Log(LogLevel::LOG_INFO, "This is an informational message from a connection");

	conn.WithTransaction([](const Context &ctx) {
		ctx.Log(LogLevel::LOG_INFO, "This is an informational message.");
		ctx.Log(LogLevel::LOG_WARN, "This is a warning message.");
		ctx.Log(LogLevel::LOG_ERROR, "This is an error message.");
	});

	// We don't have a good way to inspect error messages, cause we cant consume VARCHARs yet.
	auto res =
	    conn.Execute("SELECT case when log_level = 'INFO' then 1 when log_level = 'WARNING' then 2 when log_level "
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

//----------------------------------------------------------------------------------------------------------------------
// Streaming result surface (step primitive + conveniences)
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("Stable C++-API: step loop drains a multi-chunk result", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT i FROM range(100000) t(i)");

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

	REQUIRE(conn.Execute("CREATE TABLE t (i INTEGER)").Drain() == 0);
	REQUIRE(conn.Execute("INSERT INTO t VALUES (1), (2), (3)").Drain() == 3);
	REQUIRE(conn.Execute("DELETE FROM t WHERE i = 1").Drain() == 1);
	REQUIRE(conn.Execute("SELECT i FROM range(1000) t(i)").Drain() == 0); // rows drained and discarded

	auto result = conn.Execute("SELECT i FROM t");
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

	auto live = conn.Execute("SELECT i FROM range(100000) t(i)");

	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));

	// Draining the live result frees the connection.
	while (live.FetchChunk()) {
	}
	auto second = conn.Execute("SELECT 1");
	REQUIRE(second.FetchChunk());
}

TEST_CASE("Stable C++-API: Interrupt cancels a running query", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT i FROM range(10000000) t(i)");
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
	REQUIRE(conn.Execute("SELECT 1").Drain() == 0);
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

TEST_CASE("Stable C++-API: ParseSQL iterates statements into Execute", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto statements = conn.ParseSQL("SELECT 42; SELECT 84; SELECT 126");
	int statement_count = 0;
	while (auto statement = statements.Next()) {
		auto result = conn.Execute(statement);
		REQUIRE(result.FetchChunk());
		result.Drain();
		statement_count++;
	}
	REQUIRE(statement_count == 3);

	// Exhaustion is idempotent.
	REQUIRE(!statements.Next());

	// The string-taking Execute is single-statement sugar.
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1; SELECT 2"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE_THROWS_MATCHES(conn.Execute(""), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}

TEST_CASE("Stable C++-API: Exception carries the code and message body", "[cpp_api]") {
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
		auto result = conn.Execute(sql);
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
			input.AddParameter(Value::FromI64(5));
		});
		auto result = conn.Execute("SELECT * FROM cpp_repl_no_such_table");
		REQUIRE(result.GetSchema().GetFieldCount() == 1);
		auto chunk = result.FetchChunk();
		REQUIRE(chunk);
		REQUIRE(chunk.GetRowCount() == 5);
		// range(5) yields the BIGINT sequence 0..4 in order.
		auto data = chunk.GetVector(0).GetDataMutable<const int64_t>();
		for (idx_t i = 0; i < 5; i++) {
			REQUIRE(data[i] == static_cast<int64_t>(i));
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
			    input.AddParameter(Value::FromVarchar(path));
			    input.AddNamedParameter("sep", Value::FromVarchar(","));
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
			    input.AddParameter(Value::FromVarchar(path));
			    input.AddNamedParameter("sep", Value::FromVarchar(","));
		    },
		    csv_path);

		auto result = conn.Execute("SELECT * FROM cpp_repl_ctx_placeholder");
		REQUIRE(result.GetSchema().GetFieldCount() == 2);
		REQUIRE(count_rows(std::move(result)) == 2);
	}
}

TEST_CASE("Stable C++API: Vector AssignString", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::VARCHAR());

		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(3);

		// AssignString resolves the heap once and reuses it for the rest.
		vec.AssignString(0, "hi"); // inlined
		const std::string long_str(100, 'x');
		vec.AssignString(1, long_str); // copied into the heap
		vec.AssignString(2, "");       // empty

		// The cpp_api has no VARCHAR read path yet; decode through the C API to
		// confirm the bytes round-trip.
		auto *slots = vec.GetDataMutable<duckdb_v2_string>();
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_varchar_decode(&slots[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == "hi");
		REQUIRE(duckdb_v2_varchar_decode(&slots[1], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == long_str);
		REQUIRE(duckdb_v2_varchar_decode(&slots[2], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out.len == 0);
	});
}

TEST_CASE("Stable C++API: Vector AssignStrings (bulk)", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::VARCHAR());

		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(3);

		// A single write, then a bulk write starting at index 1; both share the
		// cached heap. The bulk batch mixes an inlined and a heap-allocated value.
		vec.AssignString(0, "head");
		const std::vector<std::string> owned = {"second", "this tail value is comfortably longer than twelve bytes"};
		const std::vector<std::string_view> tail(owned.begin(), owned.end());
		vec.AssignStrings(1, tail);

		auto *slots = vec.GetDataMutable<duckdb_v2_string>();
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_varchar_decode(&slots[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == "head");
		REQUIRE(duckdb_v2_varchar_decode(&slots[1], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == owned[0]);
		REQUIRE(duckdb_v2_varchar_decode(&slots[2], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == owned[1]);

		// An empty batch is a no-op.
		vec.AssignStrings(0, {});
	});
}

TEST_CASE("Stable C++API: StringHeap primitive (dedup + scatter)", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::VARCHAR());

		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(4);

		auto heap = vec.GetStringHeap();

		// Dedup: intern a (non-inlined) value once, reference it from many slots.
		const std::string shared_str = "this is a shared value, longer than twelve bytes";
		auto shared = heap.Add(shared_str);
		vec.SetString(0, shared);
		vec.SetString(2, shared);

		// Bulk intern, then scatter the tokens into arbitrary positions.
		const std::vector<std::string> owned = {"x", "another longer-than-inline string value"};
		auto tokens = heap.AddMany(std::vector<std::string_view>(owned.begin(), owned.end()));
		vec.SetString(3, tokens[0]);
		vec.SetString(1, tokens[1]);

		auto *slots = vec.GetDataMutable<duckdb_v2_string>();
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_varchar_decode(&slots[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == shared_str);
		REQUIRE(duckdb_v2_varchar_decode(&slots[1], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == owned[1]);
		REQUIRE(duckdb_v2_varchar_decode(&slots[2], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == shared_str);
		REQUIRE(duckdb_v2_varchar_decode(&slots[3], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == "x");

		// An empty AddMany returns an empty vector.
		REQUIRE(heap.AddMany({}).empty());
	});
}

TEST_CASE("Stable C++API: StringHeap::Allocate write-in-place", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::VARCHAR());

		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(2);

		auto heap = vec.GetStringHeap();

		// Write-in-place: generate bytes straight into the heap, then build a token over them.
		const uint32_t len = 64;
		auto *bytes = heap.Allocate(len);
		REQUIRE(bytes != nullptr);
		std::memset(bytes, 'q', len);
		auto token = StringStorage::FromHeapData(reinterpret_cast<char *>(bytes), len);
		REQUIRE_FALSE(token.IsInlined());
		REQUIRE(token.Length() == len);
		REQUIRE(token.Data() == reinterpret_cast<const char *>(bytes));
		vec.SetString(0, token);

		// Inlined token: the bytes live in the value itself.
		auto small = heap.Add("tiny");
		REQUIRE(small.IsInlined());
		REQUIRE(small.Length() == 4);
		REQUIRE(std::string(small.Data(), small.Length()) == "tiny");
		vec.SetString(1, small);

		auto *slots = vec.GetDataMutable<duckdb_v2_string>();
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_varchar_decode(&slots[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == std::string(len, 'q'));
		REQUIRE(duckdb_v2_varchar_decode(&slots[1], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == "tiny");
	});
}

TEST_CASE("Stable C++API: StringStorage GetDataWritable + Finalize", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::VARCHAR());

		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(1);

		auto heap = vec.GetStringHeap();

		// Point a token at heap bytes, write through GetDataWritable, seal with Finalize.
		const uint32_t len = 40;
		auto *bytes = heap.Allocate(len);
		REQUIRE(bytes != nullptr);

		StringStorage token {};
		token.value.pointer.length = len;
		token.value.pointer.ptr = reinterpret_cast<char *>(bytes);
		REQUIRE_FALSE(token.IsInlined());
		REQUIRE(token.GetDataWritable() == reinterpret_cast<char *>(bytes));

		const std::string payload(len, 'z');
		std::memcpy(token.GetDataWritable(), payload.data(), len);
		token.Finalize();

		// Finalize seals the prefix to the first PREFIX_LENGTH bytes.
		REQUIRE(std::memcmp(token.value.pointer.prefix, payload.data(), StringStorage::PREFIX_LENGTH) == 0);
		vec.SetString(0, token);

		auto *slots = vec.GetDataMutable<duckdb_v2_string>();
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_varchar_decode(&slots[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out.ptr, out.len) == payload);
	});
}

TEST_CASE("Stable C++API: AssignString rejects misuse", "[cpp_api]") {
	using namespace duckdb_api;

	// A non-string vector has no heap: AssignString surfaces INVALID_INPUT.
	{
		Environment env;
		auto db = env.Open(":memory:");
		auto conn = db.Connect();
		conn.WithTransaction([](const Context &ctx) {
			std::vector<LogicalType> types;
			types.push_back(LogicalType::INTEGER());
			DataChunk chunk(ctx, types);
			auto vec = chunk.GetVector(0);
			vec.SetSize(1);
			REQUIRE_THROWS_MATCHES(vec.AssignString(0, "x"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		});
	}

	// A CONSTANT vector's data array holds one slot: only index 0 is writable.
	// Built through the C API (the C++ surface has no make-constant) and wrapped.
	{
		duckdb_v2_logical_type_handle vtype = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &vtype, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_data_chunk_create(&vtype, 1, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_logical_type_destroy(&vtype) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_handle cvec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &cvec, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_value_handle val = nullptr;
		REQUIRE(duckdb_v2_value_create_varchar(duckdb_v2_str {"const", 5}, &val, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_make_constant(cvec, val, 2, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_value_destroy(&val) == DUCKDB_V2_ERROR_NONE);

		auto vec = detail::Factory::Make<Vector>(cvec);
		REQUIRE_THROWS_MATCHES(vec.AssignString(1, "x"), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		REQUIRE_NOTHROW(vec.AssignString(0, "ok"));

		REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	}
}

// ---------------------------------------------------------------------------
// Arrow stream export
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++-API: Arrow stream export", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	SECTION("default batch yields all rows and a stable schema") {
		auto result = conn.Execute("SELECT i FROM range(100000) t(i)");
		auto stream = result.ToArrowStream();
		// ToArrowStream consumes the result.
		REQUIRE_FALSE(static_cast<bool>(result));
		REQUIRE(static_cast<bool>(stream));

		ArrowSchema schema {};
		stream.GetSchema(schema);
		REQUIRE(schema.release != nullptr);
		REQUIRE(schema.n_children == 1);
		schema.release(&schema);

		int64_t rows = 0;
		idx_t arrays = 0;
		ArrowArray array {};
		while (stream.Next(array)) {
			rows += array.length;
			arrays++;
			array.release(&array);
		}
		REQUIRE(rows == 100000);
		// The default batch (131072) exceeds the row count: a single array.
		REQUIRE(arrays == 1);
	}

	SECTION("batch_size coalesces into multiple arrays") {
		auto result = conn.Execute("SELECT i FROM range(100000) t(i)");
		auto stream = result.ToArrowStream(10000);

		int64_t rows = 0;
		idx_t arrays = 0;
		int64_t first_len = 0;
		ArrowArray array {};
		while (stream.Next(array)) {
			if (arrays == 0) {
				first_len = array.length;
			}
			rows += array.length;
			arrays++;
			array.release(&array);
		}
		REQUIRE(rows == 100000);
		REQUIRE(arrays == 10);
		// Each array coalesces many engine chunks (10000 > STANDARD_VECTOR_SIZE).
		REQUIRE(first_len == 10000);
	}

	SECTION("export over a partially consumed result yields the remainder") {
		auto result = conn.Execute("SELECT i FROM range(5000) t(i)");
		// Consume the first engine chunk (2048 rows).
		auto first = result.FetchChunk();
		REQUIRE(static_cast<bool>(first));
		auto consumed = first.GetRowCount();
		REQUIRE(consumed == 2048);

		auto stream = result.ToArrowStream();

		// The stream must continue from the consumed offset, not restart: the
		// first remaining value (a BIGINT inside the struct array) must equal it.
		ArrowArray first_remaining {};
		REQUIRE(stream.Next(first_remaining));
		REQUIRE(first_remaining.n_children == 1);
		const auto *col = first_remaining.children[0];
		const auto *values = reinterpret_cast<const int64_t *>(col->buffers[1]);
		REQUIRE(values[col->offset] == static_cast<int64_t>(consumed));
		int64_t rows = first_remaining.length;
		first_remaining.release(&first_remaining);

		ArrowArray array {};
		while (stream.Next(array)) {
			rows += array.length;
			array.release(&array);
		}
		REQUIRE(rows == static_cast<int64_t>(5000 - consumed));
	}

	SECTION("the stream owns the connection until destroyed") {
		{
			auto result = conn.Execute("SELECT i FROM range(10) t(i)");
			auto stream = result.ToArrowStream();
			// The connection is busy while the stream is live.
			REQUIRE_THROWS_MATCHES(conn.Execute("SELECT 1"), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
		}
		// Stream destroyed: the connection is free again.
		REQUIRE(static_cast<bool>(conn.Execute("SELECT 1").FetchChunk()));
	}

	SECTION("operations on a moved-from stream throw") {
		auto result = conn.Execute("SELECT i FROM range(10) t(i)");
		auto stream = result.ToArrowStream();
		auto moved = std::move(stream);
		REQUIRE_FALSE(static_cast<bool>(stream));
		REQUIRE(static_cast<bool>(moved));

		ArrowSchema schema {};
		REQUIRE_THROWS_MATCHES(stream.GetSchema(schema), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		ArrowArray array {};
		REQUIRE_THROWS_MATCHES(stream.Next(array), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	}
}

TEST_CASE("Stable C++API: Bind", "[cpp_api][statement_bind]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(a INTEGER, b VARCHAR)").Drain();

	auto iter = conn.ParseSQL("SELECT a, b FROM t WHERE a = $1");
	auto stmt = iter.Next();
	REQUIRE(static_cast<bool>(stmt));

	auto sig = conn.Bind(stmt);
	REQUIRE(sig.output.GetFieldCount() == 2);
	REQUIRE(sig.output.GetFieldName(0) == "a");
	REQUIRE(sig.output.GetFieldType(0) == LogicalType::INTEGER());
	REQUIRE(sig.output.GetFieldName(1) == "b");
	REQUIRE(sig.output.GetFieldType(1) == LogicalType::VARCHAR());
	REQUIRE(sig.parameters.GetFieldCount() == 1);
	REQUIRE(sig.parameters.GetFieldName(0) == "1");
	REQUIRE(sig.parameters.GetFieldType(0) == LogicalType::INTEGER());

	// Non-consuming: the statement is still alive and re-bindable.
	REQUIRE(static_cast<bool>(stmt));
	auto sig2 = conn.Bind(stmt);
	REQUIRE(sig2.output.GetFieldCount() == 2);

	// Dynamic PIVOT is rejected with INVALID_INPUT.
	conn.Execute("CREATE TABLE sales(product VARCHAR, quarter VARCHAR, amount INTEGER)").Drain();
	auto piter = conn.ParseSQL("PIVOT sales ON quarter USING sum(amount)");
	auto pstmt = piter.Next();
	REQUIRE_THROWS_MATCHES(conn.Bind(pstmt), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}

TEST_CASE("Stable C++API: QueryResult GetSchema", "[cpp_api][query_result]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto result = conn.Execute("SELECT 1 AS a, 'x' AS b");
	auto schema = result.GetSchema();
	REQUIRE(schema.GetFieldCount() == 2);
	REQUIRE(schema.GetFieldName(0) == "a");
	REQUIRE(schema.GetFieldName(1) == "b");
	REQUIRE(schema.GetFieldType(0) == LogicalType::INTEGER());
}

namespace {
using namespace duckdb_api;

// Collect two columns of a result into rows, reading each column as its C type.
template <class TA, class TB>
std::vector<std::pair<TA, TB>> Collect2(QueryResult result, idx_t a, idx_t b) {
	std::vector<std::pair<TA, TB>> rows;
	while (auto chunk = result.FetchChunk()) {
		auto va = chunk.GetVector(a);
		auto vb = chunk.GetVector(b);
		va.Flatten();
		vb.Flatten();
		auto pa = va.GetDataMutable<const TA>();
		auto pb = vb.GetDataMutable<const TB>();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			rows.emplace_back(pa[i], pb[i]);
		}
	}
	return rows;
}

} // namespace

TEST_CASE("Stable C++API: prepared statements", "[cpp_api][prepared]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE scores(id INTEGER, score INTEGER)").Drain();
	conn.Execute("INSERT INTO scores VALUES (1, 40), (2, 55), (3, 70), (4, 90)").Drain();

	// Value is move-only, so a parameter list is built by move, not brace-init.
	auto Params = [](std::initializer_list<int64_t> values) {
		std::vector<Value> params;
		for (auto value : values) {
			params.push_back(Value::FromI64(value));
		}
		return params;
	};

	SECTION("bind once, execute many with different parameters") {
		// Parse and bind once, then reuse across executions (binding neither executes
		// nor consumes): the parse-once, bind-once, execute-many pattern.
		auto iter = conn.ParseSQL("SELECT id, score FROM scores WHERE score >= $1 ORDER BY id");
		auto stmt = iter.Next();

		auto sig = conn.Bind(stmt);
		REQUIRE(sig.output.GetFieldCount() == 2);
		REQUIRE(sig.output.GetFieldName(0) == "id");
		REQUIRE(sig.output.GetFieldType(0) == LogicalType::INTEGER());
		REQUIRE(sig.output.GetFieldName(1) == "score");
		REQUIRE(sig.parameters.GetFieldCount() == 1);
		REQUIRE(sig.parameters.GetFieldName(0) == "1");                    // $1 -> "1"
		REQUIRE(sig.parameters.GetFieldType(0) == LogicalType::INTEGER()); // inferred from score >= $1

		// Execute with one value, then another: different results, same statement, no
		// re-parse and no re-bind.
		auto high = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({50})), 0, 1);
		REQUIRE(high.size() == 3);
		REQUIRE(high[0].first == 2);
		REQUIRE(high[0].second == 55);
		REQUIRE(high[2].first == 4);

		auto higher = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({80})), 0, 1);
		REQUIRE(higher.size() == 1);
		REQUIRE(higher[0].first == 4);
		REQUIRE(higher[0].second == 90);

		// Still alive and re-bindable after executing.
		REQUIRE(static_cast<bool>(stmt));
		REQUIRE(conn.Bind(stmt).parameters.GetFieldCount() == 1);
	}

	SECTION("positional parameters bind in order") {
		auto iter = conn.ParseSQL("SELECT id, score FROM scores WHERE score >= $1 AND score < $2 ORDER BY id");
		auto stmt = iter.Next();
		REQUIRE(conn.Bind(stmt).parameters.GetFieldCount() == 2);

		// $1 = 50, $2 = 80 -> 50 <= score < 80.
		auto rows = Collect2<int32_t, int32_t>(conn.Execute(stmt, Params({50, 80})), 0, 1);
		REQUIRE(rows.size() == 2);
		REQUIRE(rows[0].first == 2);
		REQUIRE(rows[0].second == 55);
		REQUIRE(rows[1].first == 3);
		REQUIRE(rows[1].second == 70);
	}

	SECTION("a prepared DML statement reused to insert rows") {
		conn.Execute("CREATE TABLE log(v INTEGER)").Drain();
		auto iter = conn.ParseSQL("INSERT INTO log VALUES ($1)");
		auto stmt = iter.Next();

		auto sig = conn.Bind(stmt);
		REQUIRE(sig.parameters.GetFieldCount() == 1);
		REQUIRE(sig.output.GetFieldCount() == 1); // the changed-rows count column

		// Each execution inserts one row and reports one changed row.
		REQUIRE(conn.Execute(stmt, Params({10})).Drain() == 1);
		REQUIRE(conn.Execute(stmt, Params({20})).Drain() == 1);
		REQUIRE(conn.Execute(stmt, Params({30})).Drain() == 1);

		auto summary = Collect2<int64_t, int32_t>(conn.Execute("SELECT count(*) AS c, max(v) AS m FROM log"), 0, 1);
		REQUIRE(summary.size() == 1);
		REQUIRE(summary[0].first == 3);   // three rows inserted
		REQUIRE(summary[0].second == 30); // last value
	}
}

TEST_CASE("Stable C++API: PreparedStatement handle", "[cpp_api][prepared_statement]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("CREATE TABLE t(x INTEGER)").Drain();
	conn.Execute("INSERT INTO t VALUES (1),(2),(3),(4)").Drain();

	// Value is move-only, so a parameter list is built by move.
	auto Params = [](std::initializer_list<int64_t> values) {
		std::vector<Value> params;
		for (auto value : values) {
			params.push_back(Value::FromI64(value));
		}
		return params;
	};

	auto Prepare = [&](const std::string &sql, bool require_cacheable = false) {
		auto iter = conn.ParseSQL(sql);
		auto stmt = iter.Next();
		return conn.Prepare(stmt, require_cacheable);
	};

	SECTION("prepare once, execute many with different parameters") {
		auto prepared = Prepare("SELECT x FROM t WHERE x > $1 ORDER BY x");
		auto first = Collect2<int32_t, int32_t>(prepared.Execute(Params({2})), 0, 0);
		REQUIRE(first.size() == 2);
		REQUIRE(first[0].first == 3);
		REQUIRE(first[1].first == 4);
		// Same handle, different value: non-consuming.
		auto second = Collect2<int32_t, int32_t>(prepared.Execute(Params({0})), 0, 0);
		REQUIRE(second.size() == 4);
		REQUIRE(second[0].first == 1);
		REQUIRE(second[3].first == 4);
	}

	SECTION("ReusesPlan reports the honest cache signal") {
		REQUIRE(Prepare("SELECT 42").ReusesPlan() == true);
		REQUIRE(Prepare("SELECT $1::INTEGER + $2::INTEGER").ReusesPlan() == true);
		REQUIRE(Prepare("SELECT $1 + $2").ReusesPlan() == false);
		REQUIRE(Prepare("SELECT * FROM t WHERE x = $1").ReusesPlan() == false);
	}

	SECTION("require_cacheable accepts a cacheable plan and rejects an uncacheable one") {
		// Cacheable: succeeds.
		auto ok = Prepare("SELECT $1::INTEGER + $2::INTEGER", true);
		REQUIRE(ok.ReusesPlan() == true);
		// Uncacheable: throws INVALID_INPUT.
		bool threw = false;
		try {
			Prepare("SELECT * FROM t WHERE x = $1", true);
		} catch (const Exception &ex) {
			threw = true;
			REQUIRE(ex.GetCode() == DUCKDB_V2_ERROR_INVALID_INPUT);
		}
		REQUIRE(threw);
	}

	SECTION("no-parameter execute") {
		auto prepared = Prepare("SELECT x FROM t ORDER BY x");
		auto rows = Collect2<int32_t, int32_t>(prepared.Execute(), 0, 0);
		REQUIRE(rows.size() == 4);
	}

	SECTION("prepared DML reused reports rows changed") {
		conn.Execute("CREATE TABLE log(v INTEGER)").Drain();
		auto prepared = Prepare("INSERT INTO log VALUES ($1)");
		REQUIRE(prepared.Execute(Params({10})).Drain() == 1);
		REQUIRE(prepared.Execute(Params({20})).Drain() == 1);
		auto summary = Collect2<int64_t, int32_t>(conn.Execute("SELECT count(*) AS c, max(v) AS m FROM log"), 0, 1);
		REQUIRE(summary[0].first == 2);
		REQUIRE(summary[0].second == 20);
	}
}
