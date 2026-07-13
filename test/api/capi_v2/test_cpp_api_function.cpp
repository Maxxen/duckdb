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
// Stable C++ API tests: the scalar, aggregate, copy, and cast function builders.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Basic", "[cpp_api]") {
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

			    auto lhs = lhs_vec.GetView();
			    auto rhs = rhs_vec.GetView();
			    auto lhs_data = lhs.Data<int32_t>();
			    auto rhs_data = rhs.Data<int32_t>();
			    auto out_data = out_vec.GetDataMutable<int32_t>();

			    for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
				    out_data[i] = lhs_data[lhs.SelAt(i)] + rhs_data[rhs.SelAt(i)];
			    }
		    })
		    .Register(ctx);
	});

	auto result = conn.Execute("SELECT MyFunction(1, 2) AS result");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "result");

	auto chunk = result.FetchChunk();
	REQUIRE(chunk);

	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	REQUIRE(view.Data<int32_t>()[view.SelAt(0)] == 3);
}
namespace {

// Single-row INTEGER result of `sql`; proves the function's callbacks ran.
int32_t QueryI32(duckdb_api::Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	return view.Data<int32_t>()[view.SelAt(0)];
}

} // namespace
TEST_CASE("Stable C++API: Scalar Function user data", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Set once; visible in bind, init and exec.
		ScalarFunction function(ctx);
		function.SetName("scalar_ud")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<std::string>("scalar user data")
		    .SetBindCallback([](ScalarFunction::BindInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "scalar user data");
		    })
		    .SetInitCallback([](ScalarFunction::InitInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "scalar user data");
		    })
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "scalar user data");
			    auto out = input.GetResultVector().GetDataMutable<int32_t>();
			    const auto count = input.GetInputChunk().GetRowCount();
			    for (idx_t i = 0; i < count; i++) {
				    out[i] = 1;
			    }
		    })
		    .Register(ctx);

		// Never set: GetUserData and GetBindData throw clear errors.
		ScalarFunction none(ctx);
		none.SetName("scalar_ud_none")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    REQUIRE_THROWS_MATCHES(input.GetBindData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    auto out = input.GetResultVector().GetDataMutable<int32_t>();
			    const auto count = input.GetInputChunk().GetRowCount();
			    for (idx_t i = 0; i < count; i++) {
				    out[i] = 2;
			    }
		    })
		    .Register(ctx);
	});

	REQUIRE(QueryI32(conn, "SELECT scalar_ud(1)") == 1);
	REQUIRE(QueryI32(conn, "SELECT scalar_ud_none(1)") == 2);
}
TEST_CASE("Stable C++API: Scalar SetUserData is consumed by Register", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		ScalarFunction function(ctx);
		function.SetName("scalar_ud_consume")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<int>(7)
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    auto out = input.GetResultVector().GetDataMutable<int32_t>();
			    const auto count = input.GetInputChunk().GetRowCount();
			    for (idx_t i = 0; i < count; i++) {
				    out[i] = input.GetUserData<int>();
			    }
		    })
		    .Register(ctx);

		// Register consumed the user data: the second registration has none.
		function.SetName("scalar_ud_consume2")
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    auto out = input.GetResultVector().GetDataMutable<int32_t>();
			    const auto count = input.GetInputChunk().GetRowCount();
			    for (idx_t i = 0; i < count; i++) {
				    out[i] = 0;
			    }
		    })
		    .Register(ctx);
	});

	REQUIRE(QueryI32(conn, "SELECT scalar_ud_consume(1)") == 7);
	REQUIRE(QueryI32(conn, "SELECT scalar_ud_consume2(1)") == 0);
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

			    const auto count = input.GetStateCount();
			    const auto array = input.GetStateArray<int32_t>();
			    const auto view = vector.GetView();
			    const auto vdata = view.Data<int32_t>();

			    for (idx_t i = 0; i < count; i++) {
				    *array[i] += vdata[view.SelAt(i)];
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

	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	REQUIRE(view.Data<int32_t>()[view.SelAt(0)] == 12); // (1 + 2 + 3) * 2
}
namespace {

// Plain int32 sum callbacks, shared by the aggregate user-data tests.
void AggSumUpdate(duckdb_api::AggregateFunction::UpdateInput &input) {
	auto vector = input.GetInputChunk().GetVector(0);
	const auto view = vector.GetView();
	const auto data = view.Data<int32_t>();
	const auto array = input.GetStateArray<int32_t>();
	for (duckdb_api::idx_t i = 0; i < input.GetStateCount(); i++) {
		*array[i] += data[view.SelAt(i)];
	}
}
void AggSumCombine(duckdb_api::AggregateFunction::CombineInput &input) {
	const auto source = input.GetSourceStateArray<int32_t>();
	const auto target = input.GetTargetStateArray<int32_t>();
	for (duckdb_api::idx_t i = 0; i < input.GetStateCount(); i++) {
		*target[i] += *source[i];
	}
}
void AggSumFinalize(duckdb_api::AggregateFunction::FinalizeInput &input) {
	const auto array = input.GetStateArray<int32_t>();
	const auto result = input.GetResultVector().GetDataMutable<int32_t>();
	for (duckdb_api::idx_t i = 0; i < input.GetStateCount(); i++) {
		result[input.GetResultOffset() + i] = *array[i];
	}
}

} // namespace
TEST_CASE("Stable C++API: Aggregate Function user data", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Set once; asserted in the phases a serial aggregation always runs
		// (size, initialize, update, finalize). Combine and destroy carry the
		// same accessor but only run under parallel aggregation / state
		// cleanup, so they are not asserted here.
		AggregateFunction aggregate(ctx);
		aggregate.SetName("agg_ud")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<std::string>("aggregate user data")
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "aggregate user data");
			    input.Reserve<int32_t>();
		    })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "aggregate user data");
			    input.Initialize<int32_t>(0);
		    })
		    .SetUpdateCallback([](AggregateFunction::UpdateInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "aggregate user data");
			    AggSumUpdate(input);
		    })
		    .SetCombineCallback(AggSumCombine)
		    .SetFinalizeCallback([](AggregateFunction::FinalizeInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "aggregate user data");
			    AggSumFinalize(input);
		    })
		    .Register(ctx);

		// Never set: GetUserData throws a clear error.
		AggregateFunction none(ctx);
		none.SetName("agg_ud_none")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    input.Reserve<int32_t>();
		    })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) { input.Initialize<int32_t>(0); })
		    .SetUpdateCallback([](AggregateFunction::UpdateInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    AggSumUpdate(input);
		    })
		    .SetCombineCallback(AggSumCombine)
		    .SetFinalizeCallback(AggSumFinalize)
		    .Register(ctx);
	});

	// The sums round-trip, proving the callbacks (and their REQUIREs) ran.
	REQUIRE(QueryI32(conn, "SELECT agg_ud(i) FROM (VALUES (1), (2), (3)) t(i)") == 6);
	REQUIRE(QueryI32(conn, "SELECT agg_ud_none(i) FROM (VALUES (1), (2), (3)) t(i)") == 6);
}
TEST_CASE("Stable C++API: Aggregate SetUserData is consumed by Register", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		AggregateFunction aggregate(ctx);
		aggregate.SetName("agg_ud_consume")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<int>(7)
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) {
			    REQUIRE(input.GetUserData<int>() == 7);
			    input.Reserve<int32_t>();
		    })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) { input.Initialize<int32_t>(0); })
		    .SetUpdateCallback(AggSumUpdate)
		    .SetCombineCallback(AggSumCombine)
		    .SetFinalizeCallback(AggSumFinalize)
		    .Register(ctx);

		// Register consumed the user data: the second registration has none.
		aggregate.SetName("agg_ud_consume2")
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    input.Reserve<int32_t>();
		    })
		    .Register(ctx);
	});

	REQUIRE(QueryI32(conn, "SELECT agg_ud_consume(i) FROM (VALUES (1), (2), (3)) t(i)") == 6);
	REQUIRE(QueryI32(conn, "SELECT agg_ud_consume2(i) FROM (VALUES (1), (2), (3)) t(i)") == 6);
}
TEST_CASE("Stable C++API: Aggregate Function bind data", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Bind data (a multiplier) set at bind, read in update and finalize.
		AggregateFunction aggregate(ctx);
		aggregate.SetName("agg_bind")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetBindCallback([](AggregateFunction::BindInput &input) {
			    input.SetBindData<int32_t>(10);
			    REQUIRE(input.GetBindData<int32_t>() == 10);
		    })
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) { input.Reserve<int32_t>(); })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) { input.Initialize<int32_t>(0); })
		    .SetUpdateCallback([](AggregateFunction::UpdateInput &input) {
			    REQUIRE(input.GetBindData<int32_t>() == 10);
			    AggSumUpdate(input);
		    })
		    .SetCombineCallback(AggSumCombine)
		    .SetFinalizeCallback([](AggregateFunction::FinalizeInput &input) {
			    const auto multiplier = input.GetBindData<int32_t>();
			    const auto array = input.GetStateArray<int32_t>();
			    const auto result = input.GetResultVector().GetDataMutable<int32_t>();
			    for (idx_t i = 0; i < input.GetStateCount(); i++) {
				    result[input.GetResultOffset() + i] = *array[i] * multiplier;
			    }
		    })
		    .Register(ctx);

		// No bind callback: phase GetBindData throws a clear error.
		AggregateFunction none(ctx);
		none.SetName("agg_bind_none")
		    .AddParameter("a", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetSizeCallback([](AggregateFunction::SizeInput &input) { input.Reserve<int32_t>(); })
		    .SetInitializeCallback([](AggregateFunction::InitializeInput &input) { input.Initialize<int32_t>(0); })
		    .SetUpdateCallback([](AggregateFunction::UpdateInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetBindData<int32_t>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			    AggSumUpdate(input);
		    })
		    .SetCombineCallback(AggSumCombine)
		    .SetFinalizeCallback(AggSumFinalize)
		    .Register(ctx);
	});

	// (1 + 2 + 3) * 10; the multiplier travelled through the bind data.
	REQUIRE(QueryI32(conn, "SELECT agg_bind(i) FROM (VALUES (1), (2), (3)) t(i)") == 60);
	REQUIRE(QueryI32(conn, "SELECT agg_bind_none(i) FROM (VALUES (1), (2), (3)) t(i)") == 6);
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
TEST_CASE("Stable C++API: Copy Function user data", "[cpp_api]") {
	using namespace duckdb_api;

	const auto out_path = duckdb::TestCreatePath("cpp_api_copy_ud.txt");

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Set once; visible in all five phases.
		CopyFunction copy_function(ctx);
		copy_function.SetName("my_copy_ud")
		    .SetUserData<std::string>("copy user data")
		    .SetBindCallback(
		        [](CopyFunction::BindInput &input) { REQUIRE(input.GetUserData<std::string>() == "copy user data"); })
		    .SetInitCallback(
		        [](CopyFunction::InitInput &input) { REQUIRE(input.GetUserData<std::string>() == "copy user data"); })
		    .SetBatchCallback(
		        [](CopyFunction::BatchInput &input) { REQUIRE(input.GetUserData<std::string>() == "copy user data"); })
		    .SetFlushCallback(
		        [](CopyFunction::FlushInput &input) { REQUIRE(input.GetUserData<std::string>() == "copy user data"); })
		    .SetFinalizeCallback([](CopyFunction::FinalizeInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "copy user data");
		    })
		    .Register(ctx);

		// Never set: GetUserData throws a clear error.
		CopyFunction none(ctx);
		none.SetName("my_copy_ud_none")
		    .SetBindCallback([](CopyFunction::BindInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		    })
		    .SetInitCallback([](CopyFunction::InitInput &input) {
			    // Bind data never set: a clear error, not a null deref.
			    REQUIRE_THROWS_MATCHES(input.GetBindData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		    })
		    .SetBatchCallback([](CopyFunction::BatchInput &) {})
		    .SetFlushCallback([](CopyFunction::FlushInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		    })
		    .SetFinalizeCallback([](CopyFunction::FinalizeInput &) {})
		    .Register(ctx);
	});

	conn.Execute("COPY (SELECT i FROM range(3) t(i)) TO '" + out_path + "' (FORMAT my_copy_ud, USE_TMP_FILE FALSE)")
	    .Drain();
	conn.Execute("COPY (SELECT i FROM range(3) t(i)) TO '" + out_path +
	             "' (FORMAT my_copy_ud_none, USE_TMP_FILE FALSE)")
	    .Drain();
}
TEST_CASE("Stable C++API: Copy SetUserData is consumed by Register", "[cpp_api]") {
	using namespace duckdb_api;

	const auto out_path = duckdb::TestCreatePath("cpp_api_copy_ud_consume.txt");

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		CopyFunction copy_function(ctx);
		copy_function.SetName("my_copy_ud_consume")
		    .SetUserData<int>(7)
		    .SetBindCallback([](CopyFunction::BindInput &input) { REQUIRE(input.GetUserData<int>() == 7); })
		    .SetInitCallback([](CopyFunction::InitInput &) {})
		    .SetBatchCallback([](CopyFunction::BatchInput &) {})
		    .SetFlushCallback([](CopyFunction::FlushInput &) {})
		    .SetFinalizeCallback([](CopyFunction::FinalizeInput &) {})
		    .Register(ctx);

		// Register consumed the user data: the second registration has none.
		copy_function.SetName("my_copy_ud_consume2")
		    .SetBindCallback([](CopyFunction::BindInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		    })
		    .Register(ctx);
	});

	conn.Execute("COPY (SELECT i FROM range(3) t(i)) TO '" + out_path +
	             "' (FORMAT my_copy_ud_consume, USE_TMP_FILE FALSE)")
	    .Drain();
	conn.Execute("COPY (SELECT i FROM range(3) t(i)) TO '" + out_path +
	             "' (FORMAT my_copy_ud_consume2, USE_TMP_FILE FALSE)")
	    .Drain();
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
			    // The bridge passes the input vector as-is; the view reads it
			    // whether it arrived flat, constant or dictionary-encoded.
			    const auto view = in_vec.GetView();
			    const auto in = view.Data<int32_t>();
			    const auto out = out_vec.GetDataMutable<int64_t>();
			    for (idx_t i = 0; i < input.GetCount(); i++) {
				    out[i] = static_cast<int64_t>(in[view.SelAt(i)]) + 1000;
			    }
		    })
		    .Register(ctx);
	});

	auto result = conn.Execute("SELECT CAST(CAST(42 AS TEMPERATURE) AS BIGINT) AS result");
	REQUIRE(result.GetSchema().GetFieldCount() == 1);
	REQUIRE(result.GetSchema().GetFieldName(0) == "result");
	REQUIRE(result.GetSchema().GetFieldType(0) == LogicalType::BIGINT());

	auto chunk = result.FetchChunk();
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(chunk.GetRowCount() == 1);
	REQUIRE(view.IsValid(0));
	REQUIRE(view.Data<int64_t>()[view.SelAt(0)] == 1042);
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

	// Counts the rows the exec callback processes, to observe whether the
	// optimizer constant-folded the function. Handed to each function via
	// SetUserData, so the test needs no file-scope state.
	std::atomic<idx_t> exec_rows {0};

	// An exec callback that counts the rows it processes and writes a constant.
	auto exec = [](ScalarFunction::ExecInput &input) {
		auto chunk = input.GetInputChunk();
		auto out = input.GetResultVector().GetDataMutable<int32_t>();
		auto count = chunk.GetRowCount();
		for (idx_t i = 0; i < count; i++) {
			out[i] = 0;
		}
		*input.GetUserData<std::atomic<idx_t> *>() += count;
	};

	conn.WithTransaction([&](const Context &ctx) {
		// Default stability (CONSISTENT): foldable when its argument is constant.
		ScalarFunction consistent(ctx);
		consistent.SetName("prop_consistent")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<std::atomic<idx_t> *>(&exec_rows)
		    .SetExecCallback(exec)
		    .Register(ctx);

		// Same function, but VOLATILE: must be evaluated for every row.
		ScalarFunction vol(ctx);
		vol.SetName("prop_volatile")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetStability(FunctionStability::Volatile)
		    .SetUserData<std::atomic<idx_t> *>(&exec_rows)
		    .SetExecCallback(exec)
		    .Register(ctx);
	});

	auto drain = [&](const char *sql) -> idx_t {
		exec_rows = 0;
		auto result = conn.Execute(sql);
		while (auto chunk = result.FetchChunk()) {
		}
		return exec_rows.load();
	};

	// With a constant argument the consistent function is folded to a single
	// evaluation, while the volatile one runs for all 1000 rows.
	auto consistent_rows = drain("SELECT prop_consistent(42) FROM range(1000)");
	auto volatile_rows = drain("SELECT prop_volatile(42) FROM range(1000)");

	REQUIRE(volatile_rows == 1000);
	REQUIRE(consistent_rows < volatile_rows);
}

// ---------------------------------------------------------------------------
// The execution context on scalar exec.
//
// ScalarFunction::ExecInput exposes the client context when the invocation
// runs under one (HasContext()/GetContext()); context-free invocations report
// no context and GetContext() throws rather than returning a stale pointer.
// ---------------------------------------------------------------------------
namespace {
struct ScalarExecContextProbe {
	std::atomic<idx_t> with_context {0};
	std::atomic<idx_t> without_context {0};
	std::atomic<idx_t> context_used_ok {0};
};
} // namespace

TEST_CASE("Stable C++API: Scalar exec sees the execution context", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	ScalarExecContextProbe probe;

	auto exec = [](ScalarFunction::ExecInput &input) {
		auto &p = *input.GetUserData<ScalarExecContextProbe *>();
		auto out = input.GetResultVector().GetDataMutable<int32_t>();
		const auto count = input.GetInputChunk().GetRowCount();
		if (input.HasContext()) {
			// The pointer must be LIVE, not merely non-null: parsing a type
			// dereferences the ClientContext (parser + catalog).
			auto parsed = input.GetContext().ParseType("DECIMAL(10,2)");
			if (parsed.GetId() == TypeId::DECIMAL) {
				p.context_used_ok += 1;
			}
			p.with_context += (count == 0 ? 1 : count);
		} else {
			// A context-free invocation reports no context and GetContext()
			// throws rather than handing back a dangling/garbage pointer.
			REQUIRE_THROWS_MATCHES(input.GetContext(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			p.without_context += (count == 0 ? 1 : count);
		}
		for (idx_t i = 0; i < count; i++) {
			out[i] = 0;
		}
	};

	conn.WithTransaction([&](const Context &ctx) {
		ScalarFunction f(ctx);
		f.SetName("exec_ctx_probe")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<ScalarExecContextProbe *>(&probe)
		    .SetExecCallback(exec)
		    .Register(ctx);
	});

	auto reset = [&]() {
		probe.with_context = 0;
		probe.without_context = 0;
		probe.context_used_ok = 0;
	};
	auto drain = [&](const char *sql) {
		auto r = conn.Execute(sql);
		while (auto c = r.FetchChunk()) {
		}
	};

	SECTION("real query over a table: context present and live") {
		reset();
		drain("SELECT exec_ctx_probe(x) FROM (VALUES (1),(2),(3)) t(x)");
		REQUIRE(probe.with_context.load() == 3);
		REQUIRE(probe.without_context.load() == 0);
		REQUIRE(probe.context_used_ok.load() >= 1); // the context was actually USED
	}

	SECTION("optimizer constant folding still carries a context") {
		// The optimizer's ConstantFoldingRule evaluates the folded expression
		// with the client context (TryEvaluateScalar(GetContext(), ...)), so
		// HasContext() is TRUE even here. The genuine context-free path is the
		// index expression below, NOT constant folding.
		reset();
		drain("SELECT exec_ctx_probe(42)");
		REQUIRE(probe.with_context.load() >= 1);
		REQUIRE(probe.without_context.load() == 0);
		REQUIRE(probe.context_used_ok.load() >= 1);
	}

	SECTION("context-free invocation: index expression") {
		// Index-expression evaluation runs through a context-free
		// ExpressionExecutor (BoundIndex owns one built with the default
		// constructor), so exec sees no context.
		reset();
		drain("CREATE TABLE exec_ctx_tbl(x INTEGER)");
		drain("CREATE INDEX exec_ctx_idx ON exec_ctx_tbl(exec_ctx_probe(x))");
		drain("INSERT INTO exec_ctx_tbl VALUES (7),(8),(9)");
		REQUIRE(probe.without_context.load() == 3);
		REQUIRE(probe.with_context.load() == 0);
		REQUIRE(probe.context_used_ok.load() == 0);
	}
}

// ---------------------------------------------------------------------------
// The init context is nullable too, delivered as a positional callback param.
//
// A scalar function's init callback runs on whatever ExpressionExecutor
// initializes the expression state tree. That executor is context-free for an
// index expression (BoundIndex owns a default-constructed executor), so init
// must tolerate a null context exactly as exec does. Before context became a
// nullable positional param, the bridge dereferenced the context unconditionally
// while building init's args, so a function with an init callback threw
// "Cannot use <fn> in this context" at index-build time.
// ---------------------------------------------------------------------------
namespace {
struct ScalarInitContextProbe {
	std::atomic<idx_t> init_with_context {0};
	std::atomic<idx_t> init_without_context {0};
	std::atomic<idx_t> init_context_used_ok {0};
};
} // namespace

TEST_CASE("Stable C++API: Scalar init sees a nullable execution context", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	ScalarInitContextProbe probe;

	auto init = [](ScalarFunction::InitInput &input) {
		auto &p = *input.GetUserData<ScalarInitContextProbe *>();
		if (input.HasContext()) {
			// The pointer must be LIVE, not merely non-null: parsing a type
			// dereferences the ClientContext (parser + catalog).
			auto parsed = input.GetContext().ParseType("DECIMAL(10,2)");
			if (parsed.GetId() == TypeId::DECIMAL) {
				p.init_context_used_ok += 1;
			}
			p.init_with_context += 1;
		} else {
			// A context-free invocation reports no context and GetContext()
			// throws rather than handing back a dangling/garbage pointer.
			REQUIRE_THROWS_MATCHES(input.GetContext(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
			p.init_without_context += 1;
		}
	};

	auto exec = [](ScalarFunction::ExecInput &input) {
		auto out = input.GetResultVector().GetDataMutable<int32_t>();
		const auto count = input.GetInputChunk().GetRowCount();
		for (idx_t i = 0; i < count; i++) {
			out[i] = 0;
		}
	};

	conn.WithTransaction([&](const Context &ctx) {
		ScalarFunction f(ctx);
		f.SetName("init_ctx_fn")
		    .AddParameter("x", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetUserData<ScalarInitContextProbe *>(&probe)
		    .SetInitCallback(init)
		    .SetExecCallback(exec)
		    .Register(ctx);
	});

	auto reset = [&]() {
		probe.init_with_context = 0;
		probe.init_without_context = 0;
		probe.init_context_used_ok = 0;
	};
	auto drain = [&](const char *sql) {
		auto r = conn.Execute(sql);
		while (auto c = r.FetchChunk()) {
		}
	};

	SECTION("real query: init sees a live context") {
		reset();
		drain("SELECT init_ctx_fn(x) FROM (VALUES (1),(2),(3)) t(x)");
		REQUIRE(probe.init_with_context.load() >= 1);
		REQUIRE(probe.init_without_context.load() == 0);
		REQUIRE(probe.init_context_used_ok.load() >= 1); // the context was actually USED
	}

	SECTION("index expression: init runs context-free without throwing") {
		reset();
		drain("CREATE TABLE init_tbl(x INTEGER)");
		// The persistent index owns a context-free ExpressionExecutor, so it
		// initializes the function's state with no context at least once. Before
		// the fix this threw "Cannot use <fn> in this context" and the statement
		// failed (drain would throw). CREATE INDEX also runs a context-bearing
		// executor to populate the index, so init may additionally be invoked
		// with a context here; what this pins is that the context-free path no
		// longer throws.
		drain("CREATE INDEX init_idx ON init_tbl(init_ctx_fn(x))");
		drain("INSERT INTO init_tbl VALUES (7),(8),(9)");
		REQUIRE(probe.init_without_context.load() >= 1);
	}
}

// ---------------------------------------------------------------------------
// A scalar function whose return type is DECIMAL registers and runs end to end.
// This relies on the engine reporting a well-formed DECIMAL as a complete type;
// when it did not, Register rejected the return type as not "fully defined".
// ---------------------------------------------------------------------------
TEST_CASE("Stable C++API: scalar function with a DECIMAL return type", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		// Before the IsComplete fix, Register threw "Return type must be a
		// fully defined concrete type" here.
		ScalarFunction f(ctx);
		f.SetName("decimal_ret")
		    .AddParameter("x", LogicalType::BIGINT())
		    .SetReturnType(ctx.ParseType("DECIMAL(18,3)"))
		    .SetExecCallback([](ScalarFunction::ExecInput &input) {
			    // DECIMAL(18,3) is INT64-backed: value x is written as x.000.
			    auto out = input.GetResultVector().GetDataMutable<int64_t>();
			    auto in = input.GetInputChunk().GetVector(0).GetView();
			    const auto count = input.GetInputChunk().GetRowCount();
			    for (idx_t i = 0; i < count; i++) {
				    out[i] = in.Data<int64_t>()[in.SelAt(i)] * 1000;
			    }
		    })
		    .Register(ctx);
	});

	{
		// Registration and a query through the DECIMAL(18,3) return succeed.
		auto result = conn.Execute("SELECT decimal_ret(2) AS d");
		REQUIRE(result.GetSchema().GetFieldCount() == 1);
		REQUIRE(result.GetSchema().GetFieldType(0).GetId() == TypeId::DECIMAL);

		auto chunk = result.FetchChunk();
		REQUIRE(chunk);
		auto view = chunk.GetVector(0).GetView();
		REQUIRE(view.IsValid(0));
		REQUIRE(view.Data<int64_t>()[view.SelAt(0)] == 2000); // 2.000 scaled by 10^3
		while (result.FetchChunk()) {                         // drain: free the connection
		}
	}

	// The rendered value confirms the scale is honored end to end.
	auto text_result = conn.Execute("SELECT decimal_ret(2)::VARCHAR AS d");
	auto text_chunk = text_result.FetchChunk();
	REQUIRE(text_chunk);
	auto text_view = text_chunk.GetVector(0).GetView();
	REQUIRE(text_view.IsValid(0));
	REQUIRE(text_view.Data<StringLayout>()[text_view.SelAt(0)].AsStringView() == "2.000");
}
