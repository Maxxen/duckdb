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
// Stable C++ API tests: the table function builder.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Table Function", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;

	auto db = env.Open(":memory:");

	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction table_function(ctx);

		table_function
		    .SetName("MyRangeFunction")
		    // start and stop are required positional parameters; step is an optional
		    // named parameter defaulting to 1 (injected into bind when the call omits it).
		    .SetSignature(FunctionSignature::Create()
		                      .AddParameter("start", LogicalType::INTEGER())
		                      .AddParameter("stop", LogicalType::INTEGER())
		                      .AddParameterDefault("step", LogicalType::INTEGER(), Value::Bigint(1)))
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // We will emit one column named "i" of type INTEGER
			    input.AddResultColumn("i", LogicalType::INTEGER());

			    // Get parameters
			    const auto start = input.GetParameter(0).AsInteger();
			    const auto stop = input.GetParameter(1).AsInteger();

			    // "Step" is optional and named, so we try to get it and default to 1 if it's not provided
			    int32_t step = 1;
			    if (const auto step_arg = input.TryGetNamedParameter("step")) {
				    step = step_arg->AsInteger();
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

	auto view = chunk.GetVector(0).GetView();
	auto data = view.Data<int32_t>();
	REQUIRE(chunk.GetRowCount() == 5);
	for (idx_t i = 0; i < 5; i++) {
		REQUIRE(view.IsValid(i));
		REQUIRE(data[view.SelAt(i)] == static_cast<int32_t>(i * 2));
	}
}
// The table-function callbacks are captureless function pointers, so the
// fixtures they touch live at file scope.
namespace {

// Collect one BIGINT column; every row asserted valid.
std::vector<int64_t> CollectI64(duckdb_api::QueryResult result) {
	std::vector<int64_t> rows;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (duckdb_api::idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			rows.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return rows;
}

// User data of the arrow_roundtrip function: the test-produced Arrow schema
// and batches, adopted at registration (SetUserData) and freed with the
// function at engine teardown.
struct ArrowRoundtripState {
	ArrowSchema schema {};
	std::vector<ArrowArray> arrays;
	std::unique_ptr<duckdb_api::ArrowConversionPlan> plan;
	duckdb_api::idx_t next = 0;

	// Adopts the schema (Arrow move: the source's release transfers here).
	ArrowRoundtripState(ArrowSchema &schema_p, std::vector<ArrowArray> &&arrays_p)
	    : schema(schema_p), arrays(std::move(arrays_p)) {
		schema_p.release = nullptr;
	}
	ArrowRoundtripState(const ArrowRoundtripState &) = delete;
	ArrowRoundtripState &operator=(const ArrowRoundtripState &) = delete;
	~ArrowRoundtripState() {
		if (schema.release) {
			schema.release(&schema);
		}
		for (auto &array : arrays) {
			if (array.release) {
				array.release(&array);
			}
		}
	}
};

// Bind data of the pushdown function: what the pushdown callback claimed.
struct PushdownCapture {
	bool handled = false;
	int64_t captured = 0;

	bool operator==(const PushdownCapture &other) const {
		return handled == other.handled && captured == other.captured;
	}
};

// What the expression-walk pushdown callback observed.
struct WalkObservations {
	duckdb_api::idx_t filter_count = 0;
	bool saw_greater_than = false;
	bool saw_equal = false;
};
WalkObservations g_walk;

// Whether the bind-data-less pushdown callback ran.
bool g_nobind_pushdown_ran = false;

// The projected columns each init callback observed.
struct ProjectionObservations {
	std::vector<duckdb_api::idx_t> global_columns;
	std::vector<duckdb_api::idx_t> local_columns;
};
ProjectionObservations g_proj;

} // namespace
TEST_CASE("Stable C++API: Arrow round-trip through a table function", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Source rows with a NULL BIGINT (i=2), a NULL VARCHAR (i=3), and
	// heap-backed strings (14 bytes > the 12-byte inline cutoff).
	const std::string source_sql = "SELECT NULLIF(i, 2)::BIGINT AS a, "
	                               "CASE WHEN i = 3 THEN NULL ELSE 'str_' || repeat(i::VARCHAR, 10) END AS s "
	                               "FROM range(5) t(i)";

	// The bound Schema drives ToArrowSchema; the fetched chunk drives ToArrowArray.
	auto iter = conn.ParseSQL(source_sql);
	auto stmt = iter.Next();
	auto schema = std::move(conn.Bind(stmt).output);

	auto source = conn.Execute(stmt);
	auto chunk = source.FetchChunk();
	REQUIRE(chunk);
	REQUIRE(chunk.GetRowCount() == 5);
	REQUIRE_FALSE(source.FetchChunk()); // drain: frees the connection

	// Export the schema once and the same chunk twice (two Arrow batches).
	ArrowSchema arrow_schema {};
	std::vector<ArrowArray> arrays(2);
	conn.WithTransaction([&](const Context &ctx) {
		schema.ToArrowSchema(ctx, arrow_schema);
		chunk.ToArrowArray(ctx, arrays[0]);
		chunk.ToArrowArray(ctx, arrays[1]);
	});
	REQUIRE(arrow_schema.release != nullptr);
	REQUIRE(arrow_schema.n_children == 2);
	REQUIRE(arrays[0].length == 5);

	conn.WithTransaction([&](const Context &ctx) {
		TableFunction function(ctx);
		function
		    .SetName("arrow_roundtrip")
		    // The test-produced Arrow data rides the function's user data: no
		    // file-scope state.
		    .SetUserData<ArrowRoundtripState>(arrow_schema, std::move(arrays))
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // Bind-time conversion of the test-produced Arrow schema. The
			    // result columns are derived from it, not hardcoded: the real
			    // client flow for an arbitrary ArrowSchema.
			    auto &state = input.GetUserData<ArrowRoundtripState>();
			    state.plan = std::make_unique<ArrowConversionPlan>(input.GetContext(), state.schema);
			    input.AddResultColumns(state.plan->GetSchema());
		    })
		    .SetInitGlobalCallback(
		        [](TableFunction::InitGlobalInput &input) { input.GetUserData<ArrowRoundtripState>().next = 0; })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &state = input.GetUserData<ArrowRoundtripState>();
			    auto &out = input.GetResultChunk();
			    if (state.next >= state.arrays.size()) {
				    out.GetVector(0).SetSize(0);
				    return;
			    }
			    // Exec-time conversion under the execution context.
			    auto imported = state.plan->Convert(input.GetContext(), state.arrays[state.next++]);
			    const auto rows = imported.GetRowCount();

			    auto in_a = imported.GetVector(0).GetView();
			    auto out_a = out.GetVector(0);
			    auto *a_data = out_a.GetDataMutable<int64_t>();
			    auto a_validity = out_a.GetValidityMutable();
			    for (idx_t i = 0; i < rows; i++) {
				    if (in_a.IsValid(i)) {
					    a_data[i] = in_a.Data<int64_t>()[in_a.SelAt(i)];
				    } else {
					    a_validity.SetInvalid(i);
				    }
			    }

			    auto in_s = imported.GetVector(1).GetView();
			    auto out_s = out.GetVector(1);
			    auto s_validity = out_s.GetValidityMutable();
			    for (idx_t i = 0; i < rows; i++) {
				    if (in_s.IsValid(i)) {
					    out_s.AssignString(i, in_s.Data<StringLayout>()[in_s.SelAt(i)].AsStringView());
				    } else {
					    s_validity.SetInvalid(i);
				    }
			    }
			    out.GetVector(0).SetSize(rows);
		    })
		    .Register(ctx);
	});

	// Two batches of the same 5 source rows. The result schema comes from
	// GetSchema + AddResultColumns, so names and types prove that path.
	auto result = conn.Execute("SELECT a, s FROM arrow_roundtrip()");
	auto out_schema = result.GetSchema();
	REQUIRE(out_schema.GetFieldCount() == 2);
	REQUIRE(out_schema.GetFieldName(0) == "a");
	REQUIRE(out_schema.GetFieldName(1) == "s");
	REQUIRE(out_schema.GetFieldType(0) == LogicalType::BIGINT());
	REQUIRE(out_schema.GetFieldType(1) == LogicalType::VARCHAR());
	idx_t row = 0;
	while (auto out = result.FetchChunk()) {
		auto va = out.GetVector(0).GetView();
		auto vs = out.GetVector(1).GetView();
		for (idx_t i = 0; i < out.GetRowCount(); i++, row++) {
			const auto src = row % 5;
			if (src == 2) {
				REQUIRE_FALSE(va.IsValid(i));
			} else {
				REQUIRE(va.IsValid(i));
				REQUIRE(va.Data<int64_t>()[va.SelAt(i)] == static_cast<int64_t>(src));
			}
			if (src == 3) {
				REQUIRE_FALSE(vs.IsValid(i));
			} else {
				REQUIRE(vs.IsValid(i));
				const auto expected = "str_" + std::string(10, static_cast<char>('0' + src));
				REQUIRE(vs.Data<StringLayout>()[vs.SelAt(i)].AsStringView() == expected);
			}
		}
	}
	REQUIRE(row == 10);

	// The state adopted the schema, so the local struct has nothing to free;
	// the imports consumed the arrays and the function's user data (schema
	// included) is freed at engine teardown.
	REQUIRE(arrow_schema.release == nullptr);
}
TEST_CASE("Stable C++API: Table Function complex filter pushdown", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("pushdown_fn")
		    .SetUserData<std::string>("pushdown user data")
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // The registration's user data is visible in bind and pushdown.
			    REQUIRE(input.GetUserData<std::string>() == "pushdown user data");
			    input.AddResultColumn("v", LogicalType::BIGINT());
			    input.SetBindData<PushdownCapture>();
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) { input.SetGlobalState<bool>(false); })
		    .SetPushdownComplexFilterCallback([](TableFunction::PushdownInput &input) {
			    REQUIRE(input.GetUserData<std::string>() == "pushdown user data");
			    auto &capture = input.GetBindData<PushdownCapture>();
			    for (idx_t i = 0; i < input.GetCount(); i++) {
				    auto expr = input.GetExpression(i);
				    if (expr.GetType() != ExpressionType::CompareGreaterThan) {
					    continue; // leave every other filter to the engine
				    }
				    // v > K: capture K into bind data and claim the filter.
				    for (idx_t c = 0; c < expr.GetChildCount(); c++) {
					    auto child = expr.GetChild(c);
					    if (child.GetClass() == ExpressionClass::BoundConstant) {
						    capture.captured = child.GetConstantValue().AsBigint();
					    }
				    }
				    capture.handled = true;
				    input.MarkHandled(i);
			    }
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &done = input.GetGlobalState<bool>();
			    auto &out = input.GetResultChunk();
			    auto vec = out.GetVector(0);
			    if (done) {
				    vec.SetSize(0);
				    return;
			    }
			    done = true;
			    auto *data = vec.GetDataMutable<int64_t>();
			    const auto &capture = input.GetBindData<PushdownCapture>();
			    if (!capture.handled) {
				    data[0] = -1; // sentinel: no filter reached the callback
				    vec.SetSize(1);
				    return;
			    }
			    // Emit K (violates v > K, so it survives only if the engine
			    // dropped the claimed filter), K+10, and K+20.
			    data[0] = capture.captured;
			    data[1] = capture.captured + 10;
			    data[2] = capture.captured + 20;
			    vec.SetSize(3);
		    })
		    .Register(ctx);
	});

	SECTION("a claimed filter is dropped by the engine; the rest still apply") {
		auto rows = CollectI64(conn.Execute("SELECT v FROM pushdown_fn() WHERE v > 5 AND v != 15"));
		// 5 survives (the claimed v > 5 was not re-applied); 15 was dropped by
		// the engine-applied v != 15.
		REQUIRE(rows == std::vector<int64_t> {5, 25});
	}

	SECTION("no filter reaches the callback on an unfiltered scan") {
		auto rows = CollectI64(conn.Execute("SELECT v FROM pushdown_fn()"));
		REQUIRE(rows == std::vector<int64_t> {-1});
	}
}
TEST_CASE("Stable C++API: Expression walk in the pushdown callback", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	g_walk = WalkObservations {};

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("walk_fn")
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    input.AddResultColumn("a", LogicalType::BIGINT());
			    input.AddResultColumn("s", LogicalType::VARCHAR());
			    input.SetBindData<int>(0); // the pushdown route runs through bind data
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) { input.SetGlobalState<bool>(false); })
		    .SetPushdownComplexFilterCallback([](TableFunction::PushdownInput &input) {
			    g_walk.filter_count = input.GetCount();
			    for (idx_t i = 0; i < input.GetCount(); i++) {
				    auto expr = input.GetExpression(i);
				    // A comparison is a BoundFunction carrying an internal
				    // symbol; the operator is the type.
				    REQUIRE(expr.GetClass() == ExpressionClass::BoundFunction);
				    REQUIRE_FALSE(expr.GetFunctionName().empty());
				    REQUIRE(expr.GetChildCount() == 2);

				    // Identify the operands by class (either order).
				    std::optional<Expression> column;
				    std::optional<Expression> constant;
				    for (idx_t c = 0; c < 2; c++) {
					    auto child = expr.GetChild(c);
					    if (child.GetClass() == ExpressionClass::BoundColumnRef) {
						    column.emplace(std::move(child));
					    } else if (child.GetClass() == ExpressionClass::BoundConstant) {
						    constant.emplace(std::move(child));
					    }
				    }
				    REQUIRE(column.has_value());
				    REQUIRE(constant.has_value());
				    REQUIRE(column->GetChildCount() == 0);
				    REQUIRE(constant->GetChildCount() == 0);

				    switch (expr.GetType()) {
				    case ExpressionType::CompareGreaterThan: { // a > 5
					    REQUIRE(column->GetColumnBinding().column_index == 0);
					    REQUIRE(column->GetReturnType() == LogicalType::BIGINT());
					    REQUIRE(constant->GetReturnType() == LogicalType::BIGINT());
					    REQUIRE(constant->GetConstantValue().AsBigint() == 5);
					    g_walk.saw_greater_than = true;

					    // Class-mismatch accessors throw INVALID_INPUT.
					    REQUIRE_THROWS_MATCHES(expr.GetConstantValue(), Exception,
					                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
					    REQUIRE_THROWS_MATCHES(expr.GetColumnBinding(), Exception,
					                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
					    // GetReferenceIndex has no reachable happy path here:
					    // BoundRef exists only after physical planning, which
					    // this surface never exposes (pushdown trees carry
					    // BoundColumnRef).
					    REQUIRE_THROWS_MATCHES(expr.GetReferenceIndex(), Exception,
					                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
					    REQUIRE_THROWS_MATCHES(column->GetFunctionName(), Exception,
					                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
					    REQUIRE_THROWS_MATCHES(expr.GetChild(2), Exception,
					                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
					    break;
				    }
				    case ExpressionType::CompareEqual: { // s = 'x'
					    REQUIRE(column->GetColumnBinding().column_index == 1);
					    REQUIRE(column->GetReturnType() == LogicalType::VARCHAR());
					    REQUIRE(constant->GetReturnType() == LogicalType::VARCHAR());
					    REQUIRE(constant->GetConstantValue().AsVarchar() == "x");
					    g_walk.saw_equal = true;
					    break;
				    }
				    default:
					    FAIL("unexpected filter type");
				    }
			    }
			    // Nothing marked handled: the engine applies both filters.
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &done = input.GetGlobalState<bool>();
			    auto &out = input.GetResultChunk();
			    auto a_vec = out.GetVector(0);
			    if (done) {
				    a_vec.SetSize(0);
				    return;
			    }
			    done = true;
			    const int64_t a_values[] = {1, 6, 7};
			    const char *s_values[] = {"x", "x", "y"};
			    auto s_vec = out.GetVector(1);
			    auto *a_data = a_vec.GetDataMutable<int64_t>();
			    for (idx_t i = 0; i < 3; i++) {
				    a_data[i] = a_values[i];
				    s_vec.AssignString(i, s_values[i]);
			    }
			    a_vec.SetSize(3);
		    })
		    .Register(ctx);
	});

	auto rows = CollectI64(conn.Execute("SELECT a FROM walk_fn() WHERE a > 5 AND s = 'x'"));
	REQUIRE(rows == std::vector<int64_t> {6}); // both filters engine-applied

	REQUIRE(g_walk.filter_count == 2);
	REQUIRE(g_walk.saw_greater_than);
	REQUIRE(g_walk.saw_equal);
}
TEST_CASE("Stable C++API: pushdown callback works without bind data", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	g_nobind_pushdown_ran = false;

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("pushdown_nobind_fn")
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // No SetBindData: the pushdown callback needs none. No
			    // SetUserData either: GetUserData throws a clear error.
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
			    input.AddResultColumn("v", LogicalType::BIGINT());
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) { input.SetGlobalState<bool>(false); })
		    .SetPushdownComplexFilterCallback([](TableFunction::PushdownInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
			    // Unset bind data is a clear error, not a null deref.
			    REQUIRE_THROWS_MATCHES(input.GetBindData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
			    // The optimization context is live for the callback duration.
			    REQUIRE(static_cast<bool>(input.GetContext().GetFileSystem()));
			    g_nobind_pushdown_ran = input.GetCount() > 0;
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &done = input.GetGlobalState<bool>();
			    auto vec = input.GetResultChunk().GetVector(0);
			    if (done) {
				    vec.SetSize(0);
				    return;
			    }
			    done = true;
			    auto *data = vec.GetDataMutable<int64_t>();
			    data[0] = 1;
			    data[1] = 2;
			    vec.SetSize(2);
		    })
		    .Register(ctx);
	});

	// The callback ran (claiming nothing) and the engine applied the filter.
	auto rows = CollectI64(conn.Execute("SELECT v FROM pushdown_nobind_fn() WHERE v = 1"));
	REQUIRE(rows == std::vector<int64_t> {1});
	REQUIRE(g_nobind_pushdown_ran);
}
TEST_CASE("Stable C++API: a throw from the pushdown callback surfaces as a query error", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("pushdown_throw_fn")
		    .SetBindCallback([](TableFunction::BindInput &input) { input.AddResultColumn("v", LogicalType::BIGINT()); })
		    .SetPushdownComplexFilterCallback([](TableFunction::PushdownInput &) {
			    throw Exception(DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE, "synthetic pushdown failure");
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) { input.GetResultChunk().GetVector(0).SetSize(0); })
		    .Register(ctx);
	});

	// The guard turns the throw into a callback error; the code round-trips.
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT v FROM pushdown_throw_fn() WHERE v = 1").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE));
}
TEST_CASE("Stable C++API: SetUserData is consumed by Register", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("consume_ud_fn")
		    .SetUserData<int>(7)
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    REQUIRE(input.GetUserData<int>() == 7);
			    input.AddResultColumn("v", LogicalType::BIGINT());
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) { input.GetResultChunk().GetVector(0).SetSize(0); })
		    .Register(ctx);

		// Register consumed the user data: the second registration has none.
		function.SetName("consume_ud_fn2")
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    REQUIRE_THROWS_MATCHES(input.GetUserData<int>(), Exception,
			                           HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
			    input.AddResultColumn("v", LogicalType::BIGINT());
		    })
		    .Register(ctx);
	});

	REQUIRE(CollectI64(conn.Execute("SELECT v FROM consume_ud_fn()")).empty());
	REQUIRE(CollectI64(conn.Execute("SELECT v FROM consume_ud_fn2()")).empty());
}
TEST_CASE("Stable C++API: InitLocalInput reads the global state", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("local_global_fn")
		    .SetBindCallback([](TableFunction::BindInput &input) { input.AddResultColumn("v", LogicalType::BIGINT()); })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) { input.SetGlobalState<int64_t>(41); })
		    .SetInitLocalCallback([](TableFunction::InitLocalInput &input) {
			    // Derive the local state from the shared global state.
			    input.SetLocalState<int64_t>(input.GetGlobalState<int64_t>() + 1);
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &remaining = input.GetGlobalState<int64_t>();
			    auto vec = input.GetResultChunk().GetVector(0);
			    if (remaining == 0) {
				    vec.SetSize(0);
				    return;
			    }
			    remaining = 0;
			    auto *data = vec.GetDataMutable<int64_t>();
			    data[0] = input.GetLocalState<int64_t>();
			    vec.SetSize(1);
		    })
		    .Register(ctx);
	});

	REQUIRE(CollectI64(conn.Execute("SELECT v FROM local_global_fn()")) == std::vector<int64_t> {42});
}
TEST_CASE("Stable C++API: Table Function projection pushdown reports projected columns", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	g_proj = ProjectionObservations {};

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("proj_fn")
		    .SetProjectionPushdown(true)
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    input.AddResultColumn("a", LogicalType::BIGINT());
			    input.AddResultColumn("b", LogicalType::BIGINT());
			    input.AddResultColumn("c", LogicalType::BIGINT());
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) {
			    // The scan context is live for the callback duration.
			    REQUIRE(static_cast<bool>(input.GetContext().GetFileSystem()));
			    g_proj.global_columns.clear();
			    for (idx_t i = 0; i < input.GetColumnCount(); i++) {
				    g_proj.global_columns.push_back(input.GetColumnIndex(i));
			    }
			    input.SetGlobalState<bool>(false);
		    })
		    .SetInitLocalCallback([](TableFunction::InitLocalInput &input) {
			    REQUIRE(static_cast<bool>(input.GetContext().GetFileSystem()));
			    g_proj.local_columns.clear();
			    for (idx_t i = 0; i < input.GetColumnCount(); i++) {
				    g_proj.local_columns.push_back(input.GetColumnIndex(i));
			    }
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &done = input.GetGlobalState<bool>();
			    auto &out = input.GetResultChunk();
			    if (done) {
				    out.GetVector(0).SetSize(0);
				    return;
			    }
			    done = true;
			    // The chunk is sized to the projected columns; each cell encodes
			    // its original column index.
			    REQUIRE(out.GetVectorCount() == g_proj.global_columns.size());
			    for (idx_t col = 0; col < out.GetVectorCount(); col++) {
				    out.GetVector(col).GetDataMutable<int64_t>()[0] =
				        static_cast<int64_t>(g_proj.global_columns[col] * 10);
			    }
			    out.GetVector(0).SetSize(1);
		    })
		    .Register(ctx);
	});

	SECTION("natural order") {
		auto rows = Collect2<int64_t, int64_t>(conn.Execute("SELECT a, c FROM proj_fn()"), 0, 1);
		REQUIRE(rows.size() == 1);
		REQUIRE(rows[0].first == 0);   // original column 0 (a)
		REQUIRE(rows[0].second == 20); // original column 2 (c)

		REQUIRE(g_proj.global_columns == std::vector<idx_t> {0, 2});
		REQUIRE(g_proj.local_columns == std::vector<idx_t> {0, 2});
	}

	SECTION("reordered projection") {
		// The scan's projected order is the engine's choice; the result
		// mapping is what must hold.
		auto rows = Collect2<int64_t, int64_t>(conn.Execute("SELECT c, a FROM proj_fn()"), 0, 1);
		REQUIRE(rows.size() == 1);
		REQUIRE(rows[0].first == 20); // original column 2 (c)
		REQUIRE(rows[0].second == 0); // original column 0 (a)

		auto sorted_columns = g_proj.global_columns;
		std::sort(sorted_columns.begin(), sorted_columns.end());
		REQUIRE(sorted_columns == std::vector<idx_t> {0, 2});
	}

	SECTION("COUNT(*) keeps one column") {
		// No column is referenced, but the engine keeps one to preserve
		// cardinality; the exec bridge's first-vector sizing relies on it.
		auto rows = CollectI64(conn.Execute("SELECT COUNT(*) FROM proj_fn()"));
		REQUIRE(rows == std::vector<int64_t> {1}); // one emitted row counted
		REQUIRE(g_proj.global_columns.size() == 1);
		REQUIRE(g_proj.local_columns.size() == 1);
	}
}
TEST_CASE("Stable C++API: SetCardinality and SetMaxThreads smoke", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("card_fn")
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    input.AddResultColumn("v", LogicalType::BIGINT());
			    input.SetCardinality(555555, true);
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) {
			    input.SetMaxThreads(2);
			    input.SetGlobalState<std::atomic<bool>>(false);
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &emitted = input.GetGlobalState<std::atomic<bool>>();
			    auto vec = input.GetResultChunk().GetVector(0);
			    if (emitted.exchange(true)) {
				    vec.SetSize(0);
				    return;
			    }
			    vec.GetDataMutable<int64_t>()[0] = 42;
			    vec.SetSize(1);
		    })
		    .Register(ctx);
	});

	// The bind-time cardinality reaches the optimizer: EXPLAIN reports it.
	auto explain = conn.Execute("EXPLAIN SELECT v FROM card_fn()");
	std::string text;
	while (auto chunk = explain.FetchChunk()) {
		for (idx_t col = 0; col < chunk.GetVectorCount(); col++) {
			auto view = chunk.GetVector(col).GetView();
			for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
				if (!view.IsValid(i)) {
					continue;
				}
				// Strip thousands separators so the match is format-independent.
				for (char ch : view.Data<StringLayout>()[view.SelAt(i)].AsStringView()) {
					if (ch != ',') {
						text.push_back(ch);
					}
				}
			}
		}
	}
	REQUIRE(text.find("555555") != std::string::npos);

	// SetMaxThreads(2) runs on the real scan and the result stays correct.
	REQUIRE(CollectI64(conn.Execute("SELECT v FROM card_fn()")) == std::vector<int64_t> {42});
}

// ---------------------------------------------------------------------------
// The pushdown-time column list on PushdownInput.
//
// A candidate filter's BoundColumnRef.column_index indexes the scan's
// pushdown-time column list (filter-ordered, projection-pruned), NOT the
// function's bind-declared result columns. Without GetColumnIndex to resolve
// it, an index-based consumer builds the WRONG filter (e.g. `WHERE b > 7`
// becomes a filter on column a). These tests read the resolved index and
// assert it maps to the correct bind-declared column, and that the raw index
// would map to a different (wrong) one.
// ---------------------------------------------------------------------------
namespace {

struct PushdownFilterObs {
	int64_t constant = 0;
	duckdb_api::idx_t raw_index = 0;      // BoundColumnRef.column_index (pushdown-list position)
	duckdb_api::idx_t resolved_index = 0; // GetColumnIndex(raw_index): bind-declared column
};

struct PushdownColumnObservations {
	duckdb_api::idx_t pushdown_col_count = 0;
	std::vector<duckdb_api::idx_t> pushdown_cols; // GetColumnIndex(0..count)
	std::vector<PushdownFilterObs> filters;
	std::vector<duckdb_api::idx_t> init_cols; // the projected list init observes
};
PushdownColumnObservations g_pushdown_col_obs;
bool g_pushdown_mark_equal_handled = false;

} // namespace

TEST_CASE("Stable C++API: pushdown-time column list resolves filter columns", "[cpp_api]") {
	using namespace duckdb_api;

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction function(ctx);
		function.SetName("pushdown_col_fn")
		    .SetProjectionPushdown(true)
		    .SetBindCallback([](TableFunction::BindInput &input) {
			    // Bind schema: a=0, b=1, c=2, d=3.
			    input.AddResultColumn("a", LogicalType::BIGINT());
			    input.AddResultColumn("b", LogicalType::BIGINT());
			    input.AddResultColumn("c", LogicalType::BIGINT());
			    input.AddResultColumn("d", LogicalType::BIGINT());
			    input.SetBindData<int>(0);
		    })
		    .SetInitGlobalCallback([](TableFunction::InitGlobalInput &input) {
			    g_pushdown_col_obs.init_cols.clear();
			    for (idx_t i = 0; i < input.GetColumnCount(); i++) {
				    g_pushdown_col_obs.init_cols.push_back(input.GetColumnIndex(i));
			    }
			    input.SetGlobalState<bool>(false);
		    })
		    .SetPushdownComplexFilterCallback([](TableFunction::PushdownInput &input) {
			    g_pushdown_col_obs.pushdown_col_count = input.GetColumnCount();
			    g_pushdown_col_obs.pushdown_cols.clear();
			    for (idx_t j = 0; j < input.GetColumnCount(); j++) {
				    g_pushdown_col_obs.pushdown_cols.push_back(input.GetColumnIndex(j));
			    }
			    g_pushdown_col_obs.filters.clear();
			    for (idx_t i = 0; i < input.GetCount(); i++) {
				    auto expr = input.GetExpression(i);
				    PushdownFilterObs obs;
				    bool saw_column = false;
				    for (idx_t c = 0; c < expr.GetChildCount(); c++) {
					    auto child = expr.GetChild(c);
					    if (child.GetClass() == ExpressionClass::BoundColumnRef) {
						    obs.raw_index = child.GetColumnBinding().column_index;
						    obs.resolved_index = input.GetColumnIndex(obs.raw_index);
						    saw_column = true;
					    } else if (child.GetClass() == ExpressionClass::BoundConstant) {
						    obs.constant = child.GetConstantValue().AsBigint();
					    }
				    }
				    REQUIRE(saw_column);
				    g_pushdown_col_obs.filters.push_back(obs);
				    if (g_pushdown_mark_equal_handled && expr.GetType() == ExpressionType::CompareEqual) {
					    input.MarkHandled(i);
				    }
			    }
		    })
		    .SetExecCallback([](TableFunction::ExecInput &input) {
			    auto &done = input.GetGlobalState<bool>();
			    auto &out = input.GetResultChunk();
			    if (done) {
				    out.GetVector(0).SetSize(0);
				    return;
			    }
			    done = true;
			    for (idx_t col = 0; col < out.GetVectorCount(); col++) {
				    out.GetVector(col).GetDataMutable<int64_t>()[0] = 0;
			    }
			    out.GetVector(0).SetSize(1);
		    })
		    .Register(ctx);
	});

	SECTION("filter on a non-first column, projection pruned") {
		g_pushdown_col_obs = PushdownColumnObservations {};
		g_pushdown_mark_equal_handled = false;

		// Project only c; filter only b. The pushdown-time list is pruned to
		// the referenced columns, so it differs from the 4-column bind schema.
		auto rows = CollectI64(conn.Execute("SELECT c FROM pushdown_col_fn() WHERE b > 7"));
		(void)rows;

		// Pruned from 4 bind columns to the referenced {b, c}.
		REQUIRE(g_pushdown_col_obs.pushdown_col_count == 2);
		REQUIRE(g_pushdown_col_obs.pushdown_cols.size() == 2);
		auto sorted = g_pushdown_col_obs.pushdown_cols;
		std::sort(sorted.begin(), sorted.end());
		REQUIRE(sorted == std::vector<idx_t> {1, 2}); // b=1, c=2 (a and d are gone)

		// The single filter is `b > 7`. Its raw column_index does NOT equal b's
		// bind-declared index; only GetColumnIndex resolves it correctly.
		REQUIRE(g_pushdown_col_obs.filters.size() == 1);
		const auto &f = g_pushdown_col_obs.filters[0];
		REQUIRE(f.constant == 7);
		REQUIRE(f.resolved_index == 1); // correctly resolves to b (bind index 1)
		// Proof the naive path is wrong: using the raw index as the bind column
		// would select a DIFFERENT column than b.
		REQUIRE(f.raw_index != f.resolved_index);
		REQUIRE(g_pushdown_col_obs.pushdown_cols[f.raw_index] == 1); // raw index only makes sense THROUGH the list
	}

	SECTION("two filters, one marked handled: the list re-prunes") {
		g_pushdown_col_obs = PushdownColumnObservations {};
		g_pushdown_mark_equal_handled = true;

		// Filter on b (bind 1) and d (bind 3); project c. Mark the `d = 3`
		// filter handled, so column d, referenced only by the dropped filter,
		// is no longer needed and the projected list init sees re-prunes.
		auto rows = CollectI64(conn.Execute("SELECT c FROM pushdown_col_fn() WHERE b > 7 AND d = 3"));
		(void)rows;

		// Pushdown-time list carries all three referenced columns {b, c, d}.
		REQUIRE(g_pushdown_col_obs.pushdown_col_count == 3);
		auto sorted_pd = g_pushdown_col_obs.pushdown_cols;
		std::sort(sorted_pd.begin(), sorted_pd.end());
		REQUIRE(sorted_pd == std::vector<idx_t> {1, 2, 3}); // b, c, d

		// Both filters resolve to the correct bind-declared column.
		REQUIRE(g_pushdown_col_obs.filters.size() == 2);
		bool checked_gt = false, checked_eq = false;
		for (const auto &fo : g_pushdown_col_obs.filters) {
			if (fo.constant == 7) { // b > 7
				REQUIRE(fo.resolved_index == 1);
				REQUIRE(g_pushdown_col_obs.pushdown_cols[fo.raw_index] == 1);
				checked_gt = true;
			} else if (fo.constant == 3) { // d = 3
				REQUIRE(fo.resolved_index == 3);
				REQUIRE(fo.raw_index != fo.resolved_index); // naive index would pick the wrong column
				REQUIRE(g_pushdown_col_obs.pushdown_cols[fo.raw_index] == 3);
				checked_eq = true;
			}
		}
		REQUIRE(checked_gt);
		REQUIRE(checked_eq);

		// The init callback observes a DIFFERENTLY pruned list: d was dropped
		// with the handled filter, so it is absent from what init sees. Decoding
		// the pushdown filters through the init list would therefore be wrong;
		// only the pushdown-time list decodes them.
		auto sorted_init = g_pushdown_col_obs.init_cols;
		std::sort(sorted_init.begin(), sorted_init.end());
		REQUIRE(sorted_init == std::vector<idx_t> {1, 2}); // b, c (d re-pruned away)
		REQUIRE(sorted_init != sorted_pd);                 // the two lists genuinely differ
	}
}
