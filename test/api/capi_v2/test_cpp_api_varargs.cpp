#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api_helpers.hpp"
#include "test_helpers.hpp"

#include <string>

// ---------------------------------------------------------------------------
// Stable C++ API tests: varargs, ANY construction / gates, bind-argument
// introspection, and expression folding.
// ---------------------------------------------------------------------------

using namespace duckdb_api;

namespace {

int32_t ScalarI32(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	return view.Data<int32_t>()[view.SelAt(0)];
}

int64_t ScalarI64(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.IsValid(0));
	return view.Data<int64_t>()[view.SelAt(0)];
}

// Sums every INTEGER input column.
void IntSumExec(ScalarFunction::ExecInput &input) {
	auto chunk = input.GetInputChunk();
	auto out = input.GetResultVector().GetDataMutable<int32_t>();
	const idx_t rows = chunk.GetRowCount();
	const idx_t cols = chunk.GetVectorCount();
	for (idx_t r = 0; r < rows; r++) {
		out[r] = 0;
	}
	for (idx_t c = 0; c < cols; c++) {
		auto vec = chunk.GetVector(c);
		auto view = vec.GetView();
		auto data = view.Data<int32_t>();
		for (idx_t r = 0; r < rows; r++) {
			out[r] += data[view.SelAt(r)];
		}
	}
}

// ANY varargs: counts the INTEGER-typed columns.
void CountIntsExec(ScalarFunction::ExecInput &input) {
	auto chunk = input.GetInputChunk();
	int32_t n = 0;
	for (idx_t c = 0; c < chunk.GetVectorCount(); c++) {
		if (chunk.GetVector(c).GetLogicalType().GetId() == TypeId::INTEGER) {
			n++;
		}
	}
	auto out = input.GetResultVector().GetDataMutable<int32_t>();
	for (idx_t r = 0; r < chunk.GetRowCount(); r++) {
		out[r] = n;
	}
}

// Fixed ANY params, ignored; returns 42.
void TwoAnyExec(ScalarFunction::ExecInput &input) {
	auto out = input.GetResultVector().GetDataMutable<int32_t>();
	for (idx_t r = 0; r < input.GetInputChunk().GetRowCount(); r++) {
		out[r] = 42;
	}
}

} // namespace

TEST_CASE("Stable C++API: LogicalType::ANY and its gates", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto any = LogicalType::ANY();
	REQUIRE(any.GetId() == TypeId::ANY);
	REQUIRE(any.ToText() == "ANY");

	// An ANY return type is rejected at Register.
	conn.WithTransaction([](const Context &ctx) {
		ScalarFunction f(ctx);
		f.SetName("bad_any_return").AddParameter("a", LogicalType::INTEGER()).SetReturnType(LogicalType::ANY());
		f.SetExecCallback([](ScalarFunction::ExecInput &) {});
		REQUIRE_THROWS_MATCHES(f.Register(ctx), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	});
}

TEST_CASE("Stable C++API: scalar varargs", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		ScalarFunction int_sum(ctx);
		int_sum.SetName("cpp_int_sum")
		    .SetVarArgs(LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback(IntSumExec)
		    .Register(ctx);

		ScalarFunction prefix_sum(ctx);
		prefix_sum.SetName("cpp_prefix_sum")
		    .AddParameter("base", LogicalType::INTEGER())
		    .SetVarArgs(LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback(IntSumExec)
		    .Register(ctx);

		ScalarFunction count_ints(ctx);
		count_ints.SetName("cpp_count_ints")
		    .SetVarArgs(LogicalType::ANY())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback(CountIntsExec)
		    .Register(ctx);

		ScalarFunction two_any(ctx);
		two_any.SetName("cpp_two_any")
		    .AddParameter("a", LogicalType::ANY())
		    .AddParameter("b", LogicalType::ANY())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetExecCallback(TwoAnyExec)
		    .Register(ctx);
	});

	REQUIRE(ScalarI32(conn, "SELECT cpp_int_sum(1, 2, 3)") == 6);
	REQUIRE(ScalarI32(conn, "SELECT cpp_int_sum(1, '2', 3)") == 6);
	REQUIRE(ScalarI32(conn, "SELECT cpp_int_sum(1, 2, x := 3)") == 6);
	REQUIRE(ScalarI32(conn, "SELECT cpp_prefix_sum(10, 1, 2)") == 13);
	REQUIRE(ScalarI32(conn, "SELECT cpp_prefix_sum(42)") == 42);
	REQUIRE(ScalarI32(conn, "SELECT cpp_count_ints(1::INTEGER, 'x', 2::INTEGER, 3.0::DOUBLE)") == 2);
	REQUIRE(ScalarI32(conn, "SELECT cpp_two_any(1, 'x')") == 42);
	REQUIRE(ScalarI32(conn, "SELECT cpp_two_any(3.0, TRUE)") == 42);
}

namespace {

// probe_add(a, b): bind checks argument types and folds the constant b (folding
// the column a fails). exec returns a + b.
void ProbeBind(ScalarFunction::BindInput &input) {
	REQUIRE(input.GetArgumentCount() == 2);
	REQUIRE(input.GetArgumentType(0).GetId() == TypeId::INTEGER);
	REQUIRE(input.GetArgumentType(1).GetId() == TypeId::INTEGER);
	REQUIRE_THROWS_MATCHES(input.FoldArgument(0), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE(input.FoldArgument(1).AsInteger() == 7);
	REQUIRE_THROWS_MATCHES(input.GetArgumentType(5), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(input.FoldArgument(5), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

} // namespace

TEST_CASE("Stable C++API: bind argument accessors", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		ScalarFunction probe(ctx);
		probe.SetName("cpp_probe_add")
		    .AddParameter("a", LogicalType::INTEGER())
		    .AddParameter("b", LogicalType::INTEGER())
		    .SetReturnType(LogicalType::INTEGER())
		    .SetBindCallback(ProbeBind)
		    .SetExecCallback(IntSumExec)
		    .Register(ctx);
	});

	REQUIRE(ScalarI32(conn, "SELECT cpp_probe_add(x, 7) FROM (VALUES (100)) t(x)") == 107);
}

namespace {

void AggSize(AggregateFunction::SizeInput &input) {
	input.Reserve<int64_t>();
}
void AggInit(AggregateFunction::InitializeInput &input) {
	input.Initialize<int64_t>(0);
}
void AggUpdate(AggregateFunction::UpdateInput &input) {
	auto &chunk = input.GetInputChunk();
	auto states = input.GetStateArray<int64_t>();
	const idx_t rows = input.GetStateCount();
	for (idx_t c = 0; c < chunk.GetVectorCount(); c++) {
		auto vec = chunk.GetVector(c);
		auto view = vec.GetView();
		auto data = view.Data<int32_t>();
		for (idx_t i = 0; i < rows; i++) {
			if (view.IsValid(i)) {
				*states[i] += data[view.SelAt(i)];
			}
		}
	}
}
void AggCombine(AggregateFunction::CombineInput &input) {
	auto sources = input.GetSourceStateArray<int64_t>();
	auto targets = input.GetTargetStateArray<int64_t>();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		*targets[i] += *sources[i];
	}
}
void AggFinalize(AggregateFunction::FinalizeInput &input) {
	auto states = input.GetStateArray<int64_t>();
	auto out = input.GetResultVector().GetDataMutable<int64_t>();
	const idx_t offset = input.GetResultOffset();
	for (idx_t i = 0; i < input.GetStateCount(); i++) {
		out[offset + i] = *states[i];
	}
}
} // namespace

TEST_CASE("Stable C++API: aggregate varargs", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		AggregateFunction multi_sum(ctx);
		multi_sum.SetName("cpp_multi_sum")
		    .SetVarArgs(LogicalType::INTEGER())
		    .SetReturnType(LogicalType::BIGINT())
		    .SetSizeCallback(AggSize)
		    .SetInitializeCallback(AggInit)
		    .SetUpdateCallback(AggUpdate)
		    .SetCombineCallback(AggCombine)
		    .SetFinalizeCallback(AggFinalize)
		    .Register(ctx);
	});

	REQUIRE(ScalarI64(conn, "SELECT cpp_multi_sum(a, b) FROM (VALUES (1,2),(3,4)) t(a,b)") == 10);
}

namespace {

void TfBind(TableFunction::BindInput &input) {
	const idx_t count = input.GetParameterCount();
	input.AddResultColumn("n", LogicalType::INTEGER());
	input.SetBindData<idx_t>(count);
}
void TfInitGlobal(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<bool>(false);
}
void TfExec(TableFunction::ExecInput &input) {
	auto &chunk = input.GetResultChunk();
	auto vec = chunk.GetVector(0);
	auto &emitted = input.GetGlobalState<bool>();
	if (emitted) {
		vec.SetSize(0);
		return;
	}
	vec.GetDataMutable<int32_t>()[0] = static_cast<int32_t>(input.GetBindData<idx_t>());
	emitted = true;
	vec.SetSize(1);
}

} // namespace

TEST_CASE("Stable C++API: table function varargs and parameter count", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	conn.WithTransaction([](const Context &ctx) {
		TableFunction tf(ctx);
		tf.SetName("cpp_tf_count")
		    .SetVarArgs(LogicalType::INTEGER())
		    .SetBindCallback(TfBind)
		    .SetInitGlobalCallback(TfInitGlobal)
		    .SetExecCallback(TfExec)
		    .Register(ctx);
	});

	auto result = conn.Execute("SELECT n FROM cpp_tf_count(10, 20, 30)");
	auto chunk = result.FetchChunk();
	REQUIRE(chunk);
	REQUIRE(chunk.GetRowCount() == 1);
	auto view = chunk.GetVector(0).GetView();
	REQUIRE(view.Data<int32_t>()[view.SelAt(0)] == 3);
}
