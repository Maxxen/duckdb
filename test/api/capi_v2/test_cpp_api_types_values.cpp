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
// Stable C++ API tests: types and values.
// ---------------------------------------------------------------------------

TEST_CASE("Stable C++API: Value Null and the Bignum codec", "[cpp_api][types_values]") {
	using namespace duckdb_api;

	auto null_value = Value::Null(LogicalType::INTEGER());
	REQUIRE(null_value.IsNull());
	REQUIRE(null_value.GetLogicalType() == LogicalType::INTEGER());

	// 2^64: a 0x01 byte followed by eight 0x00 bytes.
	const std::vector<uint8_t> magnitude = {0x01, 0, 0, 0, 0, 0, 0, 0, 0};
	auto positive = Value::FromBignum(magnitude.data(), magnitude.size(), false);
	REQUIRE(positive.ToString() == "18446744073709551616");

	auto decoded = positive.AsBignum();
	REQUIRE(decoded.magnitude == magnitude);
	REQUIRE_FALSE(decoded.is_negative);

	auto negative = Value::FromBignum(magnitude.data(), magnitude.size(), true).AsBignum();
	REQUIRE(negative.magnitude == magnitude);
	REQUIRE(negative.is_negative);
}
TEST_CASE("Stable C++API: TypeId, ToText, and ParseType round trip", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	REQUIRE(LogicalType::INTEGER().GetId() == TypeId::INTEGER);
	REQUIRE(LogicalType::INTEGER().ToText() == "INTEGER");

	// Connection sugar.
	auto dec = conn.ParseType("DECIMAL(12,4)");
	REQUIRE(dec.GetId() == TypeId::DECIMAL);
	REQUIRE(dec.ToText() == "DECIMAL(12,4)");
	REQUIRE(dec == conn.ParseType(dec.ToText()));

	// Context primary form, usable inside a with-context scope.
	conn.WithTransaction([](const Context &ctx) {
		auto type_type = ctx.ParseType("TYPE");
		REQUIRE(type_type.GetId() == TypeId::TYPE);
		auto list = ctx.ParseType("INTEGER[]");
		REQUIRE(list.GetId() == TypeId::LIST);
	});

	REQUIRE_THROWS_MATCHES(conn.ParseType("definitely_not_a_type"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_DATABASE_CATALOG));
}
TEST_CASE("Stable C++API: CreateType named + positional params and the GetParam dual", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Positional numeric params.
	std::vector<TypeParam> dec_params;
	dec_params.push_back({"", Value::FromI64(12)});
	dec_params.push_back({"", Value::FromI64(4)});
	auto dec = conn.CreateType("decimal", dec_params);
	REQUIRE(dec == conn.ParseType("DECIMAL(12,4)"));
	REQUIRE(dec.GetParamCount() == 2);
	auto width_param = dec.GetParam(0);
	REQUIRE(width_param.name.empty());
	REQUIRE(width_param.value.AsU8() == 12);

	// Named TYPE-value params.
	std::vector<TypeParam> fields;
	fields.push_back({"a", Value::Type(LogicalType::INTEGER())});
	fields.push_back({"b", Value::Type(LogicalType::VARCHAR())});
	auto s = conn.CreateType("struct", fields);
	REQUIRE(s.ToText() == "STRUCT(a INTEGER, b VARCHAR)");
	auto field = s.GetParam(1);
	REQUIRE(field.name == "b");
	REQUIRE(field.value.AsType() == LogicalType::VARCHAR());

	// The dual: create(name, params(t)) equals t.
	std::vector<TypeParam> rebuilt_params;
	for (idx_t i = 0; i < s.GetParamCount(); i++) {
		rebuilt_params.push_back(s.GetParam(i));
	}
	REQUIRE(conn.CreateType("struct", rebuilt_params) == s);

	REQUIRE_THROWS_MATCHES(conn.CreateType("list", {}), Exception, HasErrorCode(DUCKDB_V2_ERROR_QUERY_BINDER));
}
TEST_CASE("Stable C++API: per-kind type getters are sugar over GetParam", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto dec = conn.ParseType("DECIMAL(18,3)");
	REQUIRE(dec.GetDecimalWidth() == 18);
	REQUIRE(dec.GetDecimalScale() == 3);

	std::vector<TypeParam> entries;
	entries.push_back({"", Value::FromVarchar("sad")});
	entries.push_back({"", Value::FromVarchar("ok")});
	entries.push_back({"", Value::FromVarchar("happy")});
	auto mood = conn.CreateType("enum", entries);
	REQUIRE(mood.GetEnumSize() == 3);
	REQUIRE(mood.GetEnumValue(2) == "happy");

	auto list = conn.ParseType("INTEGER[]");
	REQUIRE(list.GetListChildType() == LogicalType::INTEGER());

	auto arr = conn.ParseType("VARCHAR[7]");
	REQUIRE(arr.GetArrayChildType() == LogicalType::VARCHAR());
	REQUIRE(arr.GetArraySize() == 7);

	auto map = conn.ParseType("MAP(VARCHAR, INTEGER)");
	REQUIRE(map.GetMapKeyType() == LogicalType::VARCHAR());
	REQUIRE(map.GetMapValueType() == LogicalType::INTEGER());

	auto s = conn.ParseType("STRUCT(id INTEGER, label VARCHAR)");
	REQUIRE(s.GetStructChildCount() == 2);
	REQUIRE(s.GetStructChildName(0) == "id");
	REQUIRE(s.GetStructChildType(1) == LogicalType::VARCHAR());

	auto u = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	REQUIRE(u.GetUnionMemberCount() == 2);
	REQUIRE(u.GetUnionMemberName(1) == "s");
	REQUIRE(u.GetUnionMemberType(0) == LogicalType::INTEGER());

	// The sugar gates on the type kind.
	REQUIRE_THROWS_MATCHES(LogicalType::INTEGER().GetDecimalWidth(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE_THROWS_MATCHES(list.GetEnumSize(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: TYPE values and composite Value::Create", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// TYPE values wrap and unwrap.
	auto wrapped = Value::Type(LogicalType::INTEGER());
	REQUIRE(wrapped.GetLogicalType().GetId() == TypeId::TYPE);
	REQUIRE(wrapped.AsType() == LogicalType::INTEGER());
	REQUIRE_THROWS_MATCHES(Value::FromI64(1).AsType(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));

	// LIST: children cast to the declared child type.
	auto list_type = conn.ParseType("BIGINT[]");
	std::vector<Value> elements;
	elements.push_back(Value::FromI64(1));
	elements.push_back(Value::FromI64(2));
	auto list = Value::Create(list_type, elements);
	REQUIRE(list.GetChildCount() == 2);
	REQUIRE(list.GetChild(1).AsI64() == 2);

	// MAP: alternating key, value.
	auto map_type = conn.ParseType("MAP(VARCHAR, INTEGER)");
	std::vector<Value> entries;
	entries.push_back(Value::FromVarchar("a"));
	entries.push_back(Value::FromI64(1));
	auto map = Value::Create(map_type, entries);
	REQUIRE(map.GetChildCount() == 2);
	REQUIRE(map.GetChild(0).ToString() == "a");

	// UNION values are built via Cast, not Create.
	auto union_type = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	REQUIRE_THROWS_MATCHES(Value::Create(union_type, elements), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: Value::Cast through Context and Connection", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Connection sugar.
	auto parsed = Value::FromVarchar("42").Cast(conn, LogicalType::INTEGER());
	REQUIRE(parsed.AsI32() == 42);

	// Context primary form.
	conn.WithTransaction([](const Context &ctx) {
		auto date = Value::FromVarchar("2024-03-15").Cast(ctx, ctx.ParseType("DATE"));
		REQUIRE(date.ToString() == "2024-03-15");
	});

	// UNION via cast: [0] = tag as UTINYINT, [1] = the active member.
	auto union_type = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	auto u = Value::FromVarchar("x").Cast(conn, union_type);
	REQUIRE(u.GetChildCount() == 2);
	REQUIRE(u.GetChild(0).AsU8() == 1);
	REQUIRE(u.GetChild(1).ToString() == "x");

	// ENUM via cast.
	std::vector<TypeParam> entries;
	entries.push_back({"", Value::FromVarchar("sad")});
	entries.push_back({"", Value::FromVarchar("happy")});
	auto mood = conn.CreateType("enum", entries);
	auto happy = Value::FromVarchar("happy").Cast(conn, mood);
	REQUIRE(happy.GetLogicalType().GetId() == TypeId::ENUM);
	REQUIRE(happy.ToString() == "happy");

	// Cast failures carry the engine's code.
	REQUIRE_THROWS_MATCHES(Value::FromVarchar("abc").Cast(conn, LogicalType::INTEGER()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: Vector GetValue / SetValue", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Read path: cells of a query result chunk.
	auto result = conn.Execute("SELECT * FROM range(5)");
	auto chunk = result.FetchChunk();
	auto vec = chunk.GetVector(0);
	REQUIRE(vec.GetValue(0).AsI64() == 0);
	REQUIRE(vec.GetValue(4).AsI64() == 4);
	REQUIRE_THROWS_MATCHES(vec.GetValue(5), Exception, HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));

	// Write path: a manually built chunk, cast-on-write included.
	conn.WithTransaction([](const Context &ctx) {
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT());
		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(2);
		vec.SetValue(0, Value::FromI64(7));
		vec.SetValue(1, Value::FromVarchar("8")); // cast on write
		REQUIRE(vec.GetValue(0).AsI64() == 7);
		REQUIRE(vec.GetValue(1).AsI64() == 8);
		REQUIRE_THROWS_MATCHES(vec.SetValue(2, Value::FromI64(9)), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	});
}
namespace {
// VARCHAR -> CELSIUS cast: parses "<digits>C", so success proves the
// registered cast ran (the default VARCHAR -> INTEGER cast rejects "21C").
void VarcharToCelsius(duckdb_api::CastFunction::ExecInput &input) {
	using duckdb_api::StringStorage;
	auto in_vec = input.GetInput();
	auto out_vec = input.GetOutput();
	in_vec.Flatten();
	const auto *in = in_vec.GetDataMutable<const StringStorage>();
	auto *out = out_vec.GetDataMutable<int32_t>();
	for (duckdb_api::idx_t i = 0; i < input.GetCount(); i++) {
		const auto &token = in[i];
		auto len = token.Length();
		const auto *data = token.Data();
		if (len < 2 || data[len - 1] != 'C') {
			throw duckdb_api::Exception(DUCKDB_V2_ERROR_INVALID_INPUT, "expected '<digits>C'");
		}
		int32_t parsed = 0;
		for (uint32_t b = 0; b + 1 < len; b++) {
			if (data[b] < '0' || data[b] > '9') {
				throw duckdb_api::Exception(DUCKDB_V2_ERROR_INVALID_INPUT, "expected '<digits>C'");
			}
			parsed = parsed * 10 + (data[b] - '0');
		}
		out[i] = parsed;
	}
}
} // namespace
TEST_CASE("Stable C++API: extension type end to end through the C++ surface", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Register the type and its VARCHAR cast.
	conn.WithTransaction([](const Context &ctx) {
		CustomType custom_type(ctx);
		custom_type.SetName("CELSIUS").SetBaseType(LogicalType::INTEGER()).Register(ctx);

		// The registered type constructs through the generic constructor.
		auto celsius = ctx.CreateType("celsius", {});
		CastFunction cast(ctx);
		cast.SetSourceType(LogicalType::VARCHAR())
		    .SetTargetType(celsius)
		    .SetImplicitCastCost(0)
		    .SetExecCallback(VarcharToCelsius)
		    .Register(ctx);
	});

	auto celsius = conn.CreateType("celsius", {});
	REQUIRE(celsius.GetName() == "CELSIUS");
	REQUIRE(celsius.GetId() == TypeId::INTEGER);

	// Build a value through the registered cast.
	auto reading = Value::FromVarchar("21C").Cast(conn, celsius);
	REQUIRE(reading.AsI32() == 21);
	REQUIRE(reading.GetLogicalType().GetName() == "CELSIUS");

	// Use both in a query: the type in DDL, the value as a bound parameter.
	conn.Execute("CREATE TABLE readings(c CELSIUS)").Drain();
	auto statements = conn.ParseSQL("INSERT INTO readings VALUES ($1)");
	auto insert = statements.Next();
	std::vector<Value> parameters;
	parameters.push_back(std::move(reading));
	conn.Execute(insert, parameters).Drain();

	// Read the cell back through the single-cell bridge.
	auto result = conn.Execute("SELECT c FROM readings");
	auto chunk = result.FetchChunk();
	auto cell = chunk.GetVector(0).GetValue(0);
	REQUIRE(cell.AsI32() == 21);
	REQUIRE(cell.GetLogicalType().GetName() == "CELSIUS");
}
TEST_CASE("Stable C++API: storage-tier conveniences follow the committed tables", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	struct {
		int width;
		TypeId expected;
	} decimal_cases[] = {
	    {4, TypeId::SMALLINT}, {5, TypeId::INTEGER},  {9, TypeId::INTEGER},  {10, TypeId::BIGINT},
	    {18, TypeId::BIGINT},  {19, TypeId::HUGEINT}, {38, TypeId::HUGEINT},
	};
	for (auto &c : decimal_cases) {
		auto dec = conn.ParseType("DECIMAL(" + std::to_string(c.width) + ",2)");
		REQUIRE(dec.GetDecimalInternalTypeId() == c.expected);
	}

	struct {
		idx_t entries;
		TypeId expected;
	} enum_cases[] = {
	    {1, TypeId::UTINYINT},      {255, TypeId::UTINYINT},   {256, TypeId::USMALLINT},
	    {65535, TypeId::USMALLINT}, {65536, TypeId::UINTEGER},
	};
	for (auto &c : enum_cases) {
		std::vector<TypeParam> entries;
		entries.reserve(c.entries);
		for (idx_t i = 0; i < c.entries; i++) {
			entries.push_back({"", Value::FromVarchar("v" + std::to_string(i))});
		}
		auto mood = conn.CreateType("enum", entries);
		REQUIRE(mood.GetEnumInternalTypeId() == c.expected);
	}

	// Gated on the type kind like the other sugars.
	REQUIRE_THROWS_MATCHES(LogicalType::INTEGER().GetDecimalInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
	REQUIRE_THROWS_MATCHES(LogicalType::INTEGER().GetEnumInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
}
TEST_CASE("Stable C++API: VARIANT column consumed per row through the boxed value path",
          "[cpp_api][types_values][variant]") {
	using namespace duckdb_api;
	// Completeness pin of the accepted-inefficient totality path, NOT a
	// performance exercise: VARIANT has no committed view layout, so the
	// per-row boxed GetValue is the only cell path for this kind, and every
	// row costs a boxed Value plus an unwrap. Do not optimize this and do
	// not grow surface for it; core VARIANT work is in flux.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// One VARIANT row per inner kind. Inner-type discovery is in-surface:
	// Value::UnwrapVariant returns the carried value with its real logical
	// type, so the loop below needs no prior knowledge of any row's type.
	// variant_typeof rides along purely to document the SQL-side vocabulary
	// (variant-internal names: DECIMAL literals, OBJECT / ARRAY, MAP-ness
	// erased to a key-value array, VARIANT_NULL), which remains core-facing.
	const char *heap_string = "a string long enough to spill";
	auto result = conn.Execute("SELECT i, variant_typeof(v) AS t, v FROM (VALUES "
	                           "(1, 42::VARIANT), "
	                           "(2, (2.5::DOUBLE)::VARIANT), "
	                           "(3, 'a string long enough to spill'::VARIANT), "
	                           "(4, (DATE '2024-03-15')::VARIANT), "
	                           "(5, [1, 2, 3]::VARIANT), "
	                           "(6, {'a': 7, 'b': 'xy'}::VARIANT), "
	                           "(7, MAP {'k1': 1, 'k2': 2}::VARIANT), "
	                           "(8, NULL::VARIANT), "
	                           "(9, [42::VARIANT, 'x'::VARIANT]::VARIANT)) t(i, v) ORDER BY i");
	REQUIRE(result.GetSchema().GetFieldType(2).GetId() == TypeId::VARIANT);
	auto chunk = result.FetchChunk();
	REQUIRE(chunk.GetRowCount() == 9);
	auto vec = chunk.GetVector(2);
	REQUIRE(vec.GetLogicalType().GetId() == TypeId::VARIANT);

	// The scan hands back a FLAT vector, so GetView() itself succeeds; the
	// committed layout table has no VARIANT row, so the view's data is not
	// interpretable per contract. GetValue is the only cell path.
	REQUIRE(vec.GetVectorType() == VectorType::Flat);
	auto view = vec.GetView();
	REQUIRE(view.count == 9);

	// The SQL-side vocabulary pin.
	auto typeof_vec = chunk.GetVector(1);
	const char *expected_typeof[9] = {"INT32",        "DOUBLE",   "VARCHAR",      "DATE",    "ARRAY(3)",
	                                  "OBJECT(a, b)", "ARRAY(2)", "VARIANT_NULL", "ARRAY(2)"};
	for (idx_t row = 0; row < 9; row++) {
		auto cell = typeof_vec.GetValue(row);
		REQUIRE(cell.ToString() == expected_typeof[row]);
	}

	// The discovery loop: unwrap, switch on the REAL type id, render via
	// ordinary reads and descent. Renders one summary per row; a VARIANT
	// list element (heterogeneous array) is unwrapped recursively.
	auto render_leaf = [](const Value &inner) -> std::string {
		switch (inner.GetLogicalType().GetId()) {
		case TypeId::INTEGER:
			return "INTEGER:" + std::to_string(inner.AsI32());
		case TypeId::DOUBLE:
			return "DOUBLE:" + inner.ToString();
		case TypeId::VARCHAR:
			return "VARCHAR:" + std::string(inner.AsVarchar());
		case TypeId::DATE:
			return "DATE:" + inner.ToString();
		default:
			return "OTHER:" + inner.ToString();
		}
	};
	std::vector<std::string> summaries;
	for (idx_t row = 0; row < 9; row++) {
		auto box = vec.GetValue(row);
		if (box.IsNull()) {
			summaries.push_back("NULL");
			continue;
		}
		REQUIRE(box.GetLogicalType().GetId() == TypeId::VARIANT);
		auto inner = box.UnwrapVariant();
		auto inner_type = inner.GetLogicalType();
		switch (inner_type.GetId()) {
		case TypeId::LIST: {
			// Covers the typed list, the MAP (which the variant encoding
			// erases to LIST(STRUCT(key, value))), and the heterogeneous
			// array (LIST(VARIANT), elements unwrapped recursively).
			std::string out = "LIST(" + inner_type.GetListChildType().ToText() + "):";
			for (idx_t i = 0; i < inner.GetChildCount(); i++) {
				auto element = inner.GetChild(i);
				if (i > 0) {
					out += ",";
				}
				if (element.GetLogicalType().GetId() == TypeId::VARIANT) {
					out += render_leaf(element.UnwrapVariant());
				} else {
					out += element.ToString();
				}
			}
			summaries.push_back(out);
			break;
		}
		case TypeId::STRUCT: {
			std::string out = "STRUCT:";
			for (idx_t i = 0; i < inner.GetChildCount(); i++) {
				if (i > 0) {
					out += ",";
				}
				out += inner_type.GetStructChildName(i) + "=" + inner.GetChild(i).ToString();
			}
			summaries.push_back(out);
			break;
		}
		default:
			summaries.push_back(render_leaf(inner));
			break;
		}
	}
	REQUIRE(summaries.size() == 9);
	REQUIRE(summaries[0] == "INTEGER:42");
	REQUIRE(summaries[1] == "DOUBLE:2.5");
	REQUIRE(summaries[2] == std::string("VARCHAR:") + heap_string);
	REQUIRE(summaries[3] == "DATE:2024-03-15");
	REQUIRE(summaries[4] == "LIST(INTEGER):1,2,3");
	REQUIRE(summaries[5] == "STRUCT:a=7,b=xy");
	// MAP-ness is erased by the variant encoding: key-value struct entries.
	REQUIRE(summaries[6] == "LIST(STRUCT(\"key\" VARCHAR, \"value\" INTEGER)):"
	                        "{'key': k1, 'value': 1},{'key': k2, 'value': 2}");
	REQUIRE(summaries[7] == "NULL");
	REQUIRE(summaries[8] == "LIST(VARIANT):INTEGER:42,VARCHAR:x");
}
TEST_CASE("Stable C++API: writing a VARIANT vector through the boxed value path", "[cpp_api][types_values][variant]") {
	using namespace duckdb_api;
	// Companion completeness pin to the VARIANT read test, NOT a performance
	// exercise: per-row boxed SetValue / GetValue is the accepted-inefficient
	// totality path (VARIANT has no committed view layout, so the single-cell
	// bridge is the only access), and core VARIANT work is in flux. Do not
	// optimize this and do not grow surface for it.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// A boxed NULL fetched up front: the C++ surface has no NULL-value
	// constructor, and results must not be consumed inside the scope below.
	auto null_result = conn.Execute("SELECT NULL::VARIANT");
	auto null_chunk = null_result.FetchChunk();
	auto boxed_null = null_chunk.GetVector(0).GetValue(0);
	REQUIRE(boxed_null.IsNull());

	conn.WithTransaction([&](const Context &ctx) {
		auto variant_type = ctx.ParseType("VARIANT");
		std::vector<LogicalType> types;
		types.push_back(ctx.ParseType("VARIANT"));
		DataChunk chunk(ctx, types);
		auto vec = chunk.GetVector(0);
		REQUIRE(vec.GetLogicalType().GetId() == TypeId::VARIANT);
		REQUIRE(vec.GetVectorType() == VectorType::Flat);
		vec.SetSize(5);

		// PROBED: SetValue casts on write to VARIANT engine-side (the
		// to-VARIANT cast needs no context), so plain values write directly.
		vec.SetValue(0, Value::FromI64(42));
		const char *heap_string = "a string long enough to spill";
		vec.SetValue(1, Value::FromVarchar(heap_string));
		auto list_type = ctx.ParseType("INTEGER[]");
		std::vector<Value> elements;
		elements.push_back(Value::FromI64(1));
		elements.push_back(Value::FromI64(2));
		elements.push_back(Value::FromI64(3));
		vec.SetValue(2, Value::Create(list_type, elements));
		vec.SetValue(3, boxed_null);
		// The explicit route works too: box first, then write.
		vec.SetValue(4, Value::FromI64(43).Cast(ctx, variant_type));

		// Read back through the boxed path: every non-NULL cell is a
		// VARIANT box; Cast back to the known inner type round-trips.
		for (idx_t row : {idx_t(0), idx_t(1), idx_t(2), idx_t(4)}) {
			auto box = vec.GetValue(row);
			REQUIRE_FALSE(box.IsNull());
			REQUIRE(box.GetLogicalType().GetId() == TypeId::VARIANT);
		}
		REQUIRE(vec.GetValue(0).ToString() == "42");
		REQUIRE(vec.GetValue(0).Cast(ctx, ctx.ParseType("BIGINT")).AsI64() == 42);
		REQUIRE(vec.GetValue(1).Cast(ctx, LogicalType::VARCHAR()).AsVarchar() == heap_string);
		auto unboxed_list = vec.GetValue(2).Cast(ctx, list_type);
		REQUIRE(unboxed_list.GetChildCount() == 3);
		REQUIRE(unboxed_list.GetChild(2).AsI32() == 3);
		REQUIRE(vec.GetValue(3).IsNull());
		REQUIRE(vec.GetValue(4).Cast(ctx, ctx.ParseType("BIGINT")).AsI64() == 43);

		// MakeConstant interplay: the type-equality hardening refuses the
		// raw non-VARIANT value; the boxed value works.
		std::vector<LogicalType> constant_types;
		constant_types.push_back(ctx.ParseType("VARIANT"));
		DataChunk constant_chunk(ctx, constant_types);
		auto cvec = constant_chunk.GetVector(0);
		REQUIRE_THROWS_MATCHES(cvec.MakeConstant(Value::FromI64(7), 3), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_INVALID_INPUT));
		auto boxed = Value::FromI64(7).Cast(ctx, variant_type);
		cvec.MakeConstant(boxed, 3);
		REQUIRE(cvec.GetValue(2).GetLogicalType().GetId() == TypeId::VARIANT);
		REQUIRE(cvec.GetValue(2).ToString() == "7");
	});
}
