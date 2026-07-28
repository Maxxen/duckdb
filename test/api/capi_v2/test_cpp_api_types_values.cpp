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

TEST_CASE("Stable C++API: Value Null and the DecodedBignum codec", "[cpp_api][types_values]") {
	using namespace duckdb_api;

	auto null_value = Value::Null(LogicalType::INTEGER());
	REQUIRE(null_value.IsNull());
	REQUIRE(null_value.GetLogicalType() == LogicalType::INTEGER());

	// 2^64: a 0x01 byte followed by eight 0x00 bytes.
	const std::vector<uint8_t> magnitude = {0x01, 0, 0, 0, 0, 0, 0, 0, 0};
	auto positive = Value::Bignum(magnitude.data(), magnitude.size(), false);
	REQUIRE(positive.ToString() == "18446744073709551616");

	auto decoded = positive.AsBignum();
	REQUIRE(decoded.magnitude == magnitude);
	REQUIRE_FALSE(decoded.is_negative);

	auto negative = Value::Bignum(magnitude.data(), magnitude.size(), true).AsBignum();
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

	// TUPLE (the unnamed struct) parses and reports its own id; children
	// come back positionally through the generic GetParam path.
	auto tup = conn.ParseType("TUPLE(INTEGER, VARCHAR)");
	REQUIRE(tup.GetId() == TypeId::TUPLE);
	REQUIRE(tup.GetParamCount() == 2);
	REQUIRE(tup.GetParam(0).name.empty());
	REQUIRE(tup.GetParam(0).value.AsType().GetId() == TypeId::INTEGER);

	// Connection form.
	auto type_type = conn.ParseType("TYPE");
	REQUIRE(type_type.GetId() == TypeId::TYPE);
	auto list = conn.ParseType("INTEGER[]");
	REQUIRE(list.GetId() == TypeId::LIST);

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
	dec_params.push_back({"", Value::Bigint(12)});
	dec_params.push_back({"", Value::Bigint(4)});
	auto dec = conn.CreateType("decimal", dec_params);
	REQUIRE(dec == conn.ParseType("DECIMAL(12,4)"));
	REQUIRE(dec.GetParamCount() == 2);
	auto width_param = dec.GetParam(0);
	REQUIRE(width_param.name.empty());
	REQUIRE(width_param.value.AsUtinyint() == 12);

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
	entries.push_back({"", Value::Varchar("sad")});
	entries.push_back({"", Value::Varchar("ok")});
	entries.push_back({"", Value::Varchar("happy")});
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
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(list.GetEnumSize(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
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
	REQUIRE_THROWS_MATCHES(Value::Bigint(1).AsType(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// LIST: children cast to the declared child type.
	auto list_type = conn.ParseType("BIGINT[]");
	std::vector<Value> elements;
	elements.push_back(Value::Bigint(1));
	elements.push_back(Value::Bigint(2));
	auto list = Value::Create(list_type, elements);
	REQUIRE(list.GetChildCount() == 2);
	REQUIRE(list.GetChild(1).AsBigint() == 2);

	// MAP: alternating key, value.
	auto map_type = conn.ParseType("MAP(VARCHAR, INTEGER)");
	std::vector<Value> entries;
	entries.push_back(Value::Varchar("a"));
	entries.push_back(Value::Bigint(1));
	auto map = Value::Create(map_type, entries);
	REQUIRE(map.GetChildCount() == 2);
	REQUIRE(map.GetChild(0).ToString() == "a");

	// UNION values are built via Cast, not Create.
	auto union_type = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	REQUIRE_THROWS_MATCHES(Value::Create(union_type, elements), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: Value::Cast through Context and Connection", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Connection sugar.
	auto parsed = Value::Varchar("42").Cast(conn, LogicalType::INTEGER());
	REQUIRE(parsed.AsInteger() == 42);

	// Connection form.
	auto date = Value::Varchar("2024-03-15").Cast(conn, conn.ParseType("DATE"));
	REQUIRE(date.ToString() == "2024-03-15");

	// UNION via cast: [0] = tag as UTINYINT, [1] = the active member.
	auto union_type = conn.ParseType("UNION(i INTEGER, s VARCHAR)");
	auto u = Value::Varchar("x").Cast(conn, union_type);
	REQUIRE(u.GetChildCount() == 2);
	REQUIRE(u.GetChild(0).AsUtinyint() == 1);
	REQUIRE(u.GetChild(1).ToString() == "x");

	// ENUM via cast.
	std::vector<TypeParam> entries;
	entries.push_back({"", Value::Varchar("sad")});
	entries.push_back({"", Value::Varchar("happy")});
	auto mood = conn.CreateType("enum", entries);
	auto happy = Value::Varchar("happy").Cast(conn, mood);
	REQUIRE(happy.GetLogicalType().GetId() == TypeId::ENUM);
	REQUIRE(happy.ToString() == "happy");

	// Cast failures carry the engine's code.
	REQUIRE_THROWS_MATCHES(Value::Varchar("abc").Cast(conn, LogicalType::INTEGER()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
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
	REQUIRE(vec.GetValue(0).AsBigint() == 0);
	REQUIRE(vec.GetValue(4).AsBigint() == 4);
	REQUIRE_THROWS_MATCHES(vec.GetValue(5), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// Write path: a manually built chunk, cast-on-write included.
	{
		std::vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT());
		DataChunk chunk(types);
		auto vec = chunk.GetVector(0);
		vec.SetSize(2);
		vec.SetValue(0, Value::Bigint(7));
		vec.SetValue(1, Value::Varchar("8")); // cast on write
		REQUIRE(vec.GetValue(0).AsBigint() == 7);
		REQUIRE(vec.GetValue(1).AsBigint() == 8);
		REQUIRE_THROWS_MATCHES(vec.SetValue(2, Value::Bigint(9)), Exception,
		                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
}
namespace {
// VARCHAR -> CELSIUS cast: parses "<digits>C", so success proves the
// registered cast ran (the default VARCHAR -> INTEGER cast rejects "21C").
void VarcharToCelsius(duckdb_api::CastFunction::ExecInput &input) {
	using duckdb_api::BytesLayout;
	auto in_vec = input.GetInput();
	auto out_vec = input.GetOutput();
	in_vec.Flatten();
	const auto *in = in_vec.GetDataMutable<const BytesLayout>();
	auto *out = out_vec.GetDataMutable<int32_t>();
	for (duckdb_api::idx_t i = 0; i < input.GetCount(); i++) {
		const auto &token = in[i];
		auto len = token.Length();
		const auto *data = token.Data();
		if (len < 2 || data[len - 1] != 'C') {
			throw duckdb_api::Exception(DUCKDB_V2_ERROR_INPUT_INVALID, "expected '<digits>C'");
		}
		int32_t parsed = 0;
		for (uint32_t b = 0; b + 1 < len; b++) {
			if (data[b] < '0' || data[b] > '9') {
				throw duckdb_api::Exception(DUCKDB_V2_ERROR_INPUT_INVALID, "expected '<digits>C'");
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
	{
		CustomType custom_type;
		custom_type.SetName("CELSIUS").SetBaseType(LogicalType::INTEGER()).Register(conn);

		// The registered type constructs through the generic constructor.
		auto celsius = conn.CreateType("celsius", {});
		CastFunction cast;
		cast.SetSourceType(LogicalType::VARCHAR())
		    .SetTargetType(celsius)
		    .SetImplicitCastCost(0)
		    .SetExecCallback(VarcharToCelsius)
		    .Register(conn);
	}

	auto celsius = conn.CreateType("celsius", {});
	REQUIRE(celsius.GetName() == "CELSIUS");
	REQUIRE(celsius.GetId() == TypeId::INTEGER);

	// An extension value builds straight from its backing bytes, keeping the
	// CELSIUS type (no cast function involved).
	int32_t raw = 21;
	auto direct = Value::FromData(celsius, &raw, sizeof(raw));
	REQUIRE(direct.GetLogicalType().GetName() == "CELSIUS");
	REQUIRE(direct.AsInteger() == 21);

	// Build a value through the registered cast.
	auto reading = Value::Varchar("21C").Cast(conn, celsius);
	REQUIRE(reading.AsInteger() == 21);
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
	REQUIRE(cell.AsInteger() == 21);
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
			entries.push_back({"", Value::Varchar("v" + std::to_string(i))});
		}
		auto mood = conn.CreateType("enum", entries);
		REQUIRE(mood.GetEnumInternalTypeId() == c.expected);
	}

	// Gated on the type kind like the other sugars.
	REQUIRE_THROWS_MATCHES(LogicalType::INTEGER().GetDecimalInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(LogicalType::INTEGER().GetEnumInternalTypeId(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
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
			return "INTEGER:" + std::to_string(inner.AsInteger());
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

	auto variant_type = conn.ParseType("VARIANT");
	std::vector<LogicalType> types;
	types.push_back(conn.ParseType("VARIANT"));
	DataChunk chunk(types);
	auto vec = chunk.GetVector(0);
	REQUIRE(vec.GetLogicalType().GetId() == TypeId::VARIANT);
	REQUIRE(vec.GetVectorType() == VectorType::Flat);
	vec.SetSize(5);

	// PROBED: SetValue casts on write to VARIANT engine-side (the
	// to-VARIANT cast needs no context), so plain values write directly.
	vec.SetValue(0, Value::Bigint(42));
	const char *heap_string = "a string long enough to spill";
	vec.SetValue(1, Value::Varchar(heap_string));
	auto list_type = conn.ParseType("INTEGER[]");
	std::vector<Value> elements;
	elements.push_back(Value::Bigint(1));
	elements.push_back(Value::Bigint(2));
	elements.push_back(Value::Bigint(3));
	vec.SetValue(2, Value::Create(list_type, elements));
	vec.SetValue(3, boxed_null);
	// The explicit route works too: box first, then write.
	vec.SetValue(4, Value::Bigint(43).Cast(conn, variant_type));

	// Read back through the boxed path: every non-NULL cell is a
	// VARIANT box; Cast back to the known inner type round-trips.
	for (idx_t row : {idx_t(0), idx_t(1), idx_t(2), idx_t(4)}) {
		auto box = vec.GetValue(row);
		REQUIRE_FALSE(box.IsNull());
		REQUIRE(box.GetLogicalType().GetId() == TypeId::VARIANT);
	}
	REQUIRE(vec.GetValue(0).ToString() == "42");
	REQUIRE(vec.GetValue(0).Cast(conn, conn.ParseType("BIGINT")).AsBigint() == 42);
	REQUIRE(vec.GetValue(1).Cast(conn, LogicalType::VARCHAR()).AsVarchar() == heap_string);
	auto unboxed_list = vec.GetValue(2).Cast(conn, list_type);
	REQUIRE(unboxed_list.GetChildCount() == 3);
	REQUIRE(unboxed_list.GetChild(2).AsInteger() == 3);
	REQUIRE(vec.GetValue(3).IsNull());
	REQUIRE(vec.GetValue(4).Cast(conn, conn.ParseType("BIGINT")).AsBigint() == 43);

	// MakeConstant interplay: the type-equality hardening refuses the
	// raw non-VARIANT value; the boxed value works.
	std::vector<LogicalType> constant_types;
	constant_types.push_back(conn.ParseType("VARIANT"));
	DataChunk constant_chunk(constant_types);
	auto cvec = constant_chunk.GetVector(0);
	REQUIRE_THROWS_MATCHES(cvec.MakeConstant(Value::Bigint(7), 3), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	auto boxed = Value::Bigint(7).Cast(conn, variant_type);
	cvec.MakeConstant(boxed, 3);
	REQUIRE(cvec.GetValue(2).GetLogicalType().GetId() == TypeId::VARIANT);
	REQUIRE(cvec.GetValue(2).ToString() == "7");
}
TEST_CASE("Stable C++API: typed Value leaf ctors/getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Bool / U64 / F64 round-trip through the typed constructors.
	REQUIRE(Value::Boolean(true).AsBoolean());
	REQUIRE_FALSE(Value::Boolean(false).AsBoolean());
	REQUIRE(Value::Ubigint(18446744073709551615ULL).AsUbigint() == 18446744073709551615ULL);
	REQUIRE(Value::Double(-2.5).AsDouble() == -2.5);

	// Blob: arbitrary bytes, including an embedded NUL.
	const uint8_t blob_bytes[] = {0x00, 0xFF, 0x10, 0x00, 0x42};
	auto blob_value = Value::Blob(blob_bytes, sizeof(blob_bytes));
	auto [blob_data, blob_len] = blob_value.AsBlob();
	REQUIRE(blob_len == sizeof(blob_bytes));
	REQUIRE(std::memcmp(blob_data, blob_bytes, blob_len) == 0);
	// Empty blob is legal too.
	auto empty_blob = Value::Blob(nullptr, 0);
	REQUIRE(empty_blob.AsBlob().second == 0);

	// Date: the engine is the oracle for the days-since-epoch encoding.
	// DATE - DATE yields a BIGINT day count, not an INTEGER.
	auto date_chunk = conn.Execute("SELECT (DATE '2024-03-15' - DATE '1970-01-01')").FetchChunk();
	auto date_days = static_cast<int32_t>(date_chunk.GetVector(0).GetValue(0).AsBigint());
	auto date_value = Value::Date(date_days);
	REQUIRE(date_value.GetLogicalType().GetId() == TypeId::DATE);
	REQUIRE(date_value.ToString() == "2024-03-15");
	REQUIRE(date_value.AsDate() == date_days);

	// Time: epoch_us() of a 1970-01-01 timestamp gives the time-of-day micros.
	auto time_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '1970-01-01 13:45:30.123456')")
	                       .FetchChunk()
	                       .GetVector(0)
	                       .GetValue(0)
	                       .AsBigint();
	auto time_value = Value::Time(time_micros);
	REQUIRE(time_value.GetLogicalType().GetId() == TypeId::TIME);
	REQUIRE(time_value.ToString() == "13:45:30.123456");
	REQUIRE(time_value.AsTime() == time_micros);

	// Timestamp: epoch_us() is the direct oracle.
	auto ts_micros = conn.Execute("SELECT epoch_us(TIMESTAMP '2024-03-15 13:45:30.123456')")
	                     .FetchChunk()
	                     .GetVector(0)
	                     .GetValue(0)
	                     .AsBigint();
	auto ts_value = Value::Timestamp(ts_micros);
	REQUIRE(ts_value.GetLogicalType().GetId() == TypeId::TIMESTAMP);
	REQUIRE(ts_value.ToString() == "2024-03-15 13:45:30.123456");
	REQUIRE(ts_value.AsTimestampRaw() == ts_micros);

	// TimestampTz: stored as UTC micros since epoch, same as TIMESTAMP;
	// cross-check against a TIMESTAMPTZ literal cast through the engine.
	auto tz_type = conn.ParseType("TIMESTAMP WITH TIME ZONE");
	auto engine_tz = Value::Varchar("2024-03-15 13:45:30.123456+00").Cast(conn, tz_type);
	auto tz_value = Value::TimestampTz(ts_micros);
	REQUIRE(tz_value.GetLogicalType().GetId() == TypeId::TIMESTAMP_TZ);
	REQUIRE(tz_value.AsTimestampRaw() == ts_micros);
	REQUIRE(tz_value.AsTimestampRaw() == engine_tz.AsTimestampRaw());

	// IntervalLayout: identity round trip plus a canonical-value cross-check.
	auto interval_value = Value::Interval({14, 3, 14706789000LL});
	auto decoded = interval_value.AsInterval();
	REQUIRE(decoded.months == 14);
	REQUIRE(decoded.days == 3);
	REQUIRE(decoded.micros == 14706789000LL);

	auto three_days = Value::Varchar("3 days").Cast(conn, conn.ParseType("INTERVAL"));
	auto three_days_decoded = three_days.AsInterval();
	REQUIRE(three_days_decoded.months == 0);
	REQUIRE(three_days_decoded.days == 3);
	REQUIRE(three_days_decoded.micros == 0);

	// A getter throws INVALID_INPUT on a type mismatch, keyed on the logical
	// type id, not the payload width.
	REQUIRE_THROWS_MATCHES(Value::Bigint(1).AsDate(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(Value::Boolean(true).AsInterval(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: typed Value getters reject a mismatched logical type", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	// Getters gate on the logical type id, so a same-width cross-type read is
	// rejected, not reinterpreted.
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// int64 group: BIGINT (8 bytes) is not TIME or TIMESTAMP.
	auto bigint_value = Value::Bigint(1234567890123LL);
	REQUIRE(bigint_value.GetLogicalType().GetId() == TypeId::BIGINT);
	REQUIRE_THROWS_MATCHES(bigint_value.AsTime(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(bigint_value.AsTimestampRaw(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(bigint_value.AsBlob(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// DOUBLE (8 bytes) is not TIME.
	REQUIRE_THROWS_MATCHES(Value::Double(3.5).AsTime(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// int32 group: INTEGER (4 bytes, same width as DATE) is not DATE. There
	// is no public FromI32, so build a genuine INTEGER value through Cast.
	auto integer_value = Value::Bigint(42).Cast(conn, LogicalType::INTEGER());
	REQUIRE(integer_value.GetLogicalType().GetId() == TypeId::INTEGER);
	REQUIRE_THROWS_MATCHES(integer_value.AsDate(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// AsBlob gates on the BLOB type: a BOOLEAN (1 byte) is rejected.
	REQUIRE_THROWS_MATCHES(Value::Boolean(true).AsBlob(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// AsTimestampRaw is polymorphic across the timestamp family only: an int64
	// TIME (8 bytes) is rejected.
	REQUIRE_THROWS_MATCHES(Value::Time(1).AsTimestampRaw(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// Positive control: every getter works on its own correct type.
	REQUIRE(Value::Date(42).AsDate() == 42);
	REQUIRE(Value::Time(999).AsTime() == 999);
	REQUIRE(Value::Timestamp(123456).AsTimestampRaw() == 123456);
	const uint8_t bytes[] = {0x01, 0x02, 0x03};
	REQUIRE(Value::Blob(bytes, sizeof(bytes)).AsBlob().second == sizeof(bytes));
	auto iv = Value::Interval({1, 2, 3}).AsInterval();
	REQUIRE(iv.months == 1);
	REQUIRE(iv.days == 2);
	REQUIRE(iv.micros == 3);

	// AsTimestampRaw accepts all five timestamp variants (each stored as int64
	// in its native unit). Built through the engine so the type ids are real.
	struct {
		const char *type_name;
		TypeId expected;
	} variants[] = {
	    {"TIMESTAMP", TypeId::TIMESTAMP},
	    {"TIMESTAMP_S", TypeId::TIMESTAMP_SEC},
	    {"TIMESTAMP_MS", TypeId::TIMESTAMP_MS},
	    {"TIMESTAMP_NS", TypeId::TIMESTAMP_NS},
	    {"TIMESTAMP WITH TIME ZONE", TypeId::TIMESTAMP_TZ},
	};
	for (auto &v : variants) {
		auto value = Value::Varchar("2024-03-15 13:45:30").Cast(conn, conn.ParseType(v.type_name));
		REQUIRE(value.GetLogicalType().GetId() == v.expected);
		REQUIRE_NOTHROW(value.AsTimestampRaw());
	}
}
TEST_CASE("Stable C++API: typed Value 128-bit getters round trip", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// HUGEINT halves: value == upper * 2^64 + lower. 2^64 is {0, 1}.
	auto big = Value::Hugeint({0, 1});
	REQUIRE(big.GetLogicalType().GetId() == TypeId::HUGEINT);
	REQUIRE(big.ToString() == "18446744073709551616");
	REQUIRE(big.AsHugeint().lower == 0);
	REQUIRE(big.AsHugeint().upper == 1);

	// A HUGEINT built by the engine reads back into the same halves.
	auto from_engine = Value::Varchar("18446744073709551616").Cast(conn, conn.ParseType("HUGEINT"));
	auto halves = from_engine.AsHugeint();
	REQUIRE(halves.lower == 0);
	REQUIRE(halves.upper == 1);
	REQUIRE(Value::Hugeint(halves).ToString() == from_engine.ToString());

	// Negative: -1 == {UINT64_MAX, -1}.
	auto neg = Value::Varchar("-1").Cast(conn, conn.ParseType("HUGEINT")).AsHugeint();
	REQUIRE(neg.lower == ~static_cast<uint64_t>(0));
	REQUIRE(neg.upper == -1);

	// UHUGEINT: 2^64 is {0, 1}.
	auto ubig = Value::Uhugeint({0, 1});
	REQUIRE(ubig.GetLogicalType().GetId() == TypeId::UHUGEINT);
	REQUIRE(ubig.ToString() == "18446744073709551616");
	REQUIRE(ubig.AsUhugeint().lower == 0);
	REQUIRE(ubig.AsUhugeint().upper == 1);

	// UUID decodes to its canonical 16 big-endian bytes (the storage's sort-order
	// high-bit flip is undone), matching the source string exactly.
	auto uuid = Value::Varchar("00112233-4455-6677-8899-aabbccddeeff").Cast(conn, conn.ParseType("UUID"));
	REQUIRE(uuid.GetLogicalType().GetId() == TypeId::UUID);
	REQUIRE(uuid.ToString() == "00112233-4455-6677-8899-aabbccddeeff");
	const uint8_t expected[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	                              0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
	REQUIRE(std::memcmp(uuid.AsUuid().bytes, expected, 16) == 0);

	// Type-mismatch guards, including between the two 128-bit widths.
	REQUIRE_THROWS_MATCHES(Value::Bigint(1).AsHugeint(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(big.AsUhugeint(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(big.AsUuid(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: TIME_TZ decodes to micros + offset", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// The engine builds the value; we decode it. 12:30:00 local, +02:00 east.
	auto tz = Value::Varchar("12:30:00+02:00").Cast(conn, conn.ParseType("TIME WITH TIME ZONE"));
	REQUIRE(tz.GetLogicalType().GetId() == TypeId::TIME_TZ);
	auto d = tz.AsTimeTz();
	REQUIRE(d.micros == 45000000000LL); // (12*3600 + 30*60) s
	REQUIRE(d.offset_seconds == 2 * 60 * 60);

	// A western offset.
	auto west = Value::Varchar("06:00:00-05:30").Cast(conn, conn.ParseType("TIME WITH TIME ZONE")).AsTimeTz();
	REQUIRE(west.micros == 6LL * 60 * 60 * 1000000);
	REQUIRE(west.offset_seconds == -(5 * 60 * 60 + 30 * 60));

	// Type-mismatch guard.
	REQUIRE_THROWS_MATCHES(Value::Bigint(1).AsTimeTz(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
TEST_CASE("Stable C++API: generic Value payload access (GetData / GetDataAs / FromData)", "[cpp_api][types_values]") {
	using namespace duckdb_api;
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	// Var-length: GetData borrows the decoded wire bytes (not string_t storage)
	// for the lifetime of the value, so the value must outlive the view.
	auto str_value = Value::Varchar("hello");
	auto [str_data, str_len] = str_value.GetData();
	REQUIRE(str_len == 5);
	REQUIRE(std::memcmp(str_data, "hello", 5) == 0);

	// GetDataAs is a size-checked typed copy of the committed layout.
	REQUIRE(Value::Bigint(42).GetDataAs<int64_t>() == 42);
	REQUIRE(Value::Hugeint({0, 1}).GetDataAs<HugeintLayout>().upper == 1);
	// A width mismatch is rejected (BIGINT is 8 bytes, not 4).
	REQUIRE_THROWS_MATCHES(Value::Bigint(1).GetDataAs<int32_t>(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// A NULL value has no payload.
	REQUIRE_THROWS_MATCHES(Value::Null(conn.ParseType("INTEGER")).GetData(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// FromData builds a value of any type from its committed layout, and it
	// round-trips through GetDataAs.
	HugeintLayout bits {0, 1};
	auto rebuilt = Value::FromData(conn.ParseType("HUGEINT"), &bits, sizeof(bits));
	REQUIRE(rebuilt.GetLogicalType().GetId() == TypeId::HUGEINT);
	REQUIRE(rebuilt.ToString() == "18446744073709551616");
	REQUIRE(rebuilt.GetDataAs<HugeintLayout>().lower == 0);
	REQUIRE(rebuilt.GetDataAs<HugeintLayout>().upper == 1);

	// The payoff: read a DECIMAL's raw backing integer (what a binding wants to
	// expose) with no VARCHAR cast. DECIMAL(10,2) backs on int64; 123.45 scales
	// to the unscaled integer 12345.
	auto decimal = Value::Varchar("123.45").Cast(conn, conn.ParseType("DECIMAL(10,2)"));
	REQUIRE(decimal.GetLogicalType().GetId() == TypeId::DECIMAL);
	REQUIRE(decimal.GetLogicalType().GetDecimalScale() == 2);
	REQUIRE(decimal.GetLogicalType().GetDecimalInternalTypeId() == TypeId::BIGINT);
	REQUIRE(decimal.GetDataAs<int64_t>() == 12345);

	// The layout structs are shared: the vector view path reads the same
	// HugeintLayout that the value path builds.
	{
		auto result = conn.Execute("SELECT 18446744073709551616::HUGEINT AS h");
		auto chunk = result.FetchChunk();
		auto view = chunk.GetVector(0).GetView();
		REQUIRE(view.Data<HugeintLayout>()[view.SelAt(0)].upper == 1);
	}
}
