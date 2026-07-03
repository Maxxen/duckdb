#include "capi_v2_test_helpers.hpp"
#include "capi_v2_internal.hpp"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// V2 logical_type tests.
//
// Fixtures are V2-native: primitives via create_from_id, composites via
// logical_type_create in a context scope (V2CreateType and its sugar),
// aliases via logical_type_create_with_alias. V1 appears here only as
// deliberate interop validation: two cross-version round-trip pins, one
// V1-built decimal oracle, and the V1-only zero-entry enum (invalid in SQL,
// so V2 refuses to build it; V2 inspection degrades gracefully), plus the
// MakeV1* seed builders those pins use.
//
// INVARIANT THIS TEST RELIES ON:
//   Both V1 and V2 logical_type handles are `new duckdb::LogicalType(...)`
//   cast to `void *`. V1's `duckdb_destroy_logical_type` and V2's
//   `duckdb_v2_logical_type_destroy` both perform
//   `delete static_cast<duckdb::LogicalType *>(handle)`. As long as that
//   stays true, destroying a V1-built handle through V2 destroy (and vice
//   versa) is correct. If V2 ever wraps the LogicalType in its own
//   struct, this file must change.
//
// We do NOT pass V2-built handles into V1 functions; the casting direction
// here is one-way V1 -> V2 for fixture setup only.
// ---------------------------------------------------------------------------

// ===========================================================================
// Lifecycle: create_from_id / destroy
// ===========================================================================

TEST_CASE("V2: logical_type create_from_id primitives", "[capi_v2][logical_type][lifecycle]") {
	struct {
		DUCKDB_V2_LOGICAL_TYPE_ID id;
	} cases[] = {
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_DATE},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BLOB},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BIT},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UUID},
	};
	for (auto &c : cases) {
		duckdb_v2_logical_type_handle type = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(c.id, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(type != nullptr);
		DUCKDB_V2_LOGICAL_TYPE_ID round_trip = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(type, &round_trip, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(round_trip == c.id);
		REQUIRE(duckdb_v2_logical_type_destroy(&type) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(type == nullptr);
	}
}

TEST_CASE("V2: logical_type create_from_id rejects parameterised ids", "[capi_v2][logical_type][lifecycle]") {
	DUCKDB_V2_LOGICAL_TYPE_ID rejected[] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL, DUCKDB_V2_LOGICAL_TYPE_ID_LIST,    DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_MAP,     DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,   DUCKDB_V2_LOGICAL_TYPE_ID_UNION,
	    DUCKDB_V2_LOGICAL_TYPE_ID_ENUM,    DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT, DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY,
	};
	for (auto id : rejected) {
		duckdb_v2_logical_type_handle type = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(id, &type, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(type == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}
}

TEST_CASE("V2: logical_type create_from_id rejects sentinel and bind-time-only ids",
          "[capi_v2][logical_type][lifecycle]") {
	// INVALID is the zero sentinel; SQLNULL / ANY / UNKNOWN exist for the
	// planner and UDF binding paths and have no read-side use in PR1.
	DUCKDB_V2_LOGICAL_TYPE_ID rejected[] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_INVALID,
	    DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL,
	    DUCKDB_V2_LOGICAL_TYPE_ID_ANY,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN,
	};
	for (auto id : rejected) {
		duckdb_v2_logical_type_handle type = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(id, &type, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(type == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}
}

TEST_CASE("V2: logical_type create_from_id null out param", "[capi_v2][logical_type][lifecycle]") {
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: logical_type create_from_id leaves pre-existing err untouched on success",
          "[capi_v2][logical_type][lifecycle]") {
	// Belt-and-braces check of the error-info contract: on success the
	// library leaves the slot untouched. A stale info from a prior failure
	// survives; the return code is authoritative.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL, nullptr, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);

	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_code_t code = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_get_code(err, &code);
	REQUIRE(code == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type destroy is null-safe", "[capi_v2][logical_type][lifecycle]") {
	// Passing a nullptr slot pointer is a no-op.
	REQUIRE(duckdb_v2_logical_type_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	// Passing a slot that already holds nullptr is a no-op.
	duckdb_v2_logical_type_handle already_null = nullptr;
	REQUIRE(duckdb_v2_logical_type_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

// ===========================================================================
// Common introspection: get_id, get_name
// ===========================================================================

TEST_CASE("V2: logical_type get_id null handle / null out", "[capi_v2][logical_type][id]") {
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN;
	REQUIRE(duckdb_v2_logical_type_get_id(nullptr, &id, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_get_id(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type get_name is the canonical id name when no alias is set", "[capi_v2][logical_type][name]") {
	// Never the empty view: the un-aliased arm returns the id's canonical
	// fixed name, exactly the vocabulary logical_type_create consumes.
	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	duckdb_v2_str name = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(t, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "INTEGER");
	duckdb_v2_logical_type_destroy(&t);

	V2EnvFixture fx;
	auto dec = V2CreateType(fx.conn, "decimal", nullptr, {V2Int32Value(18), V2Int32Value(3)});
	REQUIRE(duckdb_v2_logical_type_get_name(dec, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "DECIMAL");
	duckdb_v2_logical_type_destroy(&dec);

	// The spaced canonical spellings come through verbatim.
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_get_name(t, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "TIMESTAMP WITH TIME ZONE");
	REQUIRE(name.len > 0);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type get_name prefers the alias when set", "[capi_v2][logical_type][name]") {
	auto base = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_handle t = nullptr;
	auto alias_rc = duckdb_v2_logical_type_create_with_alias(base, V2Str("my_int"), &t, nullptr);
	duckdb_v2_logical_type_destroy(&base);
	REQUIRE(alias_rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_str alias = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(t, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias == "my_int");

	// Borrowed pointer is valid as long as the type is alive: a second call
	// returns the same string contents.
	duckdb_v2_str alias2 = {nullptr, 0};
	duckdb_v2_logical_type_get_name(t, &alias2, nullptr);
	REQUIRE(alias2 == "my_int");

	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type get_name reads an alias set on a STRUCT", "[capi_v2][logical_type][name]") {
	// Mirrors the spatial / extension-type path: a composite with an alias
	// (e.g. STRUCT{x:double, y:double} aliased "POINT_2D").
	V2EnvFixture fx;
	auto base = V2StructType(fx.conn, {"x", "y"}, {DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE});
	duckdb_v2_logical_type_handle t = nullptr;
	auto alias_rc = duckdb_v2_logical_type_create_with_alias(base, V2Str("POINT_2D"), &t, nullptr);
	duckdb_v2_logical_type_destroy(&base);
	REQUIRE(alias_rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_str alias = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(t, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias == "POINT_2D");

	// The id is still STRUCT — alias is metadata, not type identity.
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(t, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);

	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type get_name null handle / null out", "[capi_v2][logical_type][name]") {
	duckdb_v2_str alias = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(nullptr, &alias, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_get_name(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t);
}

// ===========================================================================
// Fixtures shared by the sections below (V2-built via a context scope)
// ===========================================================================

namespace {

duckdb_v2_logical_type_handle MakeEnum(duckdb_v2_connection_handle conn, const char **values, idx_t count) {
	std::vector<duckdb_v2_value_handle> entries;
	for (idx_t i = 0; i < count; i++) {
		entries.push_back(V2VarcharValue(values[i]));
	}
	return V2CreateType(conn, "enum", nullptr, std::move(entries));
}

duckdb_v2_logical_type_handle MakeStruct(duckdb_v2_connection_handle conn) {
	return V2StructType(conn, {"id", "name"}, {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
}

duckdb_v2_logical_type_handle MakeUnion(duckdb_v2_connection_handle conn) {
	std::vector<const char *> names = {"i", "s"};
	return V2CreateType(
	    conn, "union", &names,
	    {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
}

// V1 seed builders for the interop pins below: the two cross-version
// round-trip tests and the V1-only zero-entry enum pin.
duckdb_v2_logical_type_handle MakeV1Struct() {
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	const char *names[2] = {"id", "name"};
	auto v1 = duckdb_create_struct_type(members, names, 2);
	// Free the member fixtures before any REQUIRE: Catch2 throws on failure
	// and would otherwise skip these destroys, leaking the LogicalTypes.
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

duckdb_v2_logical_type_handle MakeV1Union() {
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	const char *names[2] = {"i", "s"};
	auto v1 = duckdb_create_union_type(members, names, 2);
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

duckdb_v2_logical_type_handle MakeV1Enum(const char **values, idx_t count) {
	auto v1 = duckdb_create_enum_type(values, count);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

// ===========================================================================
// to_text / create_from_text
// ===========================================================================

std::string V2TypeText(duckdb_v2_logical_type_handle t) {
	char *text = nullptr;
	REQUIRE(duckdb_v2_logical_type_to_text(t, &text, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(text != nullptr);
	std::string out(text);
	std::free(text);
	return out;
}

duckdb_v2_logical_type_handle V2TypeFromText(duckdb_v2_connection_handle conn, const std::string &text) {
	duckdb_v2_logical_type_handle t = nullptr;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str(text), &t, nullptr) == DUCKDB_V2_ERROR_NONE);
	});
	REQUIRE(t != nullptr);
	return t;
}

DUCKDB_V2_LOGICAL_TYPE_ID V2TypeIdOf(duckdb_v2_logical_type_handle t) {
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(t, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	return id;
}

bool V2TypesEqual(duckdb_v2_logical_type_handle a, duckdb_v2_logical_type_handle b) {
	bool equal = false;
	REQUIRE(duckdb_v2_logical_type_is_equal(a, b, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	return equal;
}

// ---------------------------------------------------------------------------
// Param inspection helpers
// ---------------------------------------------------------------------------

idx_t V2ParamCount(duckdb_v2_logical_type_handle t) {
	idx_t count = 99;
	REQUIRE(duckdb_v2_logical_type_get_param_count(t, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	return count;
}

// Reads param `index`, requiring the name to match (nullptr = positional,
// i.e. the {NULL, 0} view). Returns the owned value; caller destroys.
duckdb_v2_value_handle V2GetParam(duckdb_v2_logical_type_handle t, idx_t index, const char *expected_name) {
	duckdb_v2_str name = {reinterpret_cast<const char *>(0x1), 99};
	duckdb_v2_value_handle value = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_param(t, index, &name, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(value != nullptr);
	// Destroy before failing so a name mismatch cannot leak the value.
	const bool name_ok = expected_name ? (name == expected_name) : (name.ptr == nullptr && name.len == 0);
	if (!name_ok) {
		auto got = V2StrTo(name);
		duckdb_v2_value_destroy(&value);
		FAIL("param name mismatch at index " << index << ": got '" << got << "'");
	}
	return value;
}

// Unwraps a TYPE param into an owned logical type.
duckdb_v2_logical_type_handle V2ParamType(duckdb_v2_logical_type_handle t, idx_t index, const char *expected_name) {
	auto v = V2GetParam(t, index, expected_name);
	duckdb_v2_logical_type_handle out = nullptr;
	auto rc = duckdb_v2_value_get_type(v, &out, nullptr);
	duckdb_v2_value_destroy(&v);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return out;
}

uint8_t V2ParamU8(duckdb_v2_logical_type_handle t, idx_t index) {
	auto v = V2GetParam(t, index, nullptr);
	return V2LeafPayloadConsume<uint8_t>(v);
}

int64_t V2ParamI64(duckdb_v2_logical_type_handle t, idx_t index) {
	auto v = V2GetParam(t, index, nullptr);
	return V2LeafPayloadConsume<int64_t>(v);
}

std::string V2ParamVarchar(duckdb_v2_logical_type_handle t, idx_t index, const char *expected_name) {
	auto v = V2GetParam(t, index, expected_name);
	return V2LeafBytesConsume(v);
}

// ---------------------------------------------------------------------------
// Param value builders + create helpers
// ---------------------------------------------------------------------------

// Same, expecting failure: checks out nulling + err population, destroys the
// borrowed values, and returns the code.
duckdb_v2_error_code_t V2CreateTypeErr(duckdb_v2_connection_handle conn, const char *name,
                                       const std::vector<const char *> *names,
                                       std::vector<duckdb_v2_value_handle> values) {
	std::vector<duckdb_v2_str> name_views;
	if (names) {
		for (auto *n : *names) {
			name_views.push_back(V2Str(n));
		}
	}
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	bool out_nulled = false;
	bool err_set = false;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		auto t = reinterpret_cast<duckdb_v2_logical_type_handle>(0x1);
		duckdb_v2_error_info_handle err = nullptr;
		rc = duckdb_v2_logical_type_create(ctx, V2Str(name), names ? name_views.data() : nullptr,
		                                   values.empty() ? nullptr : values.data(), values.size(), &t, &err);
		out_nulled = (t == nullptr);
		err_set = (err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	});
	// Assert only after the fixtures are destroyed, so a failure cannot leak.
	for (auto &v : values) {
		duckdb_v2_value_destroy(&v);
	}
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_nulled);
	REQUIRE(err_set);
	return rc;
}

// Reconstruction name: get_name returns exactly the vocabulary
// logical_type_create consumes (alias when set, else the canonical id
// name), so the duality below holds by construction.
std::string V2KindName(duckdb_v2_logical_type_handle t) {
	duckdb_v2_str name = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(t, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name.len > 0);
	return V2StrTo(name);
}

// The param round trip the design pins: create(name, params(t)) equals t.
void RequireParamRoundTrip(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle t) {
	idx_t count = V2ParamCount(t);
	std::vector<std::string> name_storage(count);
	std::vector<duckdb_v2_str> names(count);
	std::vector<duckdb_v2_value_handle> values(count, nullptr);
	bool any_named = false;
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_str name = {nullptr, 0};
		auto param_rc = duckdb_v2_logical_type_get_param(t, i, &name, &values[i], nullptr);
		if (param_rc != DUCKDB_V2_ERROR_NONE) {
			// Destroy the values collected so far before failing.
			for (auto &v : values) {
				duckdb_v2_value_destroy(&v);
			}
		}
		REQUIRE(param_rc == DUCKDB_V2_ERROR_NONE);
		if (name.ptr) {
			name_storage[i] = V2StrTo(name);
			names[i] = V2Str(name_storage[i]);
			any_named = true;
		} else {
			names[i] = duckdb_v2_str {nullptr, 0};
		}
	}
	auto kind_name = V2KindName(t);
	duckdb_v2_logical_type_handle rebuilt = nullptr;
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		rc = duckdb_v2_logical_type_create(ctx, V2Str(kind_name), any_named ? names.data() : nullptr,
		                                   values.empty() ? nullptr : values.data(), values.size(), &rebuilt, nullptr);
	});
	for (auto &v : values) {
		duckdb_v2_value_destroy(&v);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	const bool equal = V2TypesEqual(t, rebuilt);
	duckdb_v2_logical_type_destroy(&rebuilt);
	REQUIRE(equal);
}

} // namespace

TEST_CASE("V2: LOGICAL_TYPE_ID_TYPE mirrors duckdb::LogicalTypeId::TYPE", "[capi_v2][logical_type][type_value]") {
	REQUIRE(static_cast<uint32_t>(duckdb::LogicalTypeId::TYPE) ==
	        static_cast<uint32_t>(DUCKDB_V2_LOGICAL_TYPE_ID_TYPE));
}

TEST_CASE("V2: logical_type to_text renders primitives", "[capi_v2][logical_type][to_text]") {
	struct {
		DUCKDB_V2_LOGICAL_TYPE_ID id;
		const char *expected;
	} cases[] = {
	    {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, "INTEGER"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, "VARCHAR"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, "BOOLEAN"},
	    // Engine spellings for the tz kinds carry spaces; pinned here because
	    // from_text must accept exactly what to_text emits.
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ, "TIMESTAMP WITH TIME ZONE"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ, "TIME WITH TIME ZONE"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC, "TIMESTAMP_S"},
	};
	for (auto &c : cases) {
		duckdb_v2_logical_type_handle t = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(c.id, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2TypeText(t) == c.expected);
		duckdb_v2_logical_type_destroy(&t);
	}
}

TEST_CASE("V2: logical_type to_text renders composites", "[capi_v2][logical_type][to_text]") {
	V2EnvFixture fx;

	auto dec = V2CreateType(fx.conn, "decimal", nullptr, {V2Int32Value(18), V2Int32Value(3)});
	REQUIRE(V2TypeText(dec) == "DECIMAL(18,3)");
	duckdb_v2_logical_type_destroy(&dec);

	auto list = V2ListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(V2TypeText(list) == "INTEGER[]");
	duckdb_v2_logical_type_destroy(&list);

	auto arr =
	    V2CreateType(fx.conn, "array", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2Int32Value(3)});
	REQUIRE(V2TypeText(arr) == "INTEGER[3]");
	duckdb_v2_logical_type_destroy(&arr);

	auto map = V2MapType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(V2TypeText(map) == "MAP(VARCHAR, INTEGER)");
	duckdb_v2_logical_type_destroy(&map);

	auto s = MakeStruct(fx.conn); // STRUCT(id INTEGER, name VARCHAR)
	// SQLIdentifier quotes field names that collide with keywords ("name").
	REQUIRE(V2TypeText(s) == "STRUCT(id INTEGER, \"name\" VARCHAR)");
	duckdb_v2_logical_type_destroy(&s);

	auto u = MakeUnion(fx.conn); // UNION(i INTEGER, s VARCHAR)
	REQUIRE(V2TypeText(u) == "UNION(i INTEGER, s VARCHAR)");
	duckdb_v2_logical_type_destroy(&u);

	const char *enum_names[] = {"a", "bb"};
	auto e = MakeEnum(fx.conn, enum_names, 2);
	REQUIRE(V2TypeText(e) == "ENUM('a', 'bb')");
	duckdb_v2_logical_type_destroy(&e);
}

TEST_CASE("V2: logical_type to_text renders an aliased type as its alias", "[capi_v2][logical_type][to_text]") {
	auto base = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_handle t = nullptr;
	auto alias_rc = duckdb_v2_logical_type_create_with_alias(base, V2Str("my_int"), &t, nullptr);
	duckdb_v2_logical_type_destroy(&base);
	REQUIRE(alias_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2TypeText(t) == "my_int");
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type to_text null handle / null out", "[capi_v2][logical_type][to_text]") {
	char *text = nullptr;
	REQUIRE(duckdb_v2_logical_type_to_text(nullptr, &text, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_to_text(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type create_from_text parses primitives, case-insensitive",
          "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;

	auto t = V2TypeFromText(f.conn, "INTEGER");
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&t);

	t = V2TypeFromText(f.conn, "integer");
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&t);

	// TYPE is a first-class type name: the type of TYPE values.
	t = V2TypeFromText(f.conn, "TYPE");
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_TYPE);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type create_from_text parses parameterized kinds", "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;

	auto dec = V2TypeFromText(f.conn, "DECIMAL(18,3)");
	REQUIRE(V2TypeIdOf(dec) == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL);
	REQUIRE(V2ParamU8(dec, 0) == 18);
	REQUIRE(V2ParamU8(dec, 1) == 3);
	duckdb_v2_logical_type_destroy(&dec);

	auto list = V2TypeFromText(f.conn, "INTEGER[]");
	REQUIRE(V2TypeIdOf(list) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	auto child = V2ParamType(list, 0, nullptr);
	REQUIRE(V2TypeIdOf(child) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&child);
	duckdb_v2_logical_type_destroy(&list);

	auto arr = V2TypeFromText(f.conn, "INTEGER[3]");
	REQUIRE(V2TypeIdOf(arr) == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY);
	REQUIRE(V2ParamI64(arr, 1) == 3);
	duckdb_v2_logical_type_destroy(&arr);

	auto s = V2TypeFromText(f.conn, "STRUCT(a INTEGER, b VARCHAR)");
	REQUIRE(V2TypeIdOf(s) == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);
	auto field_a = V2ParamType(s, 0, "a");
	REQUIRE(V2TypeIdOf(field_a) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&field_a);
	auto field_b = V2ParamType(s, 1, "b");
	REQUIRE(V2TypeIdOf(field_b) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&field_b);
	duckdb_v2_logical_type_destroy(&s);

	auto map = V2TypeFromText(f.conn, "MAP(VARCHAR, INTEGER)");
	REQUIRE(V2TypeIdOf(map) == DUCKDB_V2_LOGICAL_TYPE_ID_MAP);
	duckdb_v2_logical_type_destroy(&map);

	auto u = V2TypeFromText(f.conn, "UNION(i INTEGER, s VARCHAR)");
	REQUIRE(V2TypeIdOf(u) == DUCKDB_V2_LOGICAL_TYPE_ID_UNION);
	duckdb_v2_logical_type_destroy(&u);

	auto e = V2TypeFromText(f.conn, "ENUM('x', 'y')");
	REQUIRE(V2TypeIdOf(e) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	REQUIRE(V2ParamCount(e) == 2);
	REQUIRE(V2ParamVarchar(e, 1, nullptr) == "y");
	duckdb_v2_logical_type_destroy(&e);

	// Deep nesting: composite parameters compose recursively.
	auto deep = V2TypeFromText(f.conn, "STRUCT(a INTEGER[], m MAP(VARCHAR, DECIMAL(10,2)))[]");
	REQUIRE(V2TypeIdOf(deep) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	REQUIRE(V2TypeText(deep) == "STRUCT(a INTEGER[], m MAP(VARCHAR, DECIMAL(10,2)))[]");
	duckdb_v2_logical_type_destroy(&deep);
}

// Deliberate cross-version pin: the composite fixtures are V1-built and the
// V2 reconstruction is compared against them via logical_type_is_equal.
TEST_CASE("V2: logical_type from_text(to_text) round-trips constructible kinds",
          "[capi_v2][logical_type][from_text][to_text]") {
	V2EnvFixture f;

	auto require_round_trip = [&](duckdb_v2_logical_type_handle t) {
		auto text = V2TypeText(t);
		auto parsed = V2TypeFromText(f.conn, text);
		REQUIRE(V2TypesEqual(t, parsed));
		// And the other direction: the canonical rendering is stable.
		REQUIRE(V2TypeText(parsed) == text);
		duckdb_v2_logical_type_destroy(&parsed);
	};

	// Every id create_from_id accepts.
	DUCKDB_V2_LOGICAL_TYPE_ID primitives[] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
	    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
	    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
	};
	for (auto id : primitives) {
		duckdb_v2_logical_type_handle t = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(id, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
		require_round_trip(t);
		duckdb_v2_logical_type_destroy(&t);
	}

	// Composites (built via V1 fixtures), including nesting.
	auto dec = V1ToV2(duckdb_create_decimal_type(18, 3));
	require_round_trip(dec);
	duckdb_v2_logical_type_destroy(&dec);

	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	auto list = V1ToV2(duckdb_create_list_type(list_v1)); // INTEGER[][]
	duckdb_destroy_logical_type(&list_v1);
	require_round_trip(list);
	duckdb_v2_logical_type_destroy(&list);

	auto arr = V1ToV2(duckdb_create_array_type(int_v1, 7));
	require_round_trip(arr);
	duckdb_v2_logical_type_destroy(&arr);

	auto varchar_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto map = V1ToV2(duckdb_create_map_type(varchar_v1, int_v1));
	require_round_trip(map);
	duckdb_v2_logical_type_destroy(&map);
	duckdb_destroy_logical_type(&int_v1);
	duckdb_destroy_logical_type(&varchar_v1);

	auto s = MakeV1Struct();
	require_round_trip(s);
	duckdb_v2_logical_type_destroy(&s);

	auto u = MakeV1Union();
	require_round_trip(u);
	duckdb_v2_logical_type_destroy(&u);

	const char *enum_names[] = {"sad", "ok", "happy"};
	auto e = MakeV1Enum(enum_names, 3);
	require_round_trip(e);
	duckdb_v2_logical_type_destroy(&e);

	// Kinds only reachable through from_text itself.
	auto type_t = V2TypeFromText(f.conn, "TYPE");
	require_round_trip(type_t);
	duckdb_v2_logical_type_destroy(&type_t);

	auto geom = V2TypeFromText(f.conn, "GEOMETRY");
	REQUIRE(V2TypeIdOf(geom) == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY);
	require_round_trip(geom);
	duckdb_v2_logical_type_destroy(&geom);
}

TEST_CASE("V2: logical_type create_from_text resolves a catalog type", "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	auto t = V2TypeFromText(f.conn, "mood");
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	REQUIRE(V2ParamCount(t) == 3);
	REQUIRE(V2ParamVarchar(t, 2, nullptr) == "happy");

	// The bound type is structural: this path attaches no alias, so
	// get_name falls back to the canonical id name and to_text renders the
	// dictionary form, which round-trips.
	duckdb_v2_str name = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(t, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "ENUM");
	auto text = V2TypeText(t);
	REQUIRE(text == "ENUM('sad', 'ok', 'happy')");
	auto again = V2TypeFromText(f.conn, text);
	REQUIRE(V2TypesEqual(t, again));
	duckdb_v2_logical_type_destroy(&again);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type create_from_text error paths", "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;
	V2WithContext(f.conn, [&](duckdb_v2_context_handle ctx) {
		duckdb_v2_logical_type_handle t = nullptr;
		duckdb_v2_error_info_handle err = nullptr;

		// Unresolvable type name: binder/catalog error surfaces from the call.
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str("definitely_not_a_type"), &t, &err) !=
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(t == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// Unparseable type expression.
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str("INTEGER[["), &t, &err) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(t == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// Empty text ({NULL, 0} is a valid empty view; parsing it fails).
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, duckdb_v2_str {nullptr, 0}, &t, &err) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(t == nullptr);
		duckdb_v2_error_info_destroy(&err);
	});
}

TEST_CASE("V2: logical_type create_from_text null-arg refusals", "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;
	duckdb_v2_logical_type_handle t = nullptr;

	// A null context is refused without any scope.
	REQUIRE(duckdb_v2_logical_type_create_from_text(nullptr, V2Str("INTEGER"), &t, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);

	V2WithContext(f.conn, [&](duckdb_v2_context_handle ctx) {
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str("INTEGER"), nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		// Malformed view: null pointer with nonzero length.
		duckdb_v2_logical_type_handle out = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, duckdb_v2_str {nullptr, 3}, &out, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(out == nullptr);
	});
}

namespace {
// The conn-level dance pinned as a contract: enter a context scope through
// connection_execute_with_context, construct there, use the type after the
// scope exits.
void BuildDecimalFromText(duckdb_v2_context_handle ctx, void *user_data, duckdb_v2_error_info_handle *err) {
	auto *out = static_cast<duckdb_v2_logical_type_handle *>(user_data);
	duckdb_v2_logical_type_create_from_text(ctx, V2Str("DECIMAL(12,4)"), out, err);
}
} // namespace

TEST_CASE("V2: create_from_text runs in a context scope and outlives it", "[capi_v2][logical_type][from_text]") {
	V2EnvFixture f;
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_connection_execute_with_context(f.conn, BuildDecimalFromText, &t, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(t != nullptr);
	// Fully usable after the scope exits.
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL);
	REQUIRE(V2ParamU8(t, 0) == 12);
	REQUIRE(V2ParamU8(t, 1) == 4);
	REQUIRE(V2TypeText(t) == "DECIMAL(12,4)");
	duckdb_v2_logical_type_destroy(&t);
}

namespace {
// Bind-phase callback path: a scalar function bind callback constructs types
// on the context it receives (catalog lookup included).
bool type_probe_bind_ok = false;

void TypeProbeBind(duckdb_v2_scalar_function_bind_args *args, duckdb_v2_error_info_handle *err) {
	type_probe_bind_ok = false;
	duckdb_v2_logical_type_handle mood = nullptr;
	if (duckdb_v2_logical_type_create_from_text(args->context, V2Str("mood"), &mood, err) != DUCKDB_V2_ERROR_NONE) {
		return; // nested call populated the borrowed err slot
	}
	duckdb_v2_value_handle elem = nullptr;
	if (duckdb_v2_value_create_type(mood, &elem, err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_destroy(&mood);
		return;
	}
	duckdb_v2_logical_type_handle list = nullptr;
	const duckdb_v2_value_handle params[1] = {elem};
	auto rc = duckdb_v2_logical_type_create(args->context, V2Str("list"), nullptr, params, 1, &list, err);
	duckdb_v2_value_destroy(&elem);
	duckdb_v2_logical_type_destroy(&mood);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(list, &id, err);
	duckdb_v2_logical_type_destroy(&list);
	type_probe_bind_ok = (id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
}

void TypeProbeInit(duckdb_v2_scalar_function_init_args *, duckdb_v2_error_info_handle *) {
}

void TypeProbeExec(duckdb_v2_scalar_function_exec_args *args, duckdb_v2_error_info_handle *err) {
	duckdb_v2_vector_handle in_vec = nullptr;
	if (duckdb_v2_data_chunk_get_vector(args->input, 0, &in_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_view view {};
	if (duckdb_v2_vector_get_view(in_vec, &view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int32_t *out = nullptr;
	if (duckdb_v2_vector_get_data_mutable(args->result, reinterpret_cast<void **>(&out), err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto *in = static_cast<const int32_t *>(view.data);
	for (idx_t i = 0; i < view.count; i++) {
		out[i] = in[view.sel ? view.sel[i] : i];
	}
}
} // namespace

TEST_CASE("V2: a scalar function bind callback constructs types on its context",
          "[capi_v2][logical_type][from_text][create]") {
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	V2WithContext(f.conn, [](duckdb_v2_context_handle ctx) {
		duckdb_v2_scalar_function_builder_handle builder = nullptr;
		REQUIRE(duckdb_v2_scalar_function_builder_create(ctx, &builder, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_logical_type_handle int_type = nullptr;
		duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, nullptr);
		REQUIRE(duckdb_v2_scalar_function_builder_set_name(builder, V2Str("type_probe"), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_add_parameter(builder, V2Str("a"), int_type, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_set_return_type(builder, int_type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(builder, TypeProbeBind, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_set_init_callback(builder, TypeProbeInit, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(builder, TypeProbeExec, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_register(ctx, builder, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_scalar_function_builder_destroy(&builder);
		duckdb_v2_logical_type_destroy(&int_type);
	});

	type_probe_bind_ok = false;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(f.conn, "SELECT type_probe(7)", &r) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	REQUIRE(static_cast<const int32_t *>(view.data)[0] == 7);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&r);
	REQUIRE(type_probe_bind_ok);
}

TEST_CASE("V2: type construction does not disturb a live streaming result",
          "[capi_v2][logical_type][from_text][create][streaming]") {
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	V2Result r;
	REQUIRE(V2Query(f.conn, "SELECT * FROM range(10000)", &r) == DUCKDB_V2_ERROR_NONE);
	auto first = V2StepChunk(r);
	REQUIRE(first != nullptr);
	idx_t seen = 0;
	duckdb_v2_data_chunk_get_size(first, &seen, nullptr);
	duckdb_v2_data_chunk_destroy(&first);

	// The context scope opens on the live stream's transaction; parse-only,
	// catalog-lookup, and generic construction all run inside it without
	// cancelling the stream. Do not step the stream inside the scope.
	V2WithContext(f.conn, [&](duckdb_v2_context_handle ctx) {
		duckdb_v2_logical_type_handle list = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str("INTEGER[]"), &list, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2TypeIdOf(list) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
		duckdb_v2_logical_type_destroy(&list);

		duckdb_v2_logical_type_handle mood = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_text(ctx, V2Str("mood"), &mood, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2TypeIdOf(mood) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
		duckdb_v2_logical_type_destroy(&mood);

		duckdb_v2_value_handle elem = V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
		const duckdb_v2_value_handle params[1] = {elem};
		duckdb_v2_logical_type_handle built = nullptr;
		auto rc = duckdb_v2_logical_type_create(ctx, V2Str("list"), nullptr, params, 1, &built, nullptr);
		duckdb_v2_value_destroy(&elem);
		REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2TypeIdOf(built) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
		duckdb_v2_logical_type_destroy(&built);
	});

	// The stream is still live and drains to completion.
	REQUIRE(seen + V2DrainRowCount(r) == 10000);
}

// ===========================================================================
// Generic parameter inspection: get_param_count / get_param
// ===========================================================================

TEST_CASE("V2: parameterless kinds report zero params", "[capi_v2][logical_type][param]") {
	V2EnvFixture f;
	DUCKDB_V2_LOGICAL_TYPE_ID ids[] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
	    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
	};
	for (auto id : ids) {
		duckdb_v2_logical_type_handle t = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(id, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2ParamCount(t) == 0);
		duckdb_v2_logical_type_destroy(&t);
	}
	// TYPE and (CRS-less) GEOMETRY carry no parameters either.
	auto type_t = V2TypeFromText(f.conn, "TYPE");
	REQUIRE(V2ParamCount(type_t) == 0);
	duckdb_v2_logical_type_destroy(&type_t);
	auto geom = V2TypeFromText(f.conn, "GEOMETRY");
	REQUIRE(V2ParamCount(geom) == 0);
	duckdb_v2_logical_type_destroy(&geom);
}

TEST_CASE("V2: DECIMAL params are width and scale as UTINYINT", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	auto t = V2CreateType(fx.conn, "decimal", nullptr, {V2Int32Value(18), V2Int32Value(3)});
	REQUIRE(V2ParamCount(t) == 2);
	REQUIRE(V2ParamU8(t, 0) == 18);
	REQUIRE(V2ParamU8(t, 1) == 3);
	// The param values are UTINYINT typed.
	auto v = V2GetParam(t, 0, nullptr);
	duckdb_v2_logical_type_handle vt = nullptr;
	duckdb_v2_value_get_logical_type(v, &vt, nullptr);
	REQUIRE(V2TypeIdOf(vt) == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);
	duckdb_v2_logical_type_destroy(&vt);
	duckdb_v2_value_destroy(&v);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: LIST and ARRAY params carry the element type as a TYPE value", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	auto inner_list = V2ListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto nested = V2CreateType(fx.conn, "list", nullptr, {V2TypeValueOf(inner_list)}); // INTEGER[][]

	REQUIRE(V2ParamCount(nested) == 1);
	auto inner = V2ParamType(nested, 0, nullptr);
	REQUIRE(V2TypeIdOf(inner) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	auto leaf = V2ParamType(inner, 0, nullptr);
	REQUIRE(V2TypeIdOf(leaf) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&leaf);
	duckdb_v2_logical_type_destroy(&inner);
	duckdb_v2_logical_type_destroy(&nested);

	auto arr = V2CreateType(fx.conn, "array", nullptr, {V2TypeValueOf(inner_list), V2Int32Value(7)}); // INTEGER[][7]
	duckdb_v2_logical_type_destroy(&inner_list);
	REQUIRE(V2ParamCount(arr) == 2);
	auto elem = V2ParamType(arr, 0, nullptr);
	REQUIRE(V2TypeIdOf(elem) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	duckdb_v2_logical_type_destroy(&elem);
	REQUIRE(V2ParamI64(arr, 1) == 7);
	duckdb_v2_logical_type_destroy(&arr);
}

TEST_CASE("V2: MAP params are key and value types", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	auto map = V2MapType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	REQUIRE(V2ParamCount(map) == 2);
	auto key = V2ParamType(map, 0, nullptr);
	REQUIRE(V2TypeIdOf(key) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&key);
	auto val = V2ParamType(map, 1, nullptr);
	REQUIRE(V2TypeIdOf(val) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&val);
	duckdb_v2_logical_type_destroy(&map);
}

TEST_CASE("V2: STRUCT and UNION params are named TYPE values", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	auto s = MakeStruct(fx.conn); // STRUCT(id INTEGER, name VARCHAR)
	REQUIRE(V2ParamCount(s) == 2);
	auto id_field = V2ParamType(s, 0, "id");
	REQUIRE(V2TypeIdOf(id_field) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&id_field);
	auto name_field = V2ParamType(s, 1, "name");
	REQUIRE(V2TypeIdOf(name_field) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&name_field);
	duckdb_v2_logical_type_destroy(&s);

	auto u = MakeUnion(fx.conn); // UNION(i INTEGER, s VARCHAR)
	REQUIRE(V2ParamCount(u) == 2);
	auto member = V2ParamType(u, 1, "s");
	REQUIRE(V2TypeIdOf(member) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&member);
	duckdb_v2_logical_type_destroy(&u);
}

TEST_CASE("V2: ENUM params are the dictionary entries as VARCHAR", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	const char *names[] = {"a", "bb", "ccc"};
	auto e = MakeEnum(fx.conn, names, 3);
	REQUIRE(V2ParamCount(e) == 3);
	REQUIRE(V2ParamVarchar(e, 0, nullptr) == "a");
	REQUIRE(V2ParamVarchar(e, 1, nullptr) == "bb");
	REQUIRE(V2ParamVarchar(e, 2, nullptr) == "ccc");
	duckdb_v2_logical_type_destroy(&e);

	// Interop pin: a zero-entry enum is invalid in SQL (the enum bind
	// requires at least one argument) and V2 correctly refuses to build it;
	// only V1 can construct the degenerate form. V2 inspection handles the
	// V1-built handle gracefully: zero params, get_param refuses.
	const char *none[] = {nullptr};
	auto empty = MakeV1Enum(none, 0);
	REQUIRE(V2ParamCount(empty) == 0);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_param(empty, 0, &name, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
	duckdb_v2_logical_type_destroy(&empty);
}

TEST_CASE("V2: get_param out-of-range and null-arg refusals", "[capi_v2][logical_type][param]") {
	V2EnvFixture fx;
	auto t = V2CreateType(fx.conn, "decimal", nullptr, {V2Int32Value(10), V2Int32Value(2)});

	duckdb_v2_str name = {nullptr, 0};
	auto v = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_param(t, 2, &name, &v, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	idx_t count = 0;
	REQUIRE(duckdb_v2_logical_type_get_param_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_param_count(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_param(nullptr, 0, &name, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_param(t, 0, nullptr, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_param(t, 0, &name, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_destroy(&t);
}

// ===========================================================================
// logical_type_create
// ===========================================================================

TEST_CASE("V2: logical_type_create builds the built-in parameterized kinds", "[capi_v2][logical_type][create]") {
	V2EnvFixture f;

	// decimal(width, scale): numeric positional params.
	auto dec = V2CreateType(f.conn, "decimal", nullptr, {V2Int32Value(18), V2Int32Value(3)});
	auto dec_expected = V1ToV2(duckdb_create_decimal_type(18, 3));
	REQUIRE(V2TypesEqual(dec, dec_expected));
	duckdb_v2_logical_type_destroy(&dec_expected);
	duckdb_v2_logical_type_destroy(&dec);

	// list(T): the element type crosses as a TYPE value.
	auto list = V2CreateType(f.conn, "list", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)});
	REQUIRE(V2TypeText(list) == "INTEGER[]");
	duckdb_v2_logical_type_destroy(&list);

	// array(T, size).
	auto arr =
	    V2CreateType(f.conn, "array", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2Int32Value(3)});
	REQUIRE(V2TypeText(arr) == "INTEGER[3]");
	duckdb_v2_logical_type_destroy(&arr);

	// map(K, V).
	auto map = V2CreateType(
	    f.conn, "map", nullptr,
	    {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR), V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)});
	REQUIRE(V2TypeText(map) == "MAP(VARCHAR, INTEGER)");
	duckdb_v2_logical_type_destroy(&map);

	// struct: named fields.
	std::vector<const char *> field_names = {"a", "b"};
	auto s = V2CreateType(
	    f.conn, "struct", &field_names,
	    {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	REQUIRE(V2TypeText(s) == "STRUCT(a INTEGER, b VARCHAR)");
	duckdb_v2_logical_type_destroy(&s);

	// struct: all positional builds an unnamed struct whose params come back
	// positional.
	auto anon = V2CreateType(
	    f.conn, "struct", nullptr,
	    {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	REQUIRE(V2TypeIdOf(anon) == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);
	REQUIRE(V2ParamCount(anon) == 2);
	auto anon_field = V2ParamType(anon, 0, nullptr);
	REQUIRE(V2TypeIdOf(anon_field) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&anon_field);
	duckdb_v2_logical_type_destroy(&anon);

	// union: named members.
	std::vector<const char *> member_names = {"i", "s"};
	auto u = V2CreateType(
	    f.conn, "union", &member_names,
	    {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	REQUIRE(V2TypeText(u) == "UNION(i INTEGER, s VARCHAR)");
	duckdb_v2_logical_type_destroy(&u);

	// enum: positional VARCHAR entries. Name resolution is case-insensitive.
	auto e = V2CreateType(f.conn, "ENUM", nullptr, {V2VarcharValue("x"), V2VarcharValue("y")});
	REQUIRE(V2TypeText(e) == "ENUM('x', 'y')");
	duckdb_v2_logical_type_destroy(&e);
}

TEST_CASE("V2: logical_type_create nests through TYPE values", "[capi_v2][logical_type][create]") {
	V2EnvFixture f;
	// list(struct(a integer[], m map(varchar, decimal(10,2)))), composed
	// bottom-up, equals the from_text form.
	auto int_list = V2CreateType(f.conn, "list", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)});
	auto dec = V2CreateType(f.conn, "decimal", nullptr, {V2Int32Value(10), V2Int32Value(2)});
	auto map =
	    V2CreateType(f.conn, "map", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR), V2TypeValueOf(dec)});
	std::vector<const char *> field_names = {"a", "m"};
	auto s = V2CreateType(f.conn, "struct", &field_names, {V2TypeValueOf(int_list), V2TypeValueOf(map)});
	auto deep = V2CreateType(f.conn, "list", nullptr, {V2TypeValueOf(s)});
	duckdb_v2_logical_type_destroy(&int_list);
	duckdb_v2_logical_type_destroy(&dec);
	duckdb_v2_logical_type_destroy(&map);
	duckdb_v2_logical_type_destroy(&s);

	auto expected = V2TypeFromText(f.conn, "STRUCT(a INTEGER[], m MAP(VARCHAR, DECIMAL(10,2)))[]");
	REQUIRE(V2TypesEqual(deep, expected));
	duckdb_v2_logical_type_destroy(&expected);
	duckdb_v2_logical_type_destroy(&deep);
}

TEST_CASE("V2: logical_type_create builds a collation VARCHAR", "[capi_v2][logical_type][create][param]") {
	V2EnvFixture f;
	std::vector<const char *> names = {"collation"};
	auto t = V2CreateType(f.conn, "varchar", &names, {V2VarcharValue("nocase")});
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	// Inspection keeps what the bound type carries: the collation, as one
	// named param (to_text renders plain VARCHAR).
	REQUIRE(V2ParamCount(t) == 1);
	REQUIRE(V2ParamVarchar(t, 0, "collation") == "nocase");
	RequireParamRoundTrip(f.conn, t);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type_create resolves catalog and extension types", "[capi_v2][logical_type][create]") {
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	// A user type without a bind function: no params allowed, comes back
	// structural.
	auto mood = V2CreateType(f.conn, "mood", nullptr, {});
	REQUIRE(V2TypeIdOf(mood) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	REQUIRE(V2ParamCount(mood) == 3);
	duckdb_v2_logical_type_destroy(&mood);
	REQUIRE(V2CreateTypeErr(f.conn, "mood", nullptr, {V2VarcharValue("extra")}) == DUCKDB_V2_ERROR_QUERY_BINDER);

	// An extension type registered through custom_type_builder resolves via
	// the system-catalog fallback and keeps its alias.
	V2WithContext(f.conn, [](duckdb_v2_context_handle ctx) {
		duckdb_v2_logical_type_handle int_type = nullptr;
		duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, nullptr);
		duckdb_v2_custom_type_builder_handle builder = nullptr;
		REQUIRE(duckdb_v2_custom_type_builder_create(ctx, &builder, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_custom_type_builder_set_name(builder, V2Str("TEMPERATURE"), nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_custom_type_builder_set_base_type(builder, int_type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_custom_type_builder_register(ctx, builder, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_custom_type_builder_destroy(&builder);
		duckdb_v2_logical_type_destroy(&int_type);
	});

	auto temp = V2CreateType(f.conn, "temperature", nullptr, {});
	REQUIRE(V2TypeIdOf(temp) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_str alias = {nullptr, 0};
	REQUIRE(duckdb_v2_logical_type_get_name(temp, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias == "TEMPERATURE");
	// from_text resolves the same entry.
	auto temp2 = V2TypeFromText(f.conn, "TEMPERATURE");
	REQUIRE(V2TypesEqual(temp, temp2));
	duckdb_v2_logical_type_destroy(&temp2);
	// Zero params; the aliased name reconstructs it.
	REQUIRE(V2ParamCount(temp) == 0);
	RequireParamRoundTrip(f.conn, temp);
	duckdb_v2_logical_type_destroy(&temp);
	REQUIRE(V2CreateTypeErr(f.conn, "temperature", nullptr, {V2Int32Value(1)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
}

TEST_CASE("V2: logical_type_create error paths", "[capi_v2][logical_type][create]") {
	V2EnvFixture f;

	// Unknown name: catalog error.
	REQUIRE(V2CreateTypeErr(f.conn, "definitely_not_a_type", nullptr, {}) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	// Wrong parameter counts and types: binder errors from the bind function.
	REQUIRE(V2CreateTypeErr(f.conn, "list", nullptr, {}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(V2CreateTypeErr(f.conn, "list", nullptr, {V2Int32Value(1)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(V2CreateTypeErr(f.conn, "decimal", nullptr, {V2Int32Value(0)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(V2CreateTypeErr(f.conn, "array", nullptr,
	                        {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2Int32Value(0)}) ==
	        DUCKDB_V2_ERROR_QUERY_BINDER);
	// UNION members must be named; ENUM entries must be non-NULL VARCHAR.
	REQUIRE(V2CreateTypeErr(f.conn, "union", nullptr, {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)}) ==
	        DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(V2CreateTypeErr(f.conn, "enum", nullptr, {V2Int32Value(1)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, nullptr);
	duckdb_v2_value_handle null_entry = nullptr;
	duckdb_v2_value_create_null(varchar_type, &null_entry, nullptr);
	duckdb_v2_logical_type_destroy(&varchar_type);
	REQUIRE(V2CreateTypeErr(f.conn, "enum", nullptr, {null_entry}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	// STRUCT fields must be all named or all positional.
	std::vector<const char *> mixed = {"a", nullptr};
	REQUIRE(V2CreateTypeErr(f.conn, "struct", &mixed,
	                        {V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
	                         V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
	// A parameterless built-in refuses params.
	REQUIRE(V2CreateTypeErr(f.conn, "integer", nullptr, {V2Int32Value(1)}) == DUCKDB_V2_ERROR_QUERY_BINDER);
}

TEST_CASE("V2: logical_type_create is unqualified-only; from_text resolves qualified names",
          "[capi_v2][logical_type][create][from_text]") {
	// The documented contract, pinned in both directions so an
	// identifier-splitting "fix" cannot change behavior unnoticed.
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	// A qualified name is one literal identifier to the generic
	// constructor: no catalog entry matches, so the lookup fails.
	REQUIRE(V2CreateTypeErr(f.conn, "main.mood", nullptr, {}) == DUCKDB_V2_ERROR_DATABASE_CATALOG);

	// The text path runs the real binder and resolves it.
	auto t = V2TypeFromText(f.conn, "main.mood");
	REQUIRE(V2TypeIdOf(t) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: logical_type_create null-arg refusals", "[capi_v2][logical_type][create]") {
	V2EnvFixture f;
	duckdb_v2_logical_type_handle t = nullptr;

	// A null context is refused without any scope.
	REQUIRE(duckdb_v2_logical_type_create(nullptr, V2Str("integer"), nullptr, nullptr, 0, &t, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);

	V2WithContext(f.conn, [&](duckdb_v2_context_handle ctx) {
		duckdb_v2_value_handle value = V2Int32Value(1);
		const duckdb_v2_value_handle values[1] = {value};
		duckdb_v2_logical_type_handle out = nullptr;
		// Null out_type.
		REQUIRE(duckdb_v2_logical_type_create(ctx, V2Str("integer"), nullptr, nullptr, 0, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		// Malformed name view.
		REQUIRE(duckdb_v2_logical_type_create(ctx, duckdb_v2_str {nullptr, 3}, nullptr, nullptr, 0, &out, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(out == nullptr);
		// param_count > 0 with a null values array.
		REQUIRE(duckdb_v2_logical_type_create(ctx, V2Str("list"), nullptr, nullptr, 1, &out, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(out == nullptr);
		// A null value handle inside the array.
		const duckdb_v2_value_handle holed[1] = {nullptr};
		REQUIRE(duckdb_v2_logical_type_create(ctx, V2Str("list"), nullptr, holed, 1, &out, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(out == nullptr);
		// A malformed name view inside the names array.
		const duckdb_v2_str bad_names[1] = {{nullptr, 3}};
		REQUIRE(duckdb_v2_logical_type_create(ctx, V2Str("list"), bad_names, values, 1, &out, nullptr) ==
		        DUCKDB_V2_ERROR_INVALID_INPUT);
		REQUIRE(out == nullptr);
		duckdb_v2_value_destroy(&value);
	});
}

TEST_CASE("V2: GEOMETRY with a coordinate system constructs and inspects",
          "[capi_v2][logical_type][create][param][geometry]") {
	V2EnvFixture f;

	// Shorthand: the in-tree default catalog ships OGC:CRS84 (and CRS83) as
	// COORDINATE_SYSTEM_ENTRY defaults; binding identifies the CRS and stores
	// its shortest form.
	auto geo = V2CreateType(f.conn, "geometry", nullptr, {V2VarcharValue("OGC:CRS84")});
	REQUIRE(V2TypeIdOf(geo) == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY);
	REQUIRE(V2ParamCount(geo) == 1);
	REQUIRE(V2ParamVarchar(geo, 0, nullptr) == "OGC:CRS84");
	RequireParamRoundTrip(f.conn, geo);

	// to_text renders the CRS; from_text re-binds the same type.
	auto text = V2TypeText(geo);
	REQUIRE(text == "GEOMETRY('OGC:CRS84')");
	auto parsed = V2TypeFromText(f.conn, text);
	REQUIRE(V2TypesEqual(geo, parsed));
	duckdb_v2_logical_type_destroy(&parsed);
	duckdb_v2_logical_type_destroy(&geo);

	// A complete definition (here PROJJSON without an id) needs no resolver
	// at all: the identify fall-through keeps it verbatim.
	const char *projjson = "{\"type\":\"GeographicCRS\",\"name\":\"Test CRS\"}";
	auto complete = V2CreateType(f.conn, "geometry", nullptr, {V2VarcharValue(projjson)});
	REQUIRE(V2TypeIdOf(complete) == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY);
	REQUIRE(V2ParamCount(complete) == 1);
	REQUIRE(V2ParamVarchar(complete, 0, nullptr) == projjson);
	RequireParamRoundTrip(f.conn, complete);
	auto complete_parsed = V2TypeFromText(f.conn, V2TypeText(complete));
	REQUIRE(V2TypesEqual(complete, complete_parsed));
	duckdb_v2_logical_type_destroy(&complete_parsed);
	duckdb_v2_logical_type_destroy(&complete);
}

TEST_CASE("V2: GEOMETRY with an unknown coordinate system", "[capi_v2][logical_type][create][geometry]") {
	V2EnvFixture f;

	// A well-formed shorthand outside the default entries fails the bind.
	REQUIRE(V2CreateTypeErr(f.conn, "geometry", nullptr, {V2VarcharValue("EPSG:4326")}) ==
	        DUCKDB_V2_ERROR_QUERY_BINDER);

	// ignore_unknown_crs degrades to the generic GEOMETRY type instead.
	V2ExecSQL(f.conn, "SET ignore_unknown_crs = true");
	auto geo = V2CreateType(f.conn, "geometry", nullptr, {V2VarcharValue("EPSG:4326")});
	REQUIRE(V2TypeIdOf(geo) == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY);
	REQUIRE(V2ParamCount(geo) == 0);
	duckdb_v2_logical_type_destroy(&geo);
}

// ===========================================================================
// Storage-tier contract pins
//
// The DECIMAL and ENUM physical storage tiers are committed contract text
// in the vector module's view docstring, derivable from width / dictionary
// size; there are no getters. These pins assert the documented tables
// against the engine's own InternalType(), so the text cannot silently
// drift if core ever changes tiers.
// ===========================================================================

TEST_CASE("V2: DECIMAL storage tier by width matches the documented table", "[capi_v2][logical_type][storage_tier]") {
	V2EnvFixture f;
	struct {
		int width;
		duckdb::PhysicalType expected;
	} cases[] = {
	    {4, duckdb::PhysicalType::INT16},   {5, duckdb::PhysicalType::INT32},  {9, duckdb::PhysicalType::INT32},
	    {10, duckdb::PhysicalType::INT64},  {18, duckdb::PhysicalType::INT64}, {19, duckdb::PhysicalType::INT128},
	    {38, duckdb::PhysicalType::INT128},
	};
	for (auto &c : cases) {
		auto t = V2TypeFromText(f.conn, "DECIMAL(" + std::to_string(c.width) + ",2)");
		REQUIRE(reinterpret_cast<duckdb::LogicalType *>(t)->InternalType() == c.expected);
		duckdb_v2_logical_type_destroy(&t);
	}
}

TEST_CASE("V2: ENUM storage tier by dictionary size matches the documented table",
          "[capi_v2][logical_type][storage_tier]") {
	V2EnvFixture f;
	struct {
		idx_t entries;
		duckdb::PhysicalType expected;
	} cases[] = {
	    {1, duckdb::PhysicalType::UINT8},      {255, duckdb::PhysicalType::UINT8},
	    {256, duckdb::PhysicalType::UINT16},   {65535, duckdb::PhysicalType::UINT16},
	    {65536, duckdb::PhysicalType::UINT32},
	};
	for (auto &c : cases) {
		std::vector<duckdb_v2_value_handle> values;
		values.reserve(c.entries);
		for (idx_t i = 0; i < c.entries; i++) {
			values.push_back(V2VarcharValue(("v" + std::to_string(i)).c_str()));
		}
		auto t = V2CreateType(f.conn, "enum", nullptr, std::move(values));
		REQUIRE(reinterpret_cast<duckdb::LogicalType *>(t)->InternalType() == c.expected);
		REQUIRE(V2ParamCount(t) == c.entries);
		duckdb_v2_logical_type_destroy(&t);
	}

	// Interop pin: the V1-only zero-entry enum (SQL forbids it) still
	// reports the uint8 tier through V2 inspection.
	const char *none[] = {nullptr};
	auto empty = MakeV1Enum(none, 0);
	REQUIRE(reinterpret_cast<duckdb::LogicalType *>(empty)->InternalType() == duckdb::PhysicalType::UINT8);
	duckdb_v2_logical_type_destroy(&empty);
}

// Deliberate cross-version pin: the composite fixtures are V1-built and the
// V2 reconstruction is compared against them via logical_type_is_equal.
TEST_CASE("V2: create(name, params(t)) round-trips every constructible kind",
          "[capi_v2][logical_type][create][param]") {
	V2EnvFixture f;
	V2ExecSQL(f.conn, "CREATE TYPE mood AS ENUM('sad', 'ok', 'happy')");

	// Primitives: zero params, the rendered text is the name.
	DUCKDB_V2_LOGICAL_TYPE_ID primitives[] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
	    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
	    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
	    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIT,
	    DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM,
	    DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
	};
	for (auto id : primitives) {
		duckdb_v2_logical_type_handle t = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(id, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
		RequireParamRoundTrip(f.conn, t);
		duckdb_v2_logical_type_destroy(&t);
	}

	// Composites, nesting included.
	auto dec = V1ToV2(duckdb_create_decimal_type(38, 10));
	RequireParamRoundTrip(f.conn, dec);
	duckdb_v2_logical_type_destroy(&dec);

	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	auto nested = V1ToV2(duckdb_create_list_type(list_v1));
	RequireParamRoundTrip(f.conn, nested);
	duckdb_v2_logical_type_destroy(&nested);

	auto arr = V1ToV2(duckdb_create_array_type(list_v1, 7));
	duckdb_destroy_logical_type(&list_v1);
	RequireParamRoundTrip(f.conn, arr);
	duckdb_v2_logical_type_destroy(&arr);

	auto varchar_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto map = V1ToV2(duckdb_create_map_type(varchar_v1, int_v1));
	duckdb_destroy_logical_type(&int_v1);
	duckdb_destroy_logical_type(&varchar_v1);
	RequireParamRoundTrip(f.conn, map);
	duckdb_v2_logical_type_destroy(&map);

	auto s = MakeV1Struct();
	RequireParamRoundTrip(f.conn, s);
	duckdb_v2_logical_type_destroy(&s);

	auto u = MakeV1Union();
	RequireParamRoundTrip(f.conn, u);
	duckdb_v2_logical_type_destroy(&u);

	const char *enum_names[] = {"sad", "ok", "happy"};
	auto e = MakeV1Enum(enum_names, 3);
	RequireParamRoundTrip(f.conn, e);
	duckdb_v2_logical_type_destroy(&e);

	// Kinds reached through the other constructors.
	auto type_t = V2TypeFromText(f.conn, "TYPE");
	RequireParamRoundTrip(f.conn, type_t);
	duckdb_v2_logical_type_destroy(&type_t);

	auto geom = V2TypeFromText(f.conn, "GEOMETRY");
	RequireParamRoundTrip(f.conn, geom);
	duckdb_v2_logical_type_destroy(&geom);

	auto mood = V2TypeFromText(f.conn, "mood");
	RequireParamRoundTrip(f.conn, mood);
	duckdb_v2_logical_type_destroy(&mood);
}

// ===========================================================================
// Volume measurement (hidden): run explicitly via "[capi_v2_bench]"
// ===========================================================================

TEST_CASE("V2 bench: 100k-entry enum inspection cost", "[.][capi_v2_bench]") {
	V2EnvFixture f;
	constexpr idx_t N = 100000;
	std::vector<duckdb_v2_value_handle> entries;
	entries.reserve(N);
	for (idx_t i = 0; i < N; i++) {
		entries.push_back(V2VarcharValue(("v" + std::to_string(i)).c_str()));
	}
	auto t = V2CreateType(f.conn, "enum", nullptr, std::move(entries));
	REQUIRE(V2ParamCount(t) == N);

	using bench_clock = std::chrono::steady_clock;

	// Generic path: one owned value per entry (3 crossings each). Failure
	// counters instead of per-iteration REQUIREs keep assertion overhead out
	// of the measurement.
	uint64_t failures = 0;
	size_t generic_bytes = 0;
	auto start = bench_clock::now();
	for (idx_t i = 0; i < N; i++) {
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_value_handle v = nullptr;
		failures += (duckdb_v2_logical_type_get_param(t, i, &name, &v, nullptr) != DUCKDB_V2_ERROR_NONE);
		const void *bytes = nullptr;
		idx_t bytes_len = 0;
		failures += (duckdb_v2_value_get_data(v, &bytes, &bytes_len, nullptr) != DUCKDB_V2_ERROR_NONE);
		generic_bytes += bytes_len;
		duckdb_v2_value_destroy(&v);
	}
	auto generic_us = std::chrono::duration_cast<std::chrono::microseconds>(bench_clock::now() - start).count();
	REQUIRE(failures == 0);

	// Borrowed floor: direct engine dictionary access, what the removed
	// borrowed enum getter handed out per entry.
	size_t borrowed_bytes = 0;
	start = bench_clock::now();
	auto &lt = *reinterpret_cast<duckdb::LogicalType *>(t);
	auto &dict = duckdb::EnumType::GetValuesInsertOrder(lt);
	auto *data = duckdb::FlatVector::GetData<duckdb::string_t>(dict);
	for (idx_t i = 0; i < N; i++) {
		borrowed_bytes += data[i].GetSize();
	}
	auto borrowed_us = std::chrono::duration_cast<std::chrono::microseconds>(bench_clock::now() - start).count();
	REQUIRE(generic_bytes == borrowed_bytes);

	WARN("enum inspection, 100k entries: generic get_param " << generic_us << " us total (" << (generic_us * 1000.0 / N)
	                                                         << " ns/entry); borrowed engine floor " << borrowed_us
	                                                         << " us");
	duckdb_v2_logical_type_destroy(&t);
}
