#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "duckdb.h" // V1 C API -- used only to build composite-type fixtures.

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 logical_type read-side tests (PR1).
//
// PR1 ships read-only introspection. The public V2 surface only constructs
// primitives (duckdb_v2_logical_type_create_from_id). Composite test
// fixtures are built via V1's constructors and the resulting handle is
// reinterpret-cast to duckdb_v2_logical_type_ptr.
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
//
// COVERAGE NOTE:
//   The DUCKDB_V2_API_ERROR branches in get_decimal_internal_type_id and
//   get_enum_internal_type_id (firing if the underlying PhysicalType is
//   outside the known DECIMAL or ENUM physical set) are unreachable through
//   any path the V1 or V2 surface exposes today, and are intentionally not
//   exercised here. They exist as a guard against future core changes that
//   introduce a new physical storage width. If duckdb::PhysicalType gains
//   a new variant used by DECIMAL or ENUM storage, the V2 enum spec must
//   add a matching id and these guards become reachable.
// ---------------------------------------------------------------------------

static duckdb_v2_logical_type_ptr V1ToV2(duckdb_logical_type t) {
	// Both V1 and V2 handles are void * aliases pointing at the same
	// new duckdb::LogicalType(...) allocation, so this is the identity cast.
	return static_cast<duckdb_v2_logical_type_ptr>(t);
}

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
		duckdb_v2_logical_type_ptr type = nullptr;
		REQUIRE(duckdb_v2_logical_type_create_from_id(c.id, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(type != nullptr);
		DUCKDB_V2_LOGICAL_TYPE_ID round_trip = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(type, &round_trip, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(round_trip == c.id);
		REQUIRE(duckdb_v2_logical_type_destroy(&type, nullptr) == DUCKDB_V2_ERROR_NONE);
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
		duckdb_v2_logical_type_ptr type = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
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
		duckdb_v2_logical_type_ptr type = nullptr;
		duckdb_v2_error_info_ptr err = nullptr;
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

TEST_CASE("V2: logical_type create_from_id clears pre-existing err on success", "[capi_v2][logical_type][lifecycle]") {
	// Belt-and-braces check of the error-info contract: on success the
	// library destroys any prior info in *err and leaves it nullptr.
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL, nullptr, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);

	duckdb_v2_logical_type_ptr t = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err == nullptr);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type destroy is null-safe", "[capi_v2][logical_type][lifecycle]") {
	// Passing a nullptr slot pointer is a no-op.
	REQUIRE(duckdb_v2_logical_type_destroy(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Passing a slot that already holds nullptr is a no-op.
	duckdb_v2_logical_type_ptr already_null = nullptr;
	REQUIRE(duckdb_v2_logical_type_destroy(&already_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

// ===========================================================================
// Common introspection: get_id, get_alias
// ===========================================================================

TEST_CASE("V2: logical_type get_id null handle / null out", "[capi_v2][logical_type][id]") {
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN;
	REQUIRE(duckdb_v2_logical_type_get_id(nullptr, &id, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_get_id(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type get_alias on un-aliased type is NULL", "[capi_v2][logical_type][alias]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);

	const char *alias = "sentinel";
	REQUIRE(duckdb_v2_logical_type_get_alias(t, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias == nullptr);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type get_alias reads alias set via V1", "[capi_v2][logical_type][alias]") {
	auto v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_logical_type_set_alias(v1, "my_int");
	auto t = V1ToV2(v1);

	const char *alias = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_alias(t, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias != nullptr);
	REQUIRE(std::string(alias) == "my_int");

	// Borrowed pointer is valid as long as the type is alive: a second call
	// returns the same string contents.
	const char *alias2 = nullptr;
	duckdb_v2_logical_type_get_alias(t, &alias2, nullptr);
	REQUIRE(std::string(alias2) == "my_int");

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type get_alias reads alias set on a STRUCT", "[capi_v2][logical_type][alias]") {
	// Mirrors the spatial / extension-type path: a composite with an alias
	// (e.g. STRUCT{x:double, y:double} aliased "POINT_2D").
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	const char *names[2] = {"x", "y"};
	auto v1 = duckdb_create_struct_type(members, names, 2);
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);
	duckdb_logical_type_set_alias(v1, "POINT_2D");
	auto t = V1ToV2(v1);

	const char *alias = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_alias(t, &alias, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(alias != nullptr);
	REQUIRE(std::string(alias) == "POINT_2D");

	// The id is still STRUCT — alias is metadata, not type identity.
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(t, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type get_alias null handle / null out", "[capi_v2][logical_type][alias]") {
	const char *alias = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_alias(nullptr, &alias, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	REQUIRE(duckdb_v2_logical_type_get_alias(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

// ===========================================================================
// DECIMAL
// ===========================================================================

TEST_CASE("V2: logical_type DECIMAL width / scale / internal_type_id", "[capi_v2][logical_type][decimal]") {
	struct {
		uint8_t width;
		uint8_t scale;
		DUCKDB_V2_LOGICAL_TYPE_ID expected_internal;
	} cases[] = {
	    {4, 2, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT},
	    {9, 4, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {18, 6, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT},
	    {38, 10, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT},
	};
	for (auto &c : cases) {
		auto t = V1ToV2(duckdb_create_decimal_type(c.width, c.scale));

		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		duckdb_v2_logical_type_get_id(t, &id, nullptr);
		REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL);

		uint8_t w = 0;
		REQUIRE(duckdb_v2_logical_type_get_decimal_width(t, &w, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(w == c.width);

		uint8_t s = 0;
		REQUIRE(duckdb_v2_logical_type_get_decimal_scale(t, &s, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(s == c.scale);

		DUCKDB_V2_LOGICAL_TYPE_ID internal = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_decimal_internal_type_id(t, &internal, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(internal == c.expected_internal);

		duckdb_v2_logical_type_destroy(&t, nullptr);
	}
}

TEST_CASE("V2: logical_type DECIMAL accessors reject non-DECIMAL", "[capi_v2][logical_type][decimal]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);

	uint8_t w = 0, s = 0;
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_decimal_width(t, &w, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_decimal_scale(t, &s, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_decimal_internal_type_id(t, &id, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type DECIMAL accessors null handle / null out", "[capi_v2][logical_type][decimal]") {
	uint8_t w = 0;
	REQUIRE(duckdb_v2_logical_type_get_decimal_width(nullptr, &w, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	auto t = V1ToV2(duckdb_create_decimal_type(10, 2));
	REQUIRE(duckdb_v2_logical_type_get_decimal_width(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_decimal_scale(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_decimal_internal_type_id(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

// ===========================================================================
// ENUM
// ===========================================================================

static duckdb_v2_logical_type_ptr MakeEnum(const char **values, idx_t count) {
	auto v1 = duckdb_create_enum_type(values, count);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

TEST_CASE("V2: logical_type ENUM size / value / internal_type_id (small)", "[capi_v2][logical_type][enum]") {
	const char *names[] = {"a", "bb", "ccc"};
	auto t = MakeEnum(names, 3);

	idx_t size = 0;
	REQUIRE(duckdb_v2_logical_type_get_enum_size(t, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 3);

	const char *v = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 0, &v, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 1);
	REQUIRE(std::string(v, len) == "a");
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 1, &v, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 2);
	REQUIRE(std::string(v, len) == "bb");
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 2, &v, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 3);
	REQUIRE(std::string(v, len) == "ccc");

	DUCKDB_V2_LOGICAL_TYPE_ID internal = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_enum_internal_type_id(t, &internal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(internal == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM internal id widens for large dictionaries", "[capi_v2][logical_type][enum]") {
	// 256 entries forces USMALLINT.
	std::vector<std::string> owned;
	owned.reserve(256);
	std::vector<const char *> values;
	values.reserve(256);
	for (idx_t i = 0; i < 256; i++) {
		owned.emplace_back("v" + std::to_string(i));
		values.push_back(owned.back().c_str());
	}
	auto t = MakeEnum(values.data(), values.size());

	idx_t size = 0;
	duckdb_v2_logical_type_get_enum_size(t, &size, nullptr);
	REQUIRE(size == 256);

	DUCKDB_V2_LOGICAL_TYPE_ID internal = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_enum_internal_type_id(t, &internal, nullptr);
	REQUIRE(internal == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM internal id is UINTEGER beyond USMALLINT range", "[capi_v2][logical_type][enum]") {
	// 65536 entries forces UINTEGER (the largest physical index type ENUM uses).
	constexpr idx_t N = 65536;
	std::vector<std::string> owned;
	owned.reserve(N);
	std::vector<const char *> values;
	values.reserve(N);
	for (idx_t i = 0; i < N; i++) {
		owned.emplace_back("v" + std::to_string(i));
		values.push_back(owned.back().c_str());
	}
	auto t = MakeEnum(values.data(), values.size());

	idx_t size = 0;
	duckdb_v2_logical_type_get_enum_size(t, &size, nullptr);
	REQUIRE(size == N);

	DUCKDB_V2_LOGICAL_TYPE_ID internal = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_enum_internal_type_id(t, &internal, nullptr);
	REQUIRE(internal == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM with empty dictionary", "[capi_v2][logical_type][enum]") {
	// Edge case: EnumTypeInfo::DictType(0) maps to UINT8, so an empty enum
	// is structurally valid. Confirm size=0, internal id UTINYINT, and that
	// get_enum_value at index 0 reports out-of-range.
	const char *names[] = {nullptr}; // unused — count is 0
	auto t = MakeEnum(names, 0);

	idx_t size = 99;
	REQUIRE(duckdb_v2_logical_type_get_enum_size(t, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 0);

	DUCKDB_V2_LOGICAL_TYPE_ID internal = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_enum_internal_type_id(t, &internal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(internal == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);

	const char *v = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 0, &v, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM get_value out-of-range", "[capi_v2][logical_type][enum]") {
	const char *names[] = {"x", "y"};
	auto t = MakeEnum(names, 2);
	const char *v = nullptr;
	idx_t len = 0;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 2, &v, &len, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM accessors reject non-ENUM", "[capi_v2][logical_type][enum]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);

	idx_t size = 0;
	const char *v = nullptr;
	idx_t len = 0;
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_enum_size(t, &size, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 0, &v, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_enum_internal_type_id(t, &id, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type ENUM accessors null handle / null out", "[capi_v2][logical_type][enum]") {
	idx_t size = 0;
	REQUIRE(duckdb_v2_logical_type_get_enum_size(nullptr, &size, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	const char *v = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_logical_type_get_enum_value(nullptr, 0, &v, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_enum_internal_type_id(nullptr, &id, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	const char *names[] = {"x", "y"};
	auto t = MakeEnum(names, 2);
	REQUIRE(duckdb_v2_logical_type_get_enum_size(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 0, nullptr, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_enum_value(t, 0, &v, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_enum_internal_type_id(t, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

// ===========================================================================
// LIST / ARRAY
// ===========================================================================

TEST_CASE("V2: logical_type LIST child type", "[capi_v2][logical_type][list_array]") {
	auto child_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto list = V1ToV2(duckdb_create_list_type(child_v1));
	duckdb_destroy_logical_type(&child_v1);

	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(list, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);

	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_list_child_type(list, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(child != nullptr);
	DUCKDB_V2_LOGICAL_TYPE_ID child_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(child, &child_id, nullptr);
	REQUIRE(child_id == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	// Caller owns the returned child.
	REQUIRE(duckdb_v2_logical_type_destroy(&child, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_destroy(&list, nullptr);
}

TEST_CASE("V2: logical_type ARRAY child type and size", "[capi_v2][logical_type][list_array]") {
	auto child_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto arr = V1ToV2(duckdb_create_array_type(child_v1, 7));
	duckdb_destroy_logical_type(&child_v1);

	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(arr, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY);

	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_array_child_type(arr, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID child_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(child, &child_id, nullptr);
	REQUIRE(child_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&child, nullptr);

	idx_t size = 0;
	REQUIRE(duckdb_v2_logical_type_get_array_size(arr, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 7);

	duckdb_v2_logical_type_destroy(&arr, nullptr);
}

TEST_CASE("V2: logical_type LIST/ARRAY accessors reject wrong kind", "[capi_v2][logical_type][list_array]") {
	auto int_t = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list = V1ToV2(duckdb_create_list_type(int_t));
	auto arr = V1ToV2(duckdb_create_array_type(int_t, 3));
	duckdb_destroy_logical_type(&int_t);

	duckdb_v2_logical_type_ptr child = nullptr;
	idx_t size = 0;

	// list_child_type on ARRAY: wrong kind.
	REQUIRE(duckdb_v2_logical_type_get_list_child_type(arr, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(child == nullptr);
	// array_child_type on LIST: wrong kind.
	REQUIRE(duckdb_v2_logical_type_get_array_child_type(list, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(child == nullptr);
	// array_size on LIST: wrong kind.
	REQUIRE(duckdb_v2_logical_type_get_array_size(list, &size, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_destroy(&list, nullptr);
	duckdb_v2_logical_type_destroy(&arr, nullptr);
}

TEST_CASE("V2: logical_type LIST/ARRAY null handle / null out", "[capi_v2][logical_type][list_array]") {
	duckdb_v2_logical_type_ptr child = nullptr;
	idx_t size = 0;
	REQUIRE(duckdb_v2_logical_type_get_list_child_type(nullptr, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_array_child_type(nullptr, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_array_size(nullptr, &size, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	auto int_t = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list = V1ToV2(duckdb_create_list_type(int_t));
	auto arr = V1ToV2(duckdb_create_array_type(int_t, 3));
	duckdb_destroy_logical_type(&int_t);

	REQUIRE(duckdb_v2_logical_type_get_list_child_type(list, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_array_child_type(arr, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_array_size(arr, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_destroy(&list, nullptr);
	duckdb_v2_logical_type_destroy(&arr, nullptr);
}

// ===========================================================================
// MAP
// ===========================================================================

TEST_CASE("V2: logical_type MAP key/value types", "[capi_v2][logical_type][map]") {
	auto k_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto v_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto map = V1ToV2(duckdb_create_map_type(k_v1, v_v1));
	duckdb_destroy_logical_type(&k_v1);
	duckdb_destroy_logical_type(&v_v1);

	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(map, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_MAP);

	duckdb_v2_logical_type_ptr key = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_map_key_type(map, &key, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID kid = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(key, &kid, nullptr);
	REQUIRE(kid == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&key, nullptr);

	duckdb_v2_logical_type_ptr val = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_map_value_type(map, &val, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID vid = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(val, &vid, nullptr);
	REQUIRE(vid == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&val, nullptr);

	duckdb_v2_logical_type_destroy(&map, nullptr);
}

TEST_CASE("V2: logical_type MAP accessors reject non-MAP", "[capi_v2][logical_type][map]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);
	duckdb_v2_logical_type_ptr out = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_map_key_type(t, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_map_value_type(t, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type MAP null handle / null out", "[capi_v2][logical_type][map]") {
	duckdb_v2_logical_type_ptr out = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_map_key_type(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_map_value_type(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	auto k_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto v_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto map = V1ToV2(duckdb_create_map_type(k_v1, v_v1));
	duckdb_destroy_logical_type(&k_v1);
	duckdb_destroy_logical_type(&v_v1);

	REQUIRE(duckdb_v2_logical_type_get_map_key_type(map, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_map_value_type(map, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&map, nullptr);
}

// ===========================================================================
// STRUCT
// ===========================================================================

static duckdb_v2_logical_type_ptr MakeStruct() {
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	const char *names[2] = {"id", "name"};
	auto v1 = duckdb_create_struct_type(members, names, 2);
	// Free the member fixtures before any REQUIRE — Catch2 throws on failure
	// and would otherwise skip these destroys, leaking the LogicalTypes.
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

TEST_CASE("V2: logical_type STRUCT count / name / child_type", "[capi_v2][logical_type][struct]") {
	auto s = MakeStruct();

	idx_t count = 0;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_count(s, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	const char *name = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(s, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "id");
	// Spec promises a null-terminated string; verify once that the reported
	// length matches strlen so callers can rely on either form.
	REQUIRE(std::strlen(name) == len);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(s, 1, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "name");

	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(s, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID cid = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(child, &cid, nullptr);
	REQUIRE(cid == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&child, nullptr);

	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(s, 1, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_get_id(child, &cid, nullptr);
	REQUIRE(cid == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&child, nullptr);

	duckdb_v2_logical_type_destroy(&s, nullptr);
}

TEST_CASE("V2: logical_type STRUCT out-of-range index", "[capi_v2][logical_type][struct]") {
	auto s = MakeStruct();
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(s, 2, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(s, 2, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(child == nullptr);
	duckdb_v2_logical_type_destroy(&s, nullptr);
}

TEST_CASE("V2: logical_type STRUCT accessors reject non-STRUCT", "[capi_v2][logical_type][struct]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);

	idx_t count = 0;
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_count(t, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(t, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(t, 0, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type STRUCT null handle / null out", "[capi_v2][logical_type][struct]") {
	idx_t count = 0;
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(nullptr, 0, &name, &len, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(nullptr, 0, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	auto s = MakeStruct();
	REQUIRE(duckdb_v2_logical_type_get_struct_child_count(s, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(s, 0, nullptr, &len, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_name(s, 0, &name, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(s, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&s, nullptr);
}

TEST_CASE("V2: logical_type STRUCT with nested LIST child round-trips", "[capi_v2][logical_type][struct]") {
	// Recursive composite: STRUCT{a: LIST<INTEGER>, b: VARCHAR}. Walks the
	// V2 surface through two levels of child-getters to confirm the
	// child-type allocations cascade cleanly.
	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	duckdb_destroy_logical_type(&int_v1);

	duckdb_logical_type fields[2] = {list_v1, duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};
	const char *names[2] = {"a", "b"};
	auto struct_v1 = duckdb_create_struct_type(fields, names, 2);
	duckdb_destroy_logical_type(&fields[0]);
	duckdb_destroy_logical_type(&fields[1]);
	REQUIRE(struct_v1 != nullptr);
	auto s = V1ToV2(struct_v1);

	// Top level: STRUCT with 2 children.
	idx_t count = 0;
	duckdb_v2_logical_type_get_struct_child_count(s, &count, nullptr);
	REQUIRE(count == 2);

	// Field "a" is a LIST<INTEGER> — drill one level down.
	duckdb_v2_logical_type_ptr field_a = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_struct_child_type(s, 0, &field_a, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(field_a, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);

	duckdb_v2_logical_type_ptr inner = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_list_child_type(field_a, &inner, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_get_id(inner, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&inner, nullptr);
	duckdb_v2_logical_type_destroy(&field_a, nullptr);

	// Field "b" is a VARCHAR.
	duckdb_v2_logical_type_ptr field_b = nullptr;
	duckdb_v2_logical_type_get_struct_child_type(s, 1, &field_b, nullptr);
	duckdb_v2_logical_type_get_id(field_b, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&field_b, nullptr);

	duckdb_v2_logical_type_destroy(&s, nullptr);
}

// ===========================================================================
// UNION
// ===========================================================================

static duckdb_v2_logical_type_ptr MakeUnion() {
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	const char *names[2] = {"i", "s"};
	auto v1 = duckdb_create_union_type(members, names, 2);
	// Free the member fixtures before any REQUIRE — Catch2 throws on failure
	// and would otherwise skip these destroys, leaking the LogicalTypes.
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);
	REQUIRE(v1 != nullptr);
	return V1ToV2(v1);
}

TEST_CASE("V2: logical_type UNION count / name / member_type", "[capi_v2][logical_type][union]") {
	auto u = MakeUnion();

	idx_t count = 0;
	REQUIRE(duckdb_v2_logical_type_get_union_member_count(u, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	const char *name = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(u, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "i");
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(u, 1, &name, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name, len) == "s");

	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_union_member_type(u, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID cid = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(child, &cid, nullptr);
	REQUIRE(cid == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&child, nullptr);

	REQUIRE(duckdb_v2_logical_type_get_union_member_type(u, 1, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_get_id(child, &cid, nullptr);
	REQUIRE(cid == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	duckdb_v2_logical_type_destroy(&child, nullptr);

	duckdb_v2_logical_type_destroy(&u, nullptr);
}

TEST_CASE("V2: logical_type UNION out-of-range index", "[capi_v2][logical_type][union]") {
	auto u = MakeUnion();
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(u, 2, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_type(u, 2, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(child == nullptr);
	duckdb_v2_logical_type_destroy(&u, nullptr);
}

TEST_CASE("V2: logical_type UNION accessors reject non-UNION", "[capi_v2][logical_type][union]") {
	duckdb_v2_logical_type_ptr t = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &t, nullptr);

	idx_t count = 0;
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_union_member_count(t, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(t, 0, &name, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_type(t, 0, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&t, nullptr);
}

TEST_CASE("V2: logical_type UNION null handle / null out", "[capi_v2][logical_type][union]") {
	idx_t count = 0;
	const char *name = nullptr;
	idx_t len = 0;
	duckdb_v2_logical_type_ptr child = nullptr;
	REQUIRE(duckdb_v2_logical_type_get_union_member_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(nullptr, 0, &name, &len, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_type(nullptr, 0, &child, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	auto u = MakeUnion();
	REQUIRE(duckdb_v2_logical_type_get_union_member_count(u, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(u, 0, nullptr, &len, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_name(u, 0, &name, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_logical_type_get_union_member_type(u, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&u, nullptr);
}
