#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "duckdb.h" // V1 C API -- used only for cross-API parity checks.

#include <cstdlib>
#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 value tests — primitive surface.
//
// Same identity-cast invariant as the logical_type bridge: both V1
// duckdb_value and V2 duckdb_v2_value_ptr are heap-allocated duckdb::Value
// cast to void *. We rely on it to round-trip a V1-built fixture through V2
// destroy in one place (the value_get_varchar test), the same way the
// logical_type tests reuse V1 fixtures. If a wrapper is added later, this
// file must change.
//
// Borrow contract being verified throughout: *_get_varchar, *_get_blob, and
// *_get_bit return pointers that stay valid until value_destroy is called.
// *_get_bignum is the outlier — it allocates a fresh magnitude buffer the
// caller must free() because core stores negative bignums bit-inverted.
// ---------------------------------------------------------------------------

static duckdb_v2_value_ptr V1ValueToV2(duckdb_value v) {
	return static_cast<duckdb_v2_value_ptr>(v);
}

// ===========================================================================
// Lifecycle: destroy null-safety
// ===========================================================================

TEST_CASE("V2: value destroy is null-safe", "[capi_v2][value][lifecycle]") {
	REQUIRE(duckdb_v2_value_destroy(nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_ptr already_null = nullptr;
	REQUIRE(duckdb_v2_value_destroy(&already_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

// ===========================================================================
// NULL construction + is_null + get_logical_type ownership
// ===========================================================================

TEST_CASE("V2: value_create_null carries the borrowed type", "[capi_v2][value][null]") {
	duckdb_v2_logical_type_ptr int_type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_null(int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);

	// Input type is borrowed: destroying it must not affect the NULL value.
	REQUIRE(duckdb_v2_logical_type_destroy(&int_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(v, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);

	duckdb_v2_logical_type_ptr out_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(v, &out_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_type != nullptr);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(out_type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	// Caller owns the returned logical type.
	REQUIRE(duckdb_v2_logical_type_destroy(&out_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_value_destroy(&v, nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: value_create_null rejects null type / null out", "[capi_v2][value][null]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_null(nullptr, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);

	duckdb_v2_logical_type_ptr int_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, nullptr);
	REQUIRE(duckdb_v2_value_create_null(int_type, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_logical_type_destroy(&int_type, nullptr);
}

TEST_CASE("V2: value_is_null distinguishes NULL from non-NULL", "[capi_v2][value][null]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_int32(7, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool is_null = true;
	REQUIRE(duckdb_v2_value_is_null(v, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!is_null);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: value_is_null / value_get_logical_type / value_destroy null guards", "[capi_v2][value][null]") {
	bool b = false;
	REQUIRE(duckdb_v2_value_is_null(nullptr, &b, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_logical_type_ptr lt = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(nullptr, &lt, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(lt == nullptr);

	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_value_create_int32(1, &v, nullptr);
	REQUIRE(duckdb_v2_value_is_null(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_value_get_logical_type(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&v, nullptr);
}

// ===========================================================================
// Numeric primitive round-trips: create_X / get_X
// ===========================================================================

TEST_CASE("V2: bool round-trip + reject wrong type + reject NULL", "[capi_v2][value][bool]") {
	for (bool input : {true, false}) {
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_bool(input, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		duckdb_v2_logical_type_ptr lt = nullptr;
		duckdb_v2_value_get_logical_type(v, &lt, nullptr);
		duckdb_v2_logical_type_get_id(lt, &id, nullptr);
		duckdb_v2_logical_type_destroy(&lt, nullptr);
		REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);

		bool out = !input;
		REQUIRE(duckdb_v2_value_get_bool(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == input);
		duckdb_v2_value_destroy(&v, nullptr);
	}

	// Wrong type: TINYINT value, BOOL getter.
	duckdb_v2_value_ptr ti = nullptr;
	duckdb_v2_value_create_int8(1, &ti, nullptr);
	bool out = false;
	REQUIRE(duckdb_v2_value_get_bool(ti, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&ti, nullptr);

	// NULL of BOOLEAN: typed getter must refuse rather than read garbage.
	duckdb_v2_logical_type_ptr bool_lt = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, &bool_lt, nullptr);
	duckdb_v2_value_ptr nb = nullptr;
	duckdb_v2_value_create_null(bool_lt, &nb, nullptr);
	REQUIRE(duckdb_v2_value_get_bool(nb, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&nb, nullptr);
	duckdb_v2_logical_type_destroy(&bool_lt, nullptr);
}

// Helper: assert a typed-payload getter rejects a wrong-id value and a NULL
// value. Used to cover every primitive without repeating boilerplate.
template <class CreateOther, class Getter, class OutT>
static void RejectWrongTypeAndNull(CreateOther create_other_value, Getter getter, OutT &out_storage,
                                   DUCKDB_V2_LOGICAL_TYPE_ID id_for_null) {
	duckdb_v2_value_ptr other = create_other_value();
	REQUIRE(getter(other, &out_storage, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&other, nullptr);

	duckdb_v2_logical_type_ptr lt = nullptr;
	duckdb_v2_logical_type_create_from_id(id_for_null, &lt, nullptr);
	duckdb_v2_value_ptr nv = nullptr;
	duckdb_v2_value_create_null(lt, &nv, nullptr);
	REQUIRE(getter(nv, &out_storage, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&nv, nullptr);
	duckdb_v2_logical_type_destroy(&lt, nullptr);
}

TEST_CASE("V2: signed integer round-trips", "[capi_v2][value][int]") {
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_int8(-42, &v, nullptr);
		int8_t out = 0;
		REQUIRE(duckdb_v2_value_get_int8(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == -42);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_int16(-30000, &v, nullptr);
		int16_t out = 0;
		duckdb_v2_value_get_int16(v, &out, nullptr);
		REQUIRE(out == -30000);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_int32(-1234567, &v, nullptr);
		int32_t out = 0;
		duckdb_v2_value_get_int32(v, &out, nullptr);
		REQUIRE(out == -1234567);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_int64(int64_t(-9000000000LL), &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_int64(v, &out, nullptr);
		REQUIRE(out == int64_t(-9000000000LL));
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: unsigned integer round-trips", "[capi_v2][value][uint]") {
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_uint8(200, &v, nullptr);
		uint8_t out = 0;
		duckdb_v2_value_get_uint8(v, &out, nullptr);
		REQUIRE(out == 200);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_uint16(60000, &v, nullptr);
		uint16_t out = 0;
		duckdb_v2_value_get_uint16(v, &out, nullptr);
		REQUIRE(out == 60000);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_uint32(4000000000u, &v, nullptr);
		uint32_t out = 0;
		duckdb_v2_value_get_uint32(v, &out, nullptr);
		REQUIRE(out == 4000000000u);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_uint64(9000000000000000000ULL, &v, nullptr);
		uint64_t out = 0;
		duckdb_v2_value_get_uint64(v, &out, nullptr);
		REQUIRE(out == 9000000000000000000ULL);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: integer wrong-type / NULL rejection (sample)", "[capi_v2][value][int]") {
	// Sample two getters; the typed getter is otherwise mechanical.
	int32_t i32 = 0;
	RejectWrongTypeAndNull(
	    []() {
		    duckdb_v2_value_ptr v = nullptr;
		    duckdb_v2_value_create_int64(0, &v, nullptr);
		    return v;
	    },
	    duckdb_v2_value_get_int32, i32, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	uint64_t u64 = 0;
	RejectWrongTypeAndNull(
	    []() {
		    duckdb_v2_value_ptr v = nullptr;
		    duckdb_v2_value_create_int32(0, &v, nullptr);
		    return v;
	    },
	    duckdb_v2_value_get_uint64, u64, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT);
}

// Helper for getters whose out-param shape doesn't fit RejectWrongTypeAndNull.
template <class CreateOther, class Getter>
static void RejectHugeintShapedGetter(CreateOther create_other, Getter getter, DUCKDB_V2_LOGICAL_TYPE_ID id_for_null) {
	uint64_t lo = 0;
	int64_t hi = 0;
	duckdb_v2_value_ptr other = create_other();
	REQUIRE(getter(other, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&other, nullptr);

	duckdb_v2_logical_type_ptr lt = nullptr;
	duckdb_v2_logical_type_create_from_id(id_for_null, &lt, nullptr);
	duckdb_v2_value_ptr nv = nullptr;
	duckdb_v2_value_create_null(lt, &nv, nullptr);
	REQUIRE(getter(nv, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&nv, nullptr);
	duckdb_v2_logical_type_destroy(&lt, nullptr);
}

TEST_CASE("V2: float / double / hugeint / uhugeint wrong-type and NULL rejection", "[capi_v2][value][float][hugeint]") {
	float f = 0.0f;
	RejectWrongTypeAndNull(
	    []() {
		    duckdb_v2_value_ptr v = nullptr;
		    duckdb_v2_value_create_int32(0, &v, nullptr);
		    return v;
	    },
	    duckdb_v2_value_get_float, f, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT);

	double d = 0.0;
	RejectWrongTypeAndNull(
	    []() {
		    duckdb_v2_value_ptr v = nullptr;
		    duckdb_v2_value_create_float(0.0f, &v, nullptr);
		    return v;
	    },
	    duckdb_v2_value_get_double, d, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);

	RejectHugeintShapedGetter(
	    []() {
		    duckdb_v2_value_ptr v = nullptr;
		    duckdb_v2_value_create_int32(0, &v, nullptr);
		    return v;
	    },
	    duckdb_v2_value_get_hugeint, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT);

	// uhugeint shares hi/lo shape but with uint64 hi.
	uint64_t lo = 0;
	uint64_t hi = 0;
	duckdb_v2_value_ptr other = nullptr;
	duckdb_v2_value_create_int32(0, &other, nullptr);
	REQUIRE(duckdb_v2_value_get_uhugeint(other, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&other, nullptr);
	duckdb_v2_logical_type_ptr lt = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT, &lt, nullptr);
	duckdb_v2_value_ptr nv = nullptr;
	duckdb_v2_value_create_null(lt, &nv, nullptr);
	REQUIRE(duckdb_v2_value_get_uhugeint(nv, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&nv, nullptr);
	duckdb_v2_logical_type_destroy(&lt, nullptr);
}

TEST_CASE("V2: hugeint / uhugeint round-trip", "[capi_v2][value][hugeint]") {
	{
		// (lower=0x0123456789abcdefULL, upper=-1) → hugeint
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_hugeint(0x0123456789abcdefULL, int64_t(-1), &v, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		uint64_t lo = 0;
		int64_t hi = 0;
		REQUIRE(duckdb_v2_value_get_hugeint(v, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(lo == 0x0123456789abcdefULL);
		REQUIRE(hi == int64_t(-1));
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_uhugeint(0xfedcba9876543210ULL, 0x0123456789abcdefULL, &v, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		uint64_t lo = 0;
		uint64_t hi = 0;
		REQUIRE(duckdb_v2_value_get_uhugeint(v, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(lo == 0xfedcba9876543210ULL);
		REQUIRE(hi == 0x0123456789abcdefULL);
		duckdb_v2_value_destroy(&v, nullptr);
	}

	// Wrong type rejection.
	duckdb_v2_value_ptr i = nullptr;
	duckdb_v2_value_create_int32(0, &i, nullptr);
	uint64_t lo = 0;
	int64_t hi = 0;
	REQUIRE(duckdb_v2_value_get_hugeint(i, &lo, &hi, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	uint64_t hi_u = 0;
	REQUIRE(duckdb_v2_value_get_uhugeint(i, &lo, &hi_u, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&i, nullptr);
}

TEST_CASE("V2: float / double round-trip", "[capi_v2][value][float]") {
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_float(3.5f, &v, nullptr);
		float out = 0.0f;
		REQUIRE(duckdb_v2_value_get_float(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == 3.5f);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_double(2.71828, &v, nullptr);
		double out = 0.0;
		REQUIRE(duckdb_v2_value_get_double(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out == 2.71828);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

// ===========================================================================
// VARCHAR — borrow contract
// ===========================================================================

TEST_CASE("V2: varchar round-trip with embedded NUL + borrow lifetime", "[capi_v2][value][varchar][borrow]") {
	// Embedded NUL forces callers to use the returned length, not strlen.
	const char raw[] = {'a', '\0', 'b', 'c'};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(raw, 4, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char *borrowed = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_varchar(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(borrowed != nullptr);
	REQUIRE(len == 4);
	REQUIRE(std::string(borrowed, len) == std::string(raw, 4));

	// Borrow stays valid across calls — same pointer or at least same bytes.
	const char *borrowed_again = nullptr;
	idx_t len_again = 0;
	duckdb_v2_value_get_varchar(v, &borrowed_again, &len_again, nullptr);
	REQUIRE(len_again == len);
	REQUIRE(std::memcmp(borrowed, borrowed_again, len) == 0);

	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: varchar empty (length=0, data may be null)", "[capi_v2][value][varchar]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(nullptr, 0, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char *borrowed = nullptr;
	idx_t len = 999;
	REQUIRE(duckdb_v2_value_get_varchar(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 0);
	// Per the borrow contract, an empty string still has a non-null
	// terminator-friendly pointer; we don't pin a specific value, only that
	// strlen(borrowed) == 0 == len for the empty case.
	REQUIRE(borrowed != nullptr);
	REQUIRE(borrowed[0] == '\0');

	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: varchar empty with non-null data ignores the byte at data", "[capi_v2][value][varchar]") {
	// length=0 with a non-null data pointer is also valid; the byte at *data
	// must not be touched. Use a non-empty, valid UTF-8 buffer to be sure
	// nothing in the validator / ctor reads past length.
	const char raw[] = "ignored";
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(raw, 0, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *borrowed = nullptr;
	idx_t len = 99;
	REQUIRE(duckdb_v2_value_get_varchar(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 0);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: varchar rejects null data with positive length", "[capi_v2][value][varchar]") {
	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(nullptr, 4, &v, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: varchar rejects invalid UTF-8", "[capi_v2][value][varchar]") {
	// 0xC0 0x80 is the modified-UTF-8 NUL — illegal in standard UTF-8.
	const unsigned char bad[] = {0xC0, 0x80};
	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(reinterpret_cast<const char *>(bad), 2, &v, &err) ==
	        DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: varchar getter rejects non-VARCHAR and NULL VARCHAR", "[capi_v2][value][varchar]") {
	duckdb_v2_value_ptr i = nullptr;
	duckdb_v2_value_create_int32(0, &i, nullptr);
	const char *p = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_varchar(i, &p, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&i, nullptr);

	duckdb_v2_logical_type_ptr vc = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &vc, nullptr);
	duckdb_v2_value_ptr nv = nullptr;
	duckdb_v2_value_create_null(vc, &nv, nullptr);
	REQUIRE(duckdb_v2_value_get_varchar(nv, &p, &len, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&nv, nullptr);
	duckdb_v2_logical_type_destroy(&vc, nullptr);
}

TEST_CASE("V2: varchar V1-built value round-trips through V2 getter / destroy",
          "[capi_v2][value][varchar][v1_v2_bridge]") {
	// Same identity-cast trick as the logical_type tests: a V1-built
	// duckdb_value is heap duckdb::Value, so destroying it via V2 is correct.
	auto v1 = duckdb_create_varchar_length("héllo", 6); // UTF-8: h é l l o
	auto v = V1ValueToV2(v1);

	const char *borrowed = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_varchar(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 6);
	REQUIRE(std::string(borrowed, len) == "héllo");

	duckdb_v2_value_destroy(&v, nullptr);
}

// ===========================================================================
// BLOB / BIT — borrow contract
// ===========================================================================

TEST_CASE("V2: blob round-trip with raw bytes including NUL", "[capi_v2][value][blob]") {
	const uint8_t raw[] = {0x00, 0x01, 0x02, 0xff};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_blob(raw, 4, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	const uint8_t *borrowed = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_blob(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 4);
	REQUIRE(std::memcmp(borrowed, raw, 4) == 0);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: blob empty + reject null with positive length", "[capi_v2][value][blob]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_blob(nullptr, 0, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	const uint8_t *p = nullptr;
	idx_t len = 99;
	REQUIRE(duckdb_v2_value_get_blob(v, &p, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 0);
	duckdb_v2_value_destroy(&v, nullptr);

	REQUIRE(duckdb_v2_value_create_blob(nullptr, 8, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
}

TEST_CASE("V2: bit round-trip preserves padding byte + data bytes", "[capi_v2][value][bit]") {
	// Raw bit-string encoding: a 1-byte padding header (count of trailing
	// padding bits) followed by data bytes. Two data bytes + 3 padding bits.
	const uint8_t raw[] = {0x03, 0b10110000, 0b00000000};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_bit(raw, 3, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	const uint8_t *borrowed = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_bit(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 3);
	REQUIRE(std::memcmp(borrowed, raw, 3) == 0);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: bit boundary inputs are pass-through (core is permissive)", "[capi_v2][value][bit]") {
	// V2 doesn't validate the raw on-disk shape beyond what core does. Pin
	// the current permissive behaviour for length >= 1: a single-byte input
	// (padding header only, no data) and a padding-byte > 7 (semantically
	// nonsensical) both round-trip without error. If core ever starts
	// validating, this test will fail loudly and the spec should grow
	// per-byte validation.
	{
		const uint8_t only_padding[] = {0x00};
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_bit(only_padding, 1, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
		const uint8_t *borrowed = nullptr;
		idx_t len = 0;
		REQUIRE(duckdb_v2_value_get_bit(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 1);
		REQUIRE(borrowed[0] == 0x00);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		const uint8_t weird_padding[] = {0xff, 0xaa};
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_bit(weird_padding, 2, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
		const uint8_t *borrowed = nullptr;
		idx_t len = 0;
		REQUIRE(duckdb_v2_value_get_bit(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 2);
		REQUIRE(std::memcmp(borrowed, weird_padding, 2) == 0);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: bit constructor rejects null data and zero length", "[capi_v2][value][bit]") {
	// BIT requires data != NULL and length >= 1 — the padding header byte is
	// mandatory, so the empty form is malformed by encoding.
	const uint8_t one[] = {0x00};
	duckdb_v2_value_ptr v = nullptr;

	REQUIRE(duckdb_v2_value_create_bit(nullptr, 4, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);

	REQUIRE(duckdb_v2_value_create_bit(nullptr, 0, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);

	REQUIRE(duckdb_v2_value_create_bit(one, 0, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
}

TEST_CASE("V2: V1-built blob / bit / bignum round-trip through V2", "[capi_v2][value][v1_v2_bridge]") {
	// Same identity-cast invariant as the logical_type bridge: V1's
	// duckdb_value is a heap duckdb::Value, so V2 getter + V2 destroy work
	// on it. Exercising this for each string-backed primitive (and bignum)
	// keeps the trick proven across the value-module surface, not just one
	// varchar case.
	{
		const uint8_t bytes[] = {0xde, 0xad, 0xbe, 0xef};
		auto v1 = duckdb_create_blob(bytes, 4);
		auto v = V1ValueToV2(v1);
		const uint8_t *borrowed = nullptr;
		idx_t len = 0;
		REQUIRE(duckdb_v2_value_get_blob(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 4);
		REQUIRE(std::memcmp(borrowed, bytes, 4) == 0);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		// V1 duckdb_bit and duckdb_bignum take a non-const uint8_t * field; we
		// keep the buffer non-const here rather than casting it away.
		uint8_t bytes[] = {0x02, 0xc0, 0x00};
		duckdb_bit input {bytes, 3};
		auto v1 = duckdb_create_bit(input);
		auto v = V1ValueToV2(v1);
		const uint8_t *borrowed = nullptr;
		idx_t len = 0;
		REQUIRE(duckdb_v2_value_get_bit(v, &borrowed, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 3);
		REQUIRE(std::memcmp(borrowed, bytes, 3) == 0);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		uint8_t magnitude[] = {0x12, 0x34, 0x56};
		duckdb_bignum input {magnitude, 3, true};
		auto v1 = duckdb_create_bignum(input);
		auto v = V1ValueToV2(v1);
		uint8_t *out_data = nullptr;
		idx_t out_len = 0;
		bool is_negative = false;
		REQUIRE(duckdb_v2_value_get_bignum(v, &out_data, &out_len, &is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out_len == 3);
		REQUIRE(is_negative);
		REQUIRE(std::memcmp(out_data, magnitude, 3) == 0);
		free(out_data);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

// ===========================================================================
// BIGNUM — owned output (caller frees)
// ===========================================================================

TEST_CASE("V2: bignum positive round-trip (magnitude bytes match)", "[capi_v2][value][bignum]") {
	// 0x010203 = 66051, positive.
	const uint8_t magnitude[] = {0x01, 0x02, 0x03};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_bignum(magnitude, 3, false, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	bool is_negative = true;
	REQUIRE(duckdb_v2_value_get_bignum(v, &out_data, &out_len, &is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_data != nullptr);
	REQUIRE(out_len == 3);
	REQUIRE(!is_negative);
	REQUIRE(std::memcmp(out_data, magnitude, 3) == 0);
	free(out_data);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: bignum negative round-trip (magnitude bytes match, sign flag set)", "[capi_v2][value][bignum]") {
	// Magnitude 0x010203, is_negative = true: V2 must round-trip the same
	// magnitude bytes despite core's internal bit-inversion.
	const uint8_t magnitude[] = {0x01, 0x02, 0x03};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_bignum(magnitude, 3, true, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	bool is_negative = false;
	REQUIRE(duckdb_v2_value_get_bignum(v, &out_data, &out_len, &is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 3);
	REQUIRE(is_negative);
	REQUIRE(std::memcmp(out_data, magnitude, 3) == 0);
	free(out_data);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: bignum multi-byte negative round-trip (high-bit + trailing zero)", "[capi_v2][value][bignum]") {
	// Magnitude {0x80, 0x00} forces the encoder to bit-invert into {0x7f, 0xff}
	// internally, while still reporting the original magnitude on read. A
	// reader naively assuming "stored bytes == magnitude" would see 0x7fff
	// (32767, positive) instead of -32768.
	const uint8_t magnitude[] = {0x80, 0x00};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_bignum(magnitude, 2, true, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	bool is_negative = false;
	REQUIRE(duckdb_v2_value_get_bignum(v, &out_data, &out_len, &is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 2);
	REQUIRE(is_negative);
	REQUIRE(out_data[0] == 0x80);
	REQUIRE(out_data[1] == 0x00);
	free(out_data);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: bignum zero is one 0x00 byte (explicit form)", "[capi_v2][value][bignum]") {
	// Core's BIGNUM encoding requires at least one data byte. The canonical
	// representation of zero is a single 0x00 magnitude byte.
	const uint8_t zero[] = {0x00};
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_bignum(zero, 1, false, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	bool is_negative = true;
	REQUIRE(duckdb_v2_value_get_bignum(v, &out_data, &out_len, &is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 1);
	REQUIRE(!is_negative);
	REQUIRE(out_data[0] == 0x00);
	free(out_data);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: bignum constructor rejects null data and zero length", "[capi_v2][value][bignum]") {
	// BIGNUM has no empty encoding — the spec requires data != NULL and
	// length >= 1, with zero expressed explicitly as {0x00}. Every shape
	// that violates this returns INVALID_INPUT.
	const uint8_t one[] = {0x01};
	duckdb_v2_value_ptr v = nullptr;

	REQUIRE(duckdb_v2_value_create_bignum(nullptr, 4, false, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);

	REQUIRE(duckdb_v2_value_create_bignum(nullptr, 0, false, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);

	REQUIRE(duckdb_v2_value_create_bignum(one, 0, false, &v, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
}

TEST_CASE("V2: bignum getter rejects non-BIGNUM and NULL BIGNUM", "[capi_v2][value][bignum]") {
	duckdb_v2_value_ptr i = nullptr;
	duckdb_v2_value_create_int32(0, &i, nullptr);
	uint8_t *out_data = nullptr;
	idx_t len = 0;
	bool neg = false;
	REQUIRE(duckdb_v2_value_get_bignum(i, &out_data, &len, &neg, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(out_data == nullptr);
	duckdb_v2_value_destroy(&i, nullptr);

	duckdb_v2_logical_type_ptr bn_lt = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM, &bn_lt, nullptr);
	duckdb_v2_value_ptr nv = nullptr;
	duckdb_v2_value_create_null(bn_lt, &nv, nullptr);
	REQUIRE(duckdb_v2_value_get_bignum(nv, &out_data, &len, &neg, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(out_data == nullptr);
	duckdb_v2_value_destroy(&nv, nullptr);
	duckdb_v2_logical_type_destroy(&bn_lt, nullptr);
}

// ===========================================================================
// Date / time / timestamp / interval round-trips
// ===========================================================================

TEST_CASE("V2: date round-trip", "[capi_v2][value][date]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_date(20000, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	int32_t out = 0;
	REQUIRE(duckdb_v2_value_get_date(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == 20000);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: time / time_ns round-trip", "[capi_v2][value][time]") {
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_time(int64_t(12345678), &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_time(v, &out, nullptr);
		REQUIRE(out == 12345678);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_time_ns(int64_t(1234567890), &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_time_ns(v, &out, nullptr);
		REQUIRE(out == int64_t(1234567890));
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: time_tz round-trip preserves (micros, offset_seconds)", "[capi_v2][value][time_tz]") {
	// Pick an offset within the dtime_tz_t valid range (±15:59:59).
	const int64_t micros = int64_t(13) * 3600 * 1000000;
	const int32_t offset = 5 * 3600 + 30 * 60; // +05:30
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_time_tz(micros, offset, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	int64_t out_micros = 0;
	int32_t out_off = 0;
	REQUIRE(duckdb_v2_value_get_time_tz(v, &out_micros, &out_off, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_micros == micros);
	REQUIRE(out_off == offset);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: timestamp variants round-trip", "[capi_v2][value][timestamp]") {
	struct Case {
		const char *label;
		// 0=usec,1=sec,2=ms,3=ns,4=tz
		int kind;
	};
	const int64_t payload = 1700000000123456LL;
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp(payload, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp(v, &out, nullptr);
		REQUIRE(out == payload);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp_sec(1700000000, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp_sec(v, &out, nullptr);
		REQUIRE(out == 1700000000);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp_ms(1700000000123LL, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp_ms(v, &out, nullptr);
		REQUIRE(out == 1700000000123LL);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp_ns(payload * 1000LL, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp_ns(v, &out, nullptr);
		REQUIRE(out == payload * 1000LL);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp_tz(payload, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp_tz(v, &out, nullptr);
		REQUIRE(out == payload);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_timestamp_tz_ns(payload * 1000LL, &v, nullptr);
		int64_t out = 0;
		duckdb_v2_value_get_timestamp_tz_ns(v, &out, nullptr);
		REQUIRE(out == payload * 1000LL);

		// Wrong-type rejection: a TIMESTAMP_TZ value must not satisfy the
		// TIMESTAMP_TZ_NS getter (the two have distinct LogicalTypeIds).
		duckdb_v2_value_ptr tz = nullptr;
		duckdb_v2_value_create_timestamp_tz(payload, &tz, nullptr);
		REQUIRE(duckdb_v2_value_get_timestamp_tz_ns(tz, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
		duckdb_v2_value_destroy(&tz, nullptr);

		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: interval round-trip", "[capi_v2][value][interval]") {
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_interval(1, 2, int64_t(3000000), &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	int32_t months = 0;
	int32_t days = 0;
	int64_t micros = 0;
	REQUIRE(duckdb_v2_value_get_interval(v, &months, &days, &micros, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(months == 1);
	REQUIRE(days == 2);
	REQUIRE(micros == 3000000);
	duckdb_v2_value_destroy(&v, nullptr);
}

// ===========================================================================
// DECIMAL — width selects the physical storage; (lower, upper) is the scaled
// 128-bit signed integer payload.
// ===========================================================================

TEST_CASE("V2: decimal round-trip across all internal widths", "[capi_v2][value][decimal]") {
	struct Case {
		int64_t signed_payload;
		uint8_t width;
		uint8_t scale;
	};
	Case cases[] = {
	    {12345, 4, 2},                   // SMALLINT
	    {1234567, 9, 4},                 // INTEGER
	    {int64_t(-9876543210LL), 18, 6}, // BIGINT
	};
	for (auto &c : cases) {
		// Sign-extend the int64 payload to 128 bits.
		uint64_t lower = static_cast<uint64_t>(c.signed_payload);
		int64_t upper = (c.signed_payload < 0) ? int64_t(-1) : int64_t(0);

		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_decimal(lower, upper, c.width, c.scale, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

		uint64_t out_lo = 0;
		int64_t out_hi = 0;
		uint8_t out_w = 0;
		uint8_t out_s = 0;
		REQUIRE(duckdb_v2_value_get_decimal(v, &out_lo, &out_hi, &out_w, &out_s, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out_lo == lower);
		REQUIRE(out_hi == upper);
		REQUIRE(out_w == c.width);
		REQUIRE(out_s == c.scale);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: decimal with hugeint payload (width >= 19)", "[capi_v2][value][decimal]") {
	// Width 38 forces hugeint physical storage. Carry a value with non-zero
	// upper bits to confirm both halves round-trip.
	const uint64_t lower = 0xdeadbeefcafebabeULL;
	const int64_t upper = int64_t(0x0123456789abcdefLL);

	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_decimal(lower, upper, 38, 10, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	uint64_t out_lo = 0;
	int64_t out_hi = 0;
	uint8_t out_w = 0;
	uint8_t out_s = 0;
	REQUIRE(duckdb_v2_value_get_decimal(v, &out_lo, &out_hi, &out_w, &out_s, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_lo == lower);
	REQUIRE(out_hi == upper);
	REQUIRE(out_w == 38);
	REQUIRE(out_s == 10);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: decimal getter rejects non-DECIMAL", "[capi_v2][value][decimal]") {
	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_value_create_int32(0, &v, nullptr);
	uint64_t lo = 0;
	int64_t hi = 0;
	uint8_t w = 0;
	uint8_t s = 0;
	REQUIRE(duckdb_v2_value_get_decimal(v, &lo, &hi, &w, &s, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: decimal dispatches on width (small payload, large width = HUGEINT physical)",
          "[capi_v2][value][decimal][dispatch]") {
	// payload = 5 fits int64, but width=38 demands HUGEINT physical. Dispatch
	// must follow width, not payload. The round-trip is byte-identical and
	// the rendered string carries 38 digits of precision.
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_decimal(uint64_t(5), int64_t(0), 38, 0, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	uint64_t lo = 0;
	int64_t hi = 0;
	uint8_t w = 0;
	uint8_t s = 0;
	REQUIRE(duckdb_v2_value_get_decimal(v, &lo, &hi, &w, &s, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lo == 5);
	REQUIRE(hi == 0);
	REQUIRE(w == 38);
	REQUIRE(s == 0);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: decimal int64 boundary (width=18, max-magnitude payload)", "[capi_v2][value][decimal][dispatch]") {
	// Largest payload that still uses int64 physical (18 nines). Confirms
	// width=18 is inside the int64 path; flipping the dispatch in either
	// direction would break this.
	const int64_t signed_payload = 999999999999999999LL;
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_decimal(uint64_t(signed_payload), int64_t(0), 18, 0, &v, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	uint64_t lo = 0;
	int64_t hi = 0;
	uint8_t w = 0;
	uint8_t s = 0;
	REQUIRE(duckdb_v2_value_get_decimal(v, &lo, &hi, &w, &s, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lo == uint64_t(signed_payload));
	REQUIRE(hi == 0);
	REQUIRE(w == 18);
	duckdb_v2_value_destroy(&v, nullptr);
}

TEST_CASE("V2: decimal rejects payload that doesn't fit chosen width", "[capi_v2][value][decimal][dispatch]") {
	// Width=18 → int64 physical, but the supplied 128-bit payload has a
	// non-zero upper half (doesn't fit int64). Reject rather than silently
	// truncate.
	const uint64_t lower = 0x0123456789abcdefULL;
	const int64_t upper = int64_t(0x0123456789abcdefLL);
	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_value_create_decimal(lower, upper, 18, 4, &v, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(v == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

// ===========================================================================
// UUID
// ===========================================================================

TEST_CASE("V2: uuid round-trip", "[capi_v2][value][uuid]") {
	// Pick a value we can also verify against UUID::ToString().
	const uint64_t lower = 0xfedcba9876543210ULL;
	const uint64_t upper = 0x0123456789abcdefULL;
	duckdb_v2_value_ptr v = nullptr;
	REQUIRE(duckdb_v2_value_create_uuid(lower, upper, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint64_t out_lo = 0;
	uint64_t out_hi = 0;
	REQUIRE(duckdb_v2_value_get_uuid(v, &out_lo, &out_hi, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_lo == lower);
	REQUIRE(out_hi == upper);
	duckdb_v2_value_destroy(&v, nullptr);
}

// ===========================================================================
// value_to_string — owned output, free with free()
// ===========================================================================

TEST_CASE("V2: value_to_string for primitives", "[capi_v2][value][to_string]") {
	{
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_int32(42, &v, nullptr);
		char *out = nullptr;
		REQUIRE(duckdb_v2_value_to_string(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out != nullptr);
		REQUIRE(std::string(out) == "42");
		free(out);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		// NULL value renders as "NULL".
		duckdb_v2_logical_type_ptr lt = nullptr;
		duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &lt, nullptr);
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_null(lt, &v, nullptr);
		char *out = nullptr;
		REQUIRE(duckdb_v2_value_to_string(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out) == "NULL");
		free(out);
		duckdb_v2_value_destroy(&v, nullptr);
		duckdb_v2_logical_type_destroy(&lt, nullptr);
	}
	{
		// VARCHAR renders as the raw bytes (no SQL quoting — ToString, not
		// ToSQLString). Confirms the fresh-output, malloc'd contract on a
		// string-backed value.
		duckdb_v2_value_ptr v = nullptr;
		duckdb_v2_value_create_varchar("hi", 2, &v, nullptr);
		char *out = nullptr;
		REQUIRE(duckdb_v2_value_to_string(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out) == "hi");
		free(out);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		// DECIMAL renders with scale-aware decimal point.
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_decimal(uint64_t(12345), int64_t(0), 5, 2, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
		char *out = nullptr;
		REQUIRE(duckdb_v2_value_to_string(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out) == "123.45");
		free(out);
		duckdb_v2_value_destroy(&v, nullptr);
	}
	{
		// BIGNUM renders as a base-10 integer string.
		const uint8_t mag[] = {0x01, 0x00}; // 256
		duckdb_v2_value_ptr v = nullptr;
		REQUIRE(duckdb_v2_value_create_bignum(mag, 2, false, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
		char *out = nullptr;
		REQUIRE(duckdb_v2_value_to_string(v, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(out) == "256");
		free(out);
		duckdb_v2_value_destroy(&v, nullptr);
	}
}

TEST_CASE("V2: value_to_string null handle / null out", "[capi_v2][value][to_string]") {
	char *out = nullptr;
	REQUIRE(duckdb_v2_value_to_string(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_value_create_int32(1, &v, nullptr);
	REQUIRE(duckdb_v2_value_to_string(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_value_destroy(&v, nullptr);
}

// ===========================================================================
// Error info propagation: a sample failure path attaches an info handle.
// ===========================================================================

TEST_CASE("V2: failure path populates error info", "[capi_v2][value][error]") {
	duckdb_v2_value_ptr v = nullptr;
	duckdb_v2_error_info_ptr err = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar(nullptr, 4, &v, &err) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(err != nullptr);
	const char *msg = nullptr;
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(msg != nullptr);
	REQUIRE(std::strlen(msg) > 0);
	duckdb_v2_error_info_destroy(&err);
}
