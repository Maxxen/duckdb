#include "test_capi_v2.hpp"

#include "duckdb.h" // V1 C API: used only by the [v1_v2_bridge] cross-API parity pin.

// ---------------------------------------------------------------------------
// V2 value tests — primitive surface.
//
// Same identity-cast invariant as the logical_type bridge: both V1
// duckdb_value and V2 duckdb_v2_value_handle are heap-allocated duckdb::Value
// cast to void *. We rely on it to round-trip a V1-built fixture through V2
// destroy in one place (the V1-built leaf test), the same way the
// logical_type tests reuse V1 fixtures. If a wrapper is added later, this
// file must change.
//
// Borrow contract being verified throughout: value_get_data returns a pointer
// that stays valid until value_destroy is called. That holds for BIGNUM too —
// what it borrows is the opaque storage, which bignum_decode then translates
// into a magnitude in a buffer the caller owns.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {
namespace {

duckdb_v2_value_handle V1ValueToV2(duckdb_value v) {
	return reinterpret_cast<duckdb_v2_value_handle>(v);
}

} // namespace

// ===========================================================================
// Lifecycle: destroy null-safety
// ===========================================================================

TEST_CASE("V2: value destroy is null-safe", "[capi_v2][value][lifecycle]") {
	REQUIRE(duckdb_v2_value_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle already_null = nullptr;
	REQUIRE(duckdb_v2_value_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);
}

// ===========================================================================
// NULL construction + is_null + get_logical_type ownership
// ===========================================================================

TEST_CASE("V2: value_create_null carries the borrowed type", "[capi_v2][value][null]") {
	EnvFixture fx;
	duckdb_v2_logical_type_handle int_type = nullptr;
	REQUIRE(duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0,
	                                                 &int_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_null(int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);

	// Input type is borrowed: destroying it must not affect the NULL value.
	REQUIRE(duckdb_v2_logical_type_destroy(&int_type) == DUCKDB_V2_ERROR_NONE);

	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(v, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);

	duckdb_v2_logical_type_handle out_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(v, &out_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_type != nullptr);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(out_type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	// Caller owns the returned logical type.
	REQUIRE(duckdb_v2_logical_type_destroy(&out_type) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_value_destroy(&v) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: value_create_null rejects null type / null out", "[capi_v2][value][null]") {
	EnvFixture fx;
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_null(nullptr, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	REQUIRE(duckdb_v2_value_create_null(int_type, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&int_type);
}

TEST_CASE("V2: value_is_null distinguishes NULL from non-NULL", "[capi_v2][value][null]") {
	EnvFixture fx;
	duckdb_v2_value_handle v = nullptr;
	v = MakeInt32Value(fx.conn, 7);
	bool is_null = true;
	REQUIRE(duckdb_v2_value_is_null(v, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!is_null);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: value_is_null / value_get_logical_type / value_destroy null guards", "[capi_v2][value][null]") {
	EnvFixture fx;
	bool b = false;
	REQUIRE(duckdb_v2_value_is_null(nullptr, &b, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_logical_type_handle lt = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(nullptr, &lt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(lt == nullptr);

	duckdb_v2_value_handle v = nullptr;
	v = MakeInt32Value(fx.conn, 1);
	REQUIRE(duckdb_v2_value_is_null(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_logical_type(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&v);
}

// ===========================================================================
// Leaf payload codec: value_create_from_data / value_get_data
// ===========================================================================

namespace {

// Builds a leaf value of a non-primitive-id type (DECIMAL / ENUM) from its
// payload; MakeValue covers the create_from_id kinds.
duckdb_v2_value_handle LeafOfType(duckdb_v2_logical_type_handle type, const void *data, idx_t len) {
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(type, data, len, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	return v;
}

// Round-trips a payload through the codec, optionally pinning the rendering.
void RequireLeafRoundTrip(duckdb_v2_connection_handle conn, DUCKDB_V2_LOGICAL_TYPE_ID id, const void *payload,
                          idx_t len, const char *expected_text = nullptr) {
	auto v = MakeValue(conn, id, payload, len);
	// Read everything, destroy the owned fixtures, then assert: a failing
	// REQUIRE in between would leak them.
	const void *out = nullptr;
	idx_t out_len = 0;
	auto data_rc = duckdb_v2_value_get_data(v, &out, &out_len, nullptr);
	const bool len_ok = (out_len == len);
	// Guard the empty case: {NULL, 0} is the canonical empty payload, and
	// glibc declares memcmp's pointers nonnull even for length 0.
	const bool bytes_ok =
	    (data_rc == DUCKDB_V2_ERROR_NONE) && len_ok && (len == 0 || std::memcmp(out, payload, len) == 0);
	auto render_rc = DUCKDB_V2_ERROR_NONE;
	std::string rendered_text;
	if (expected_text) {
		rendered_text = RenderText(
		    [&](char *buf, idx_t cap, idx_t *out) { return duckdb_v2_value_to_string(v, buf, cap, out, nullptr); },
		    render_rc);
	}
	duckdb_v2_value_destroy(&v);
	REQUIRE(data_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len_ok);
	REQUIRE(bytes_ok);
	if (expected_text) {
		REQUIRE(render_rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(rendered_text == expected_text);
	}
}

} // namespace

TEST_CASE("V2: leaf payloads round-trip across the fixed-size kinds", "[capi_v2][value][leaf]") {
	EnvFixture fx;
	bool b = true;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, &b, 1, "true");
	int8_t i8 = -5;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT, &i8, 1, "-5");
	int16_t i16 = -1234;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT, &i16, 2, "-1234");
	int32_t i32 = -123456;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &i32, 4, "-123456");
	int64_t i64 = -1234567890123;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &i64, 8, "-1234567890123");
	uint8_t u8 = 200;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT, &u8, 1, "200");
	uint16_t u16 = 60000;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT, &u16, 2, "60000");
	uint32_t u32 = 4000000000u;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER, &u32, 4, "4000000000");
	uint64_t u64 = 18000000000000000000ull;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT, &u64, 8, "18000000000000000000");
	float f = 1.5f;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT, &f, 4, "1.5");
	double d = -2.25;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, &d, 8, "-2.25");

	// DATE is int32 days since the epoch; 19797 = 2024-03-15.
	int32_t days = 19797;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_DATE, &days, 4, "2024-03-15");

	// The time and timestamp kinds carry int64 offsets in their unit; the
	// packed TIME_TZ form round-trips as bytes.
	int64_t micros = ((12 * 60 + 34) * 60 + 56) * 1000000ll;
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIME, &micros, 8, "12:34:56");
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ, &micros, 8);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS, &micros, 8);

	// INTERVAL is the (months, days, micros) triple.
	duckdb_v2_interval_t iv {1, 2, 3000000};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL, &iv, sizeof(iv), "1 month 2 days 00:00:03");
}

TEST_CASE("V2: hugeint and uuid payloads use the committed 128-bit layout", "[capi_v2][value][leaf]") {
	EnvFixture fx;
	duckdb_v2_hugeint_t pos {42, 0};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT, &pos, sizeof(pos), "42");
	duckdb_v2_hugeint_t neg {0xFFFFFFFFFFFFFFFFull, -1};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT, &neg, sizeof(neg), "-1");
	duckdb_v2_uhugeint_t upos {7, 1};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT, &upos, sizeof(upos), "18446744073709551623");

	// UUID crosses in its internal hugeint form, exactly the bytes a vector
	// view exposes (not the RFC textual order); to_string still renders the
	// canonical 36-char form.
	duckdb_v2_hugeint_t raw {0x1234, 0x5678};
	auto v = MakeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_UUID, &raw, sizeof(raw));
	const void *out = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_value_get_data(v, &out, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 16);
	REQUIRE(std::memcmp(out, &raw, 16) == 0);
	auto rendered = Render(v);
	REQUIRE(rendered.size() == 36);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: DECIMAL payloads are the scaled integer of the width tier", "[capi_v2][value][leaf][decimal]") {
	EnvFixture fx;
	struct {
		uint8_t width;
		uint8_t scale;
		int64_t scaled;
		idx_t len;
		const char *text;
	} cases[] = {
	    {4, 2, 1234, 2, "12.34"},
	    {9, 4, 123456789, 4, "12345.6789"},
	    {18, 6, 123456789012345678, 8, "123456789012.345678"},
	};
	for (auto &c : cases) {
		auto t =
		    MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, c.width), MakeInt32Value(fx.conn, c.scale)});
		// Assemble the tier-sized little payload from the low bytes.
		auto v = LeafOfType(t, &c.scaled, c.len);
		const void *out = nullptr;
		idx_t out_len = 0;
		REQUIRE(duckdb_v2_value_get_data(v, &out, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out_len == c.len);
		REQUIRE(std::memcmp(out, &c.scaled, c.len) == 0);
		auto rendered = Render(v);
		REQUIRE(rendered == c.text);
		duckdb_v2_value_destroy(&v);
		duckdb_v2_logical_type_destroy(&t);
	}

	// Width 38 sits in the int128 tier.
	auto wide = MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, 38), MakeInt32Value(fx.conn, 10)});
	duckdb_v2_hugeint_t scaled {123456789012345ull, 0};
	auto v = LeafOfType(wide, &scaled, sizeof(scaled));
	REQUIRE(ConsumeValue<duckdb_v2_hugeint_t>(v).lower == scaled.lower);
	duckdb_v2_value_destroy(&v);

	// The tier table is load-bearing: a mismatched payload size is refused.
	int32_t wrong = 1234;
	auto narrow = MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, 4), MakeInt32Value(fx.conn, 2)});
	duckdb_v2_value_handle bad = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(narrow, &wrong, sizeof(wrong), &bad, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(bad == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Layout-raw: semantic invariants are not validated. A width-4 payload
	// above 9999 constructs and renders; value_cast is the validating path.
	int16_t overflow = 30000;
	auto over = LeafOfType(narrow, &overflow, sizeof(overflow));
	auto rendered = Render(over);
	REQUIRE(rendered == "300.00");
	duckdb_v2_value_destroy(&over);
	duckdb_v2_logical_type_destroy(&narrow);
	duckdb_v2_logical_type_destroy(&wide);
}

TEST_CASE("V2: ENUM payloads are bounds-checked dictionary indices", "[capi_v2][value][leaf][enum]") {
	EnvFixture fx;
	auto t = MakeType(
	    fx.conn, "enum", nullptr,
	    {MakeVarcharValue(fx.conn, "sad"), MakeVarcharValue(fx.conn, "ok"), MakeVarcharValue(fx.conn, "happy")});

	uint8_t index = 2;
	auto v = LeafOfType(t, &index, 1);
	// Render first: ConsumeValue destroys the value it reads.
	REQUIRE(Render(v) == "happy");
	REQUIRE(ConsumeValue<uint8_t>(v) == 2);

	// An out-of-range index is not addressable storage: the one semantic
	// gate the layout-raw constructor keeps.
	uint8_t oob = 3;
	duckdb_v2_value_handle bad = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(t, &oob, 1, &bad, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(bad == nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: wire-bytes kinds round-trip verbatim", "[capi_v2][value][leaf][borrow]") {
	EnvFixture fx;
	// VARCHAR with an embedded NUL; the borrow is stable until destroy.
	const char raw[4] = {'a', '\0', 'b', 'c'};
	auto v = MakeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, raw, 4);
	const void *first = nullptr;
	idx_t first_len = 0;
	REQUIRE(duckdb_v2_value_get_data(v, &first, &first_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first_len == 4);
	REQUIRE(std::memcmp(first, raw, 4) == 0);
	const void *second = nullptr;
	idx_t second_len = 0;
	REQUIRE(duckdb_v2_value_get_data(v, &second, &second_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(second == first);
	duckdb_v2_value_destroy(&v);

	// Empty VARCHAR: a null pointer is valid when len is 0.
	auto empty = MakeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, nullptr, 0);
	REQUIRE(ConsumeValue<std::string>(empty).empty());
	duckdb_v2_value_destroy(&empty);

	// The engine rejects invalid UTF-8 for VARCHAR at construction.
	const unsigned char bad_utf8[2] = {0xC0, 0x00};
	auto varchar_type = DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;
	duckdb_v2_logical_type_handle vt = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, varchar_type, nullptr, nullptr, 0, &vt, nullptr);
	duckdb_v2_value_handle bad = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(vt, bad_utf8, 2, &bad, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(bad == nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&vt);

	// BLOB takes any bytes.
	const uint8_t blob_bytes[5] = {0x00, 0xFF, 0x10, 0x00, 0x7F};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BLOB, blob_bytes, 5);
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BLOB, nullptr, 0);

	// BIT wire form: the mandatory padding header byte plus data bytes.
	const uint8_t bit_bytes[3] = {3, 0xA8, 0xF0};
	RequireLeafRoundTrip(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIT, bit_bytes, 3);
	duckdb_v2_logical_type_handle bit_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIT, nullptr, nullptr, 0, &bit_type,
	                                         nullptr);
	REQUIRE(duckdb_v2_value_create_from_data(bit_type, nullptr, 0, &bad, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(bad == nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&bit_type);
}

TEST_CASE("V2: V1-built leaf values read through value_get_data", "[capi_v2][value][leaf][v1_v2_bridge]") {
	// Same identity-cast invariant as before: V1 and V2 value handles are
	// both bare heap duckdb::Value.
	auto v1_int = V1ValueToV2(duckdb_create_int64(-77));
	REQUIRE(ConsumeValue<int64_t>(v1_int) == -77);
	duckdb_v2_value_destroy(&v1_int);

	const uint8_t blob_bytes[3] = {1, 0, 2};
	auto v1_blob = V1ValueToV2(duckdb_create_blob(blob_bytes, 3));
	REQUIRE(ConsumeValue<std::string>(v1_blob) == std::string("\x01\x00\x02", 3));
	duckdb_v2_value_destroy(&v1_blob);
}

TEST_CASE("V2: leaf codec refuses kinds without a committed layout", "[capi_v2][value][leaf]") {
	EnvFixture fx;
	// TYPE: use value_create_type / value_get_type.
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	duckdb_v2_value_handle type_value = nullptr;
	REQUIRE(duckdb_v2_value_create_type(int_type, &type_value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle type_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(type_value, &type_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	int32_t dummy = 0;
	duckdb_v2_value_handle out = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(type_type, &dummy, 4, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	duckdb_v2_error_info_destroy(&err);
	const void *data = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_data(type_value, &data, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(data == nullptr);
	duckdb_v2_logical_type_destroy(&type_type);
	duckdb_v2_value_destroy(&type_value);

	// BIGNUM is leaf-addressable, but its storage form needs a header plus at
	// least one magnitude byte; anything shorter is not addressable storage.
	duckdb_v2_logical_type_handle bignum_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM, nullptr, nullptr, 0,
	                                         &bignum_type, nullptr);
	REQUIRE(duckdb_v2_value_create_from_data(bignum_type, &dummy, 3, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&bignum_type);

	// Composites: use value_create / value_get_child.
	auto list_type = MakeType(fx.conn, "list", nullptr, {MakeTypeValue(int_type)});
	REQUIRE(duckdb_v2_value_create_from_data(list_type, &dummy, 4, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&list_type);

	// NULL values have no payload.
	duckdb_v2_value_handle null_value = nullptr;
	REQUIRE(duckdb_v2_value_create_null(int_type, &null_value, nullptr) == DUCKDB_V2_ERROR_NONE);
	data = reinterpret_cast<const void *>(0x1);
	REQUIRE(duckdb_v2_value_get_data(null_value, &data, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(data == nullptr);
	duckdb_v2_value_destroy(&null_value);

	// Length and null-arg gates.
	int64_t wide = 0;
	REQUIRE(duckdb_v2_value_create_from_data(int_type, &wide, 8, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_from_data(int_type, nullptr, 4, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_from_data(nullptr, &dummy, 4, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_from_data(int_type, &dummy, 4, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto probe = MakeInt32Value(fx.conn, 1);
	REQUIRE(duckdb_v2_value_get_data(nullptr, &data, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_data(probe, nullptr, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_data(probe, &data, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&probe);
	duckdb_v2_logical_type_destroy(&int_type);

	// VARIANT: no committed layout; access is the boxed vector_get_value /
	// set_value path plus value_get_variant to unwrap. The type comes from
	// from_text (create_from_id rejects VARIANT).
	EnvFixture f;
	duckdb_v2_logical_type_handle variant_type = nullptr;
	REQUIRE(duckdb_v2_connection_create_type_from_text(f.conn, Convert("VARIANT"), &variant_type, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_create_from_data(variant_type, &dummy, 4, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	duckdb_v2_logical_type_destroy(&variant_type);
}

TEST_CASE("V2: value_to_string null handle / short buffer", "[capi_v2][value][to_string]") {
	EnvFixture fx;
	char buf[64] = {};
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_to_string(nullptr, buf, sizeof(buf), &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	auto v = MakeInt32Value(fx.conn, 7);
	// out_length carries the answer, so it is mandatory in both modes.
	REQUIRE(duckdb_v2_value_to_string(v, buf, sizeof(buf), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Sizing excludes the terminator; the buffer must still have room for it.
	REQUIRE(duckdb_v2_value_to_string(v, nullptr, 0, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == 1);
	REQUIRE(duckdb_v2_value_to_string(v, buf, len, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE);
	REQUIRE(len == 1);
	REQUIRE(duckdb_v2_value_to_string(v, buf, len + 1, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buf) == "7");
	duckdb_v2_value_destroy(&v);
}

// ===========================================================================
// BIGNUM — storage bytes through the generic leaf codec, magnitude through
// the bignum codec pair. Neither direction allocates on the caller's behalf.
// ===========================================================================

namespace {

// Builds a BIGNUM value the way the API intends: encode the magnitude into
// storage bytes, then hand those to the generic leaf constructor.
duckdb_v2_value_handle BignumValue(duckdb_v2_connection_handle conn, const uint8_t *magnitude, idx_t length,
                                   bool is_negative) {
	idx_t storage_len = 0;
	REQUIRE(duckdb_v2_bignum_encode(magnitude, length, is_negative, nullptr, 0, &storage_len, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	std::vector<uint8_t> storage(storage_len);
	REQUIRE(duckdb_v2_bignum_encode(magnitude, length, is_negative, storage.data(), storage.size(), &storage_len,
	                                nullptr) == DUCKDB_V2_ERROR_NONE);

	auto type = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM);
	duckdb_v2_value_handle v = nullptr;
	auto rc = duckdb_v2_value_create_from_data(type, storage.data(), storage.size(), &v, nullptr);
	duckdb_v2_logical_type_destroy(&type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return v;
}

// Reads one back: borrow the storage bytes, then decode them.
std::vector<uint8_t> BignumMagnitude(duckdb_v2_value_handle v, bool &out_is_negative) {
	const void *storage = nullptr;
	idx_t storage_len = 0;
	REQUIRE(duckdb_v2_value_get_data(v, &storage, &storage_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	const auto *bytes = static_cast<const uint8_t *>(storage);

	idx_t mag_len = 0;
	REQUIRE(duckdb_v2_bignum_decode(bytes, storage_len, nullptr, 0, &mag_len, &out_is_negative, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	std::vector<uint8_t> magnitude(mag_len);
	REQUIRE(duckdb_v2_bignum_decode(bytes, storage_len, magnitude.data(), magnitude.size(), &mag_len, &out_is_negative,
	                                nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(mag_len == magnitude.size());
	return magnitude;
}

} // namespace

TEST_CASE("V2: bignum positive round-trip (magnitude bytes match)", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// 0x010203 = 66051, positive.
	const uint8_t magnitude[] = {0x01, 0x02, 0x03};
	auto v = BignumValue(fx.conn, magnitude, 3, false);

	bool is_negative = true;
	auto out = BignumMagnitude(v, is_negative);
	REQUIRE(out.size() == 3);
	REQUIRE(!is_negative);
	REQUIRE(std::memcmp(out.data(), magnitude, 3) == 0);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: bignum negative round-trip (magnitude bytes match, sign flag set)", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// Magnitude 0x010203, is_negative = true: V2 must round-trip the same
	// magnitude bytes despite core's internal bit-inversion.
	const uint8_t magnitude[] = {0x01, 0x02, 0x03};
	auto v = BignumValue(fx.conn, magnitude, 3, true);

	bool is_negative = false;
	auto out = BignumMagnitude(v, is_negative);
	REQUIRE(out.size() == 3);
	REQUIRE(is_negative);
	REQUIRE(std::memcmp(out.data(), magnitude, 3) == 0);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: bignum multi-byte negative round-trip (high-bit + trailing zero)", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// Magnitude {0x80, 0x00} forces the encoder to bit-invert into {0x7f, 0xff}
	// internally, while still reporting the original magnitude on read. A
	// reader naively assuming "stored bytes == magnitude" would see 0x7fff
	// (32767, positive) instead of -32768.
	const uint8_t magnitude[] = {0x80, 0x00};
	auto v = BignumValue(fx.conn, magnitude, 2, true);

	bool is_negative = false;
	auto out = BignumMagnitude(v, is_negative);
	REQUIRE(out.size() == 2);
	REQUIRE(is_negative);
	REQUIRE(out[0] == 0x80);
	REQUIRE(out[1] == 0x00);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: bignum zero is one 0x00 byte (explicit form)", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// Core's BIGNUM encoding requires at least one data byte. The canonical
	// representation of zero is a single 0x00 magnitude byte.
	const uint8_t zero[] = {0x00};
	auto v = BignumValue(fx.conn, zero, 1, false);

	bool is_negative = true;
	auto out = BignumMagnitude(v, is_negative);
	REQUIRE(out.size() == 1);
	REQUIRE(!is_negative);
	REQUIRE(out[0] == 0x00);
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: value_get_data refuses a NULL BIGNUM", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// BIGNUM is leaf-addressable, but a NULL has no payload to hand back.
	auto bn_lt = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM);
	duckdb_v2_value_handle nv = nullptr;
	duckdb_v2_value_create_null(bn_lt, &nv, nullptr);

	const void *out_data = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_data(nv, &out_data, &len, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out_data == nullptr);
	duckdb_v2_value_destroy(&nv);
	duckdb_v2_logical_type_destroy(&bn_lt);
}

// ===========================================================================
// Error info propagation: a sample failure path attaches an info handle.
// ===========================================================================

TEST_CASE("V2: failure path populates error info", "[capi_v2][value][error]") {
	EnvFixture fx;
	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, nullptr, nullptr, 0,
	                                         &varchar_type, nullptr);
	duckdb_v2_value_handle v = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_from_data(varchar_type, nullptr, 4, &v, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&varchar_type);
}

// ===========================================================================
// TYPE values (a logical type carried as a value)
// ===========================================================================

TEST_CASE("V2: TYPE value wraps and unwraps a logical type", "[capi_v2][value][type_value]") {
	EnvFixture fx;
	duckdb_v2_logical_type_handle int_type = nullptr;
	REQUIRE(duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0,
	                                                 &int_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type(int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	// Input type is borrowed: destroying it must not affect the value.
	REQUIRE(duckdb_v2_logical_type_destroy(&int_type) == DUCKDB_V2_ERROR_NONE);

	bool is_null = true;
	REQUIRE(duckdb_v2_value_is_null(v, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!is_null);

	// The value's own type is TYPE.
	duckdb_v2_logical_type_handle value_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(v, &value_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(value_type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_TYPE);
	duckdb_v2_logical_type_destroy(&value_type);

	// Unwrap: an owned copy equal to the wrapped type.
	duckdb_v2_logical_type_handle unwrapped = nullptr;
	REQUIRE(duckdb_v2_value_get_type(v, &unwrapped, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(unwrapped != nullptr);
	duckdb_v2_logical_type_handle expected = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &expected,
	                                         nullptr);
	bool equal = false;
	REQUIRE(duckdb_v2_logical_type_is_equal(unwrapped, expected, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(equal);
	duckdb_v2_logical_type_destroy(&expected);
	duckdb_v2_logical_type_destroy(&unwrapped);

	// The unwrapped copy is independent of the value.
	REQUIRE(duckdb_v2_value_get_type(v, &unwrapped, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&v);
	DUCKDB_V2_LOGICAL_TYPE_ID unwrapped_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(unwrapped, &unwrapped_id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(unwrapped_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&unwrapped);
}

TEST_CASE("V2: TYPE value round-trips a composite type", "[capi_v2][value][type_value]") {
	EnvFixture fx;
	// Nested composite: STRUCT(a INTEGER[], b VARCHAR) survives the value's
	// internal serialize/deserialize round trip.
	auto list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	std::vector<const char *> names = {"a", "b"};
	auto t = MakeType(fx.conn, "struct", &names,
	                  {MakeTypeValue(list_type), MakeTypeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	duckdb_v2_logical_type_destroy(&list_type);

	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type(t, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_handle unwrapped = nullptr;
	REQUIRE(duckdb_v2_value_get_type(v, &unwrapped, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool equal = false;
	REQUIRE(duckdb_v2_logical_type_is_equal(t, unwrapped, &equal, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(equal);

	duckdb_v2_logical_type_destroy(&unwrapped);
	duckdb_v2_value_destroy(&v);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: TYPE value to_string renders the type text", "[capi_v2][value][type_value][to_string]") {
	EnvFixture fx;
	auto t = MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, 18), MakeInt32Value(fx.conn, 3)});
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type(t, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&t);

	auto str = Render(v);
	REQUIRE(str == "DECIMAL(18,3)");
	duckdb_v2_value_destroy(&v);
}

TEST_CASE("V2: value_get_type rejects non-TYPE and NULL TYPE values", "[capi_v2][value][type_value]") {
	EnvFixture fx;
	// Wrong kind: an INTEGER value is not a TYPE value.
	duckdb_v2_value_handle int_value = MakeInt32Value(fx.conn, 42);
	duckdb_v2_logical_type_handle out = reinterpret_cast<duckdb_v2_logical_type_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_get_type(int_value, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_value_destroy(&int_value);

	// NULL TYPE value: no payload to unwrap. The TYPE logical type is taken
	// from a TYPE value's own type (create_from_id does not construct it).
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	duckdb_v2_value_handle type_value = nullptr;
	REQUIRE(duckdb_v2_value_create_type(int_type, &type_value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&int_type);
	duckdb_v2_logical_type_handle type_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(type_value, &type_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&type_value);

	duckdb_v2_value_handle null_type_value = nullptr;
	REQUIRE(duckdb_v2_value_create_null(type_type, &null_type_value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&type_type);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(null_type_value, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	out = reinterpret_cast<duckdb_v2_logical_type_handle>(0x1);
	REQUIRE(duckdb_v2_value_get_type(null_type_value, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_value_destroy(&null_type_value);
}

TEST_CASE("V2: value_create_type / value_get_type null-arg refusals", "[capi_v2][value][type_value]") {
	EnvFixture fx;
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type(nullptr, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	REQUIRE(duckdb_v2_value_create_type(int_type, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_logical_type_handle out = nullptr;
	REQUIRE(duckdb_v2_value_get_type(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_value_create_type(int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_get_type(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&v);
	duckdb_v2_logical_type_destroy(&int_type);
}

// ===========================================================================
// Composite construction + descent (value_create / get_child_count / get_child)
// ===========================================================================

namespace {

duckdb_v2_value_handle V2I32(duckdb_v2_connection_handle conn, int32_t x) {
	return MakeInt32Value(conn, x);
}

duckdb_v2_value_handle V2Varchar(duckdb_v2_connection_handle conn, const char *s) {
	return MakeVarcharValue(conn, s);
}

duckdb_v2_value_handle V2NullOf(duckdb_v2_connection_handle conn, DUCKDB_V2_LOGICAL_TYPE_ID id) {
	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_connection_create_type_from_id(conn, id, nullptr, nullptr, 0, &t, nullptr);
	duckdb_v2_value_handle v = nullptr;
	auto rc = duckdb_v2_value_create_null(t, &v, nullptr);
	duckdb_v2_logical_type_destroy(&t);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return v;
}

// Builds a composite value, destroying the borrowed children.
duckdb_v2_value_handle V2Composite(duckdb_v2_logical_type_handle type, std::vector<duckdb_v2_value_handle> children) {
	duckdb_v2_value_handle v = nullptr;
	auto rc = duckdb_v2_value_create(type, children.empty() ? nullptr : children.data(), children.size(), &v, nullptr);
	for (auto &c : children) {
		duckdb_v2_value_destroy(&c);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	return v;
}

// Same, expecting failure: returns the code, checks out nulling + err.
DUCKDB_V2_ERROR V2CompositeErr(duckdb_v2_logical_type_handle type, std::vector<duckdb_v2_value_handle> children) {
	auto v = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_value_create(type, children.empty() ? nullptr : children.data(), children.size(), &v, &err);
	for (auto &c : children) {
		duckdb_v2_value_destroy(&c);
	}
	const bool out_nulled = (v == nullptr);
	const bool err_set = (err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_nulled);
	REQUIRE(err_set);
	return rc;
}

idx_t V2ChildCount(duckdb_v2_value_handle v) {
	idx_t count = 99;
	REQUIRE(duckdb_v2_value_get_child_count(v, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	return count;
}

duckdb_v2_value_handle V2Child(duckdb_v2_value_handle v, idx_t index) {
	duckdb_v2_value_handle child = nullptr;
	REQUIRE(duckdb_v2_value_get_child(v, index, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(child != nullptr);
	return child;
}

// Renders child `index` via value_to_string (type-agnostic comparisons).
std::string V2ChildText(duckdb_v2_value_handle v, idx_t index) {
	auto child = V2Child(v, index);
	auto out = Render(child);
	duckdb_v2_value_destroy(&child);
	return out;
}

} // namespace

TEST_CASE("V2: value_create LIST round-trips elements, NULLs, and empty", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	auto list = V2Composite(
	    list_type, {V2I32(fx.conn, 1), V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2I32(fx.conn, 3)});
	REQUIRE(V2ChildCount(list) == 3);
	REQUIRE(V2ChildText(list, 0) == "1");
	REQUIRE(V2ChildText(list, 2) == "3");
	auto middle = V2Child(list, 1);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(middle, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	duckdb_v2_value_destroy(&middle);
	// Children are owned copies: the parent survives destroying them.
	REQUIRE(V2ChildCount(list) == 3);
	duckdb_v2_value_destroy(&list);

	auto empty = V2Composite(list_type, {});
	REQUIRE(V2ChildCount(empty) == 0);
	duckdb_v2_value_handle child = nullptr;
	REQUIRE(duckdb_v2_value_get_child(empty, 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr);
	duckdb_v2_value_destroy(&empty);
	duckdb_v2_logical_type_destroy(&list_type);
}

TEST_CASE("V2: value_create casts children to the declared child type", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	// An INTEGER child comes back as BIGINT.
	auto list = V2Composite(list_type, {V2I32(fx.conn, 7)});
	auto child = V2Child(list, 0);
	duckdb_v2_logical_type_handle child_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(child, &child_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(child_type, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	REQUIRE(ConsumeValue<int64_t>(child) == 7);
	duckdb_v2_logical_type_destroy(&child_type);
	duckdb_v2_value_destroy(&child);
	duckdb_v2_value_destroy(&list);
	duckdb_v2_logical_type_destroy(&list_type);

	// An uncastable child surfaces the conversion error.
	auto int_list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	// The engine's value-cast failure path throws InvalidInputException.
	REQUIRE(V2CompositeErr(int_list_type, {V2Varchar(fx.conn, "abc")}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&int_list_type);
}

TEST_CASE("V2: value_create ARRAY enforces the declared size", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto array_type = MakeType(fx.conn, "array", nullptr,
	                           {MakeTypeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), MakeInt32Value(fx.conn, 3)});

	auto arr = V2Composite(array_type, {V2I32(fx.conn, 1), V2I32(fx.conn, 2), V2I32(fx.conn, 3)});
	REQUIRE(V2ChildCount(arr) == 3);
	REQUIRE(V2ChildText(arr, 1) == "2");
	duckdb_v2_logical_type_handle arr_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(arr, &arr_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(arr_type, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY);
	duckdb_v2_logical_type_destroy(&arr_type);
	duckdb_v2_value_destroy(&arr);

	REQUIRE(V2CompositeErr(array_type, {V2I32(fx.conn, 1), V2I32(fx.conn, 2)}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&array_type);
}

namespace {

duckdb_v2_logical_type_handle MakeValueTestStruct(duckdb_v2_connection_handle conn) {
	return MakeStructType(conn, {"id", "label"},
	                      {DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR});
}

} // namespace

TEST_CASE("V2: value_create STRUCT takes positional fields", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto struct_type = MakeValueTestStruct(fx.conn);

	auto s = V2Composite(struct_type, {V2I32(fx.conn, 42), V2Varchar(fx.conn, "joe")});
	REQUIRE(V2ChildCount(s) == 2);
	auto id_child = V2Child(s, 0);
	REQUIRE(ConsumeValue<int32_t>(id_child) == 42);
	duckdb_v2_value_destroy(&id_child);
	REQUIRE(V2ChildText(s, 1) == "joe");
	duckdb_v2_value_destroy(&s);

	// A NULL field is a typed NULL.
	auto with_null =
	    V2Composite(struct_type, {V2I32(fx.conn, 1), V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	auto null_field = V2Child(with_null, 1);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(null_field, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	duckdb_v2_value_destroy(&null_field);
	duckdb_v2_value_destroy(&with_null);

	// Field count is enforced.
	REQUIRE(V2CompositeErr(struct_type, {V2I32(fx.conn, 1)}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&struct_type);
}

TEST_CASE("V2: value_create TUPLE takes positional fields", "[capi_v2][value][composite]") {
	EnvFixture fx;
	duckdb_v2_logical_type_handle tuple_type = nullptr;
	REQUIRE(duckdb_v2_connection_create_type_from_text(fx.conn, Convert("TUPLE(INTEGER, VARCHAR)"), &tuple_type,
	                                                   nullptr) == DUCKDB_V2_ERROR_NONE);

	auto t = V2Composite(tuple_type, {V2I32(fx.conn, 42), V2Varchar(fx.conn, "joe")});
	REQUIRE(V2ChildCount(t) == 2);
	auto first = V2Child(t, 0);
	REQUIRE(ConsumeValue<int32_t>(first) == 42);
	duckdb_v2_value_destroy(&first);
	REQUIRE(V2ChildText(t, 1) == "joe");
	duckdb_v2_value_destroy(&t);

	// Field count is enforced.
	REQUIRE(V2CompositeErr(tuple_type, {V2I32(fx.conn, 1)}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&tuple_type);
}

TEST_CASE("V2: value_create MAP alternates keys and values", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto map_type = MakeMapType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	auto map =
	    V2Composite(map_type, {V2Varchar(fx.conn, "a"), V2I32(fx.conn, 1), V2Varchar(fx.conn, "b"), V2I32(fx.conn, 2)});
	REQUIRE(V2ChildCount(map) == 4);
	REQUIRE(V2ChildText(map, 0) == "a");
	REQUIRE(V2ChildText(map, 1) == "1");
	REQUIRE(V2ChildText(map, 2) == "b");
	REQUIRE(V2ChildText(map, 3) == "2");
	duckdb_v2_value_destroy(&map);

	// MAP values are cast to the declared value type too (one level down,
	// through the internal STRUCT entry): a BIGINT value into
	// MAP(VARCHAR, INTEGER) descends as INTEGER.
	duckdb_v2_value_handle mixed_key = V2Varchar(fx.conn, "k");
	duckdb_v2_value_handle big = MakeInt64Value(fx.conn, 9);
	const duckdb_v2_value_handle mixed[2] = {mixed_key, big};
	duckdb_v2_value_handle cast_map = nullptr;
	auto cast_rc = duckdb_v2_value_create(map_type, mixed, 2, &cast_map, nullptr);
	duckdb_v2_value_destroy(&mixed_key);
	duckdb_v2_value_destroy(&big);
	REQUIRE(cast_rc == DUCKDB_V2_ERROR_NONE);
	auto descended = V2Child(cast_map, 1);
	duckdb_v2_logical_type_handle descended_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(descended, &descended_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID descended_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(descended_type, &descended_id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(descended_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(ConsumeValue<int32_t>(descended) == 9);
	duckdb_v2_logical_type_destroy(&descended_type);
	duckdb_v2_value_destroy(&descended);
	duckdb_v2_value_destroy(&cast_map);

	// Odd child counts, duplicate keys, and NULL keys are rejected.
	REQUIRE(V2CompositeErr(map_type, {V2Varchar(fx.conn, "a")}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(V2CompositeErr(map_type, {V2Varchar(fx.conn, "a"), V2I32(fx.conn, 1), V2Varchar(fx.conn, "a"),
	                                  V2I32(fx.conn, 2)}) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2CompositeErr(map_type, {V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR), V2I32(fx.conn, 1)}) !=
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&map_type);
}

TEST_CASE("V2: value_create rejects non-composite and UNION types", "[capi_v2][value][composite]") {
	EnvFixture fx;
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	REQUIRE(V2CompositeErr(int_type, {V2I32(fx.conn, 1)}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&int_type);

	std::vector<const char *> names = {"i", "s"};
	auto union_type = MakeType(fx.conn, "union", &names,
	                           {MakeTypeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
	                            MakeTypeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	// UNION values are built via value_cast, not value_create.
	REQUIRE(V2CompositeErr(union_type, {V2I32(fx.conn, 1)}) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&union_type);
}

TEST_CASE("V2: value_create null-arg refusals", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_value_handle out = nullptr;
	REQUIRE(duckdb_v2_value_create(nullptr, nullptr, 0, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create(list_type, nullptr, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	// child_count > 0 with a null children array.
	REQUIRE(duckdb_v2_value_create(list_type, nullptr, 1, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	// A null child handle inside the array.
	const duckdb_v2_value_handle holed[1] = {nullptr};
	REQUIRE(duckdb_v2_value_create(list_type, holed, 1, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	duckdb_v2_logical_type_destroy(&list_type);
}

TEST_CASE("V2: value_get_child_count is 0 for primitives and NULL composites", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto primitive = V2I32(fx.conn, 42);
	REQUIRE(V2ChildCount(primitive) == 0);
	duckdb_v2_value_handle child = nullptr;
	REQUIRE(duckdb_v2_value_get_child(primitive, 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(child == nullptr);
	duckdb_v2_value_destroy(&primitive);

	auto list_type = MakeListType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_value_handle null_list = nullptr;
	REQUIRE(duckdb_v2_value_create_null(list_type, &null_list, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&list_type);
	REQUIRE(V2ChildCount(null_list) == 0);
	REQUIRE(duckdb_v2_value_get_child(null_list, 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&null_list);

	// Null-arg refusals.
	idx_t count = 0;
	REQUIRE(duckdb_v2_value_get_child_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto v = V2I32(fx.conn, 1);
	REQUIRE(duckdb_v2_value_get_child_count(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_child(nullptr, 0, &child, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_child(v, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&v);
}

// ===========================================================================
// value_cast: generic conversion; the UNION / ENUM construction path
// ===========================================================================

namespace {

// Cast helper: casts straight from the connection.
// Returns the owned result.
duckdb_v2_value_handle V2CastValue(duckdb_v2_connection_handle conn, duckdb_v2_value_handle value,
                                   duckdb_v2_logical_type_handle target) {
	duckdb_v2_value_handle out = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(conn, value, target, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out != nullptr);
	return out;
}

} // namespace

TEST_CASE("V2: value_cast converts across types and from text", "[capi_v2][value][cast]") {
	EnvFixture f;

	// Widening numeric cast.
	auto small = V2I32(f.conn, 42);
	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_connection_create_type_from_id(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, 0,
	                                         &bigint_type, nullptr);
	auto widened = V2CastValue(f.conn, small, bigint_type);
	REQUIRE(ConsumeValue<int64_t>(widened) == 42);
	duckdb_v2_value_destroy(&widened);
	duckdb_v2_value_destroy(&small);
	duckdb_v2_logical_type_destroy(&bigint_type);

	// Text to DATE.
	auto date_text = V2Varchar(f.conn, "2024-03-15");
	duckdb_v2_logical_type_handle date_type = nullptr;
	duckdb_v2_connection_create_type_from_id(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_DATE, nullptr, nullptr, 0, &date_type,
	                                         nullptr);
	auto date = V2CastValue(f.conn, date_text, date_type);
	auto rendered = Render(date);
	REQUIRE(rendered == "2024-03-15");
	duckdb_v2_value_destroy(&date);
	duckdb_v2_value_destroy(&date_text);
	duckdb_v2_logical_type_destroy(&date_type);

	// Text to a composite: with a VARCHAR built through the leaf codec this
	// constructs any value from text.
	auto list_text = V2Varchar(f.conn, "[1, 2, 3]");
	auto list_type = MakeListType(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto list = V2CastValue(f.conn, list_text, list_type);
	REQUIRE(V2ChildCount(list) == 3);
	REQUIRE(V2ChildText(list, 2) == "3");
	duckdb_v2_value_destroy(&list);
	duckdb_v2_value_destroy(&list_text);
	duckdb_v2_logical_type_destroy(&list_type);

	// A failing cast surfaces the conversion error and nulls the out param.
	auto bad = V2Varchar(f.conn, "abc");
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	auto out = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(f.conn, bad, int_type, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_logical_type_destroy(&int_type);
	duckdb_v2_value_destroy(&bad);
}

TEST_CASE("V2: UNION values build via value_cast and descend as tag + member", "[capi_v2][value][cast][union]") {
	EnvFixture f;
	std::vector<const char *> names = {"i", "s"};
	auto union_type = MakeType(f.conn, "union", &names,
	                           {MakeTypeValue(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
	                            MakeTypeValue(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});

	// Member to union: the engine selects the matching member.
	auto int_member = V2I32(f.conn, 42);
	auto u = V2CastValue(f.conn, int_member, union_type);
	duckdb_v2_value_destroy(&int_member);
	REQUIRE(V2ChildCount(u) == 2);
	// [0] = the tag as UTINYINT; [1] = the active member. A union value
	// holds only its active member (unlike the vector module's
	// [1..N] = all members convention).
	auto tag = V2Child(u, 0);
	REQUIRE(ConsumeValue<uint8_t>(tag) == 0);
	duckdb_v2_value_destroy(&tag);
	auto member = V2Child(u, 1);
	REQUIRE(ConsumeValue<int32_t>(member) == 42);
	duckdb_v2_value_destroy(&member);
	REQUIRE(duckdb_v2_value_get_child(u, 2, &member, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&u);

	// The other member selects tag 1.
	auto str_member = V2Varchar(f.conn, "x");
	auto u2 = V2CastValue(f.conn, str_member, union_type);
	duckdb_v2_value_destroy(&str_member);
	tag = V2Child(u2, 0);
	REQUIRE(ConsumeValue<uint8_t>(tag) == 1);
	duckdb_v2_value_destroy(&tag);
	REQUIRE(V2ChildText(u2, 1) == "x");
	duckdb_v2_value_destroy(&u2);
	duckdb_v2_logical_type_destroy(&union_type);
}

TEST_CASE("V2: ENUM values build via value_cast from VARCHAR", "[capi_v2][value][cast][enum]") {
	EnvFixture f;
	auto enum_type =
	    MakeType(f.conn, "enum", nullptr,
	             {MakeVarcharValue(f.conn, "sad"), MakeVarcharValue(f.conn, "ok"), MakeVarcharValue(f.conn, "happy")});

	auto text = V2Varchar(f.conn, "happy");
	auto e = V2CastValue(f.conn, text, enum_type);
	duckdb_v2_value_destroy(&text);
	duckdb_v2_logical_type_handle vt = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(e, &vt, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(vt, &id, nullptr);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);
	duckdb_v2_logical_type_destroy(&vt);
	auto rendered = Render(e);
	REQUIRE(rendered == "happy");

	// And back to text through the cast machinery.
	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_connection_create_type_from_id(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, nullptr, nullptr, 0,
	                                         &varchar_type, nullptr);
	auto back = V2CastValue(f.conn, e, varchar_type);
	REQUIRE(ConsumeValue<std::string>(back) == "happy");
	duckdb_v2_value_destroy(&back);
	duckdb_v2_logical_type_destroy(&varchar_type);
	duckdb_v2_value_destroy(&e);

	// A string outside the dictionary fails the cast.
	auto bad = V2Varchar(f.conn, "angry");
	auto out = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(f.conn, bad, enum_type, &out, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_value_destroy(&bad);
	duckdb_v2_logical_type_destroy(&enum_type);
}

TEST_CASE("V2: value_cast null-arg refusals", "[capi_v2][value][cast]") {
	EnvFixture f;
	duckdb_v2_value_handle out = nullptr;
	auto v = V2I32(f.conn, 1);
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(f.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);

	// A null connection is refused without any scope.
	REQUIRE(duckdb_v2_value_cast_with_connection(nullptr, v, int_type, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_value_cast_with_connection(f.conn, nullptr, int_type, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_cast_with_connection(f.conn, v, nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_cast_with_connection(f.conn, v, int_type, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&int_type);
	duckdb_v2_value_destroy(&v);
}
// ===========================================================================
// VARIANT codec: value_get_variant
// ===========================================================================

TEST_CASE("V2: value_get_variant unwraps the boxed cell and gates its edges", "[capi_v2][value][variant]") {
	EnvFixture f;
	QueryResult r;

	REQUIRE(Query(f.conn, "SELECT 42::VARIANT AS v, NULL::VARIANT AS n", &r) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle value_vec = nullptr;
	duckdb_v2_vector_handle null_vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &value_vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &null_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_handle box = nullptr;
	REQUIRE(duckdb_v2_vector_get_value(value_vec, 0, &box, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle inner = nullptr;
	REQUIRE(duckdb_v2_value_get_variant(box, &inner, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle inner_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(inner, &inner_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID inner_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(inner_type, &inner_id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(inner_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(ConsumeValue<int32_t>(inner) == 42);
	duckdb_v2_logical_type_destroy(&inner_type);
	duckdb_v2_value_destroy(&inner);

	// A NULL variant cell has nothing to unwrap.
	duckdb_v2_value_handle null_box = nullptr;
	REQUIRE(duckdb_v2_vector_get_value(null_vec, 0, &null_box, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(null_box, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	auto out = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_get_variant(null_box, &out, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Non-VARIANT values and null args are refused.
	auto plain = MakeInt32Value(f.conn, 1);
	REQUIRE(duckdb_v2_value_get_variant(plain, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(duckdb_v2_value_get_variant(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_variant(box, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&plain);
	duckdb_v2_value_destroy(&null_box);
	duckdb_v2_value_destroy(&box);
	duckdb_v2_data_chunk_destroy(&chunk);
}

} // namespace test_capi_v2
