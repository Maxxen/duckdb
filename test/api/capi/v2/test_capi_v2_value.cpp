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
// Borrow contract being verified throughout: the string value_get_varchar and
// value_get_blob hand back stays valid until value_destroy is called. That
// holds for BIGNUM too — what it borrows is the opaque storage, which
// bignum_decode then translates into a magnitude in a buffer the caller owns.
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
// Typed constructors + typed getters
//
// One pair per built-in whose payload is a plain C scalar or byte string.
// The constructors are scoped to a connection (a context in a bind callback);
// the getters need no scope, and read exactly their own type id.
// ===========================================================================

namespace {

// Builds a value through its typed constructor, reads it back through the
// matching typed getter, and optionally pins the rendering. Reads everything
// first, then destroys, then asserts: a failing REQUIRE in between would leak.
template <class T, class MAKE>
void RequireTypedRoundTrip(MAKE make, T payload, const char *expected_text = nullptr) {
	duckdb_v2_value_handle v = nullptr;
	auto create_rc = make(payload, &v);

	T out {};
	auto read_rc = create_rc == DUCKDB_V2_ERROR_NONE ? GetTypedValue(v, &out) : create_rc;
	auto render_rc = DUCKDB_V2_ERROR_NONE;
	std::string rendered;
	if (expected_text && create_rc == DUCKDB_V2_ERROR_NONE) {
		rendered = RenderText(
		    [&](char *buf, idx_t cap, idx_t *len) { return duckdb_v2_value_to_string(v, buf, cap, len, nullptr); },
		    render_rc);
	}
	duckdb_v2_value_destroy(&v);

	REQUIRE(create_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(read_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::memcmp(&out, &payload, sizeof(T)) == 0);
	if (expected_text) {
		REQUIRE(render_rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(rendered == expected_text);
	}
}

} // namespace

TEST_CASE("V2: typed scalars round-trip through their constructor / getter pair", "[capi_v2][value][typed]") {
	EnvFixture fx;
	auto conn = fx.conn;

	RequireTypedRoundTrip<bool>(
	    [&](bool x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_bool_from_connection(conn, x, v, nullptr);
	    },
	    true, "true");
	RequireTypedRoundTrip<int8_t>(
	    [&](int8_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_tinyint_from_connection(conn, x, v, nullptr);
	    },
	    -5, "-5");
	RequireTypedRoundTrip<int16_t>(
	    [&](int16_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_smallint_from_connection(conn, x, v, nullptr);
	    },
	    -1234, "-1234");
	RequireTypedRoundTrip<int32_t>(
	    [&](int32_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_int_from_connection(conn, x, v, nullptr);
	    },
	    -123456, "-123456");
	RequireTypedRoundTrip<int64_t>(
	    [&](int64_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_bigint_from_connection(conn, x, v, nullptr);
	    },
	    -1234567890123ll, "-1234567890123");
	RequireTypedRoundTrip<uint8_t>(
	    [&](uint8_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_utinyint_from_connection(conn, x, v, nullptr);
	    },
	    200, "200");
	RequireTypedRoundTrip<uint16_t>(
	    [&](uint16_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_usmallint_from_connection(conn, x, v, nullptr);
	    },
	    60000, "60000");
	RequireTypedRoundTrip<uint32_t>(
	    [&](uint32_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_uint_from_connection(conn, x, v, nullptr);
	    },
	    4000000000u, "4000000000");
	RequireTypedRoundTrip<uint64_t>(
	    [&](uint64_t x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_ubigint_from_connection(conn, x, v, nullptr);
	    },
	    18000000000000000000ull, "18000000000000000000");
	RequireTypedRoundTrip<float>(
	    [&](float x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_float_from_connection(conn, x, v, nullptr);
	    },
	    1.5f, "1.5");
	RequireTypedRoundTrip<double>(
	    [&](double x, duckdb_v2_value_handle *v) {
		    return duckdb_v2_value_create_double_from_connection(conn, x, v, nullptr);
	    },
	    -2.25, "-2.25");
}

TEST_CASE("V2: HUGEINT and UHUGEINT carry both 128-bit halves", "[capi_v2][value][typed]") {
	EnvFixture fx;
	auto conn = fx.conn;
	auto make_hugeint = [&](duckdb_v2_hugeint_t x, duckdb_v2_value_handle *v) {
		return duckdb_v2_value_create_hugeint_from_connection(conn, x, v, nullptr);
	};
	auto make_uhugeint = [&](duckdb_v2_uhugeint_t x, duckdb_v2_value_handle *v) {
		return duckdb_v2_value_create_uhugeint_from_connection(conn, x, v, nullptr);
	};

	RequireTypedRoundTrip<duckdb_v2_hugeint_t>(make_hugeint, {42, 0}, "42");
	// Negative: all-ones lower with upper -1 is the two's-complement -1.
	RequireTypedRoundTrip<duckdb_v2_hugeint_t>(make_hugeint, {0xFFFFFFFFFFFFFFFFull, -1}, "-1");
	RequireTypedRoundTrip<duckdb_v2_uhugeint_t>(make_uhugeint, {7, 1}, "18446744073709551623");
}

TEST_CASE("V2: VARCHAR round-trips its bytes and borrows them", "[capi_v2][value][typed][borrow]") {
	EnvFixture fx;
	// An embedded NUL: the payload is a byte range, not a C string.
	const char raw[4] = {'a', '\0', 'b', 'c'};
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar_from_connection(fx.conn, duckdb_v2_str {raw, 4}, &v, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// The borrow is stable: two reads hand back the same pointer.
	duckdb_v2_str first = {nullptr, 0};
	duckdb_v2_str second = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_varchar(v, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first.len == 4);
	REQUIRE(std::memcmp(first.ptr, raw, 4) == 0);
	REQUIRE(duckdb_v2_value_get_varchar(v, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(second.ptr == first.ptr);
	duckdb_v2_value_destroy(&v);

	// Empty: a null pointer is valid when the length is 0.
	REQUIRE(duckdb_v2_value_create_varchar_from_connection(fx.conn, duckdb_v2_str {nullptr, 0}, &v, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ConsumeValue<std::string>(v).empty());

	// The engine rejects invalid UTF-8 at construction.
	const char bad_utf8[2] = {'\xC0', '\x00'};
	duckdb_v2_value_handle bad = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar_from_connection(fx.conn, duckdb_v2_str {bad_utf8, 2}, &bad, &err) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(bad == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: BLOB takes any bytes and reads back through get_blob", "[capi_v2][value][typed][borrow]") {
	EnvFixture fx;
	const char blob_bytes[5] = {'\x00', '\xFF', '\x10', '\x00', '\x7F'};
	auto v = MakeBlobValue(fx.conn, blob_bytes, 5);
	REQUIRE(ConsumeBlob(v) == std::string(blob_bytes, 5));

	auto empty = MakeBlobValue(fx.conn, nullptr, 0);
	REQUIRE(ConsumeBlob(empty).empty());

	// VARCHAR is text and has its own getter; the two do not stand in for
	// each other.
	auto text = MakeVarcharValue(fx.conn, "abc");
	duckdb_v2_str out = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_blob(text, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&text);
}

TEST_CASE("V2: typed getters refuse a mismatched type and a NULL value", "[capi_v2][value][typed]") {
	EnvFixture fx;
	// Wrong kind: an INTEGER value is not a BIGINT value. A getter is not a
	// cast, so no widening happens here either.
	auto v = MakeInt32Value(fx.conn, 42);
	int64_t wide = 0;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_get_bigint(v, &wide, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_str str = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_varchar(v, &str, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&v);

	// A NULL has no payload to hand back.
	auto int_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_value_handle null_value = nullptr;
	REQUIRE(duckdb_v2_value_create_null_from_connection(fx.conn, int_type, &null_value, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	int32_t narrow = 7;
	REQUIRE(duckdb_v2_value_get_int(null_value, &narrow, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&null_value);
	duckdb_v2_logical_type_destroy(&int_type);

	// Composites have no scalar payload; descent is value_get_child.
	auto elem_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_value_handle list = nullptr;
	REQUIRE(duckdb_v2_value_create_list_from_connection(fx.conn, elem_type, nullptr, 0, &list, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_get_int(list, &narrow, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&list);
	duckdb_v2_logical_type_destroy(&elem_type);
}

TEST_CASE("V2: typed constructors and getters null-arg refusals", "[capi_v2][value][typed]") {
	EnvFixture fx;
	duckdb_v2_value_handle v = nullptr;

	// The scope handle is mandatory: it is what binds the value to a catalog.
	REQUIRE(duckdb_v2_value_create_int_from_connection(nullptr, 1, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);
	REQUIRE(duckdb_v2_value_create_int_from_context(nullptr, 1, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_int_from_connection(fx.conn, 1, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// A null byte range is only valid empty.
	REQUIRE(duckdb_v2_value_create_varchar_from_connection(fx.conn, duckdb_v2_str {nullptr, 4}, &v, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	int32_t out = 0;
	REQUIRE(duckdb_v2_value_get_int(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto probe = MakeInt32Value(fx.conn, 1);
	REQUIRE(duckdb_v2_value_get_int(probe, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_str str = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_varchar(nullptr, &str, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_get_blob(probe, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&probe);
}

// ===========================================================================
// The kinds outside the typed set: built through value_cast from a VARCHAR,
// which is the engine's own construction path for them.
// ===========================================================================

TEST_CASE("V2: temporal and UUID values are built from text", "[capi_v2][value][cast]") {
	EnvFixture fx;
	struct {
		DUCKDB_V2_LOGICAL_TYPE_ID id;
		const char *text;
	} cases[] = {
	    {DUCKDB_V2_LOGICAL_TYPE_ID_DATE, "2024-03-15"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME, "12:34:56"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS, "12:34:56.123456789"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP, "2024-03-15 12:34:56"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC, "2024-03-15 12:34:56"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS, "2024-03-15 12:34:56.123"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS, "2024-03-15 12:34:56.123456789"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL, "1 month 2 days 00:00:03"},
	    {DUCKDB_V2_LOGICAL_TYPE_ID_UUID, "47183823-2574-4bfd-b411-99ed177d3e43"},
	};
	for (auto &c : cases) {
		auto v = MakeValueFromText(fx.conn, c.id, c.text);
		REQUIRE(Render(v) == c.text);
		duckdb_v2_value_destroy(&v);
	}
}

TEST_CASE("V2: DECIMAL values are built from text and read through a cast", "[capi_v2][value][cast][decimal]") {
	EnvFixture fx;
	auto t = MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, 18), MakeInt32Value(fx.conn, 6)});
	auto v = MakeValueFromText(fx.conn, t, "123456789012.345678");
	REQUIRE(Render(v) == "123456789012.345678");

	// DECIMAL has no getter of its own; cast to one that does.
	auto double_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
	duckdb_v2_value_handle as_double = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(fx.conn, v, double_type, &as_double, nullptr) == DUCKDB_V2_ERROR_NONE);
	const double read_back = ConsumeValue<double>(as_double);
	REQUIRE(read_back > 123456789012.345 - 1e-3);
	REQUIRE(read_back < 123456789012.345 + 1e-3);
	duckdb_v2_logical_type_destroy(&double_type);

	// A value outside the declared width is a cast failure, not a silent
	// truncation.
	auto narrow = MakeType(fx.conn, "decimal", nullptr, {MakeInt32Value(fx.conn, 4), MakeInt32Value(fx.conn, 2)});
	auto text = MakeVarcharValue(fx.conn, "12.34");
	duckdb_v2_value_handle fits = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(fx.conn, text, narrow, &fits, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Render(fits) == "12.34");
	duckdb_v2_value_destroy(&fits);
	duckdb_v2_value_destroy(&text);

	auto too_wide = MakeVarcharValue(fx.conn, "300.00");
	auto out = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(fx.conn, too_wide, narrow, &out, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_value_destroy(&too_wide);

	duckdb_v2_value_destroy(&v);
	duckdb_v2_logical_type_destroy(&narrow);
	duckdb_v2_logical_type_destroy(&t);
}

TEST_CASE("V2: BIT values are built from text and read as storage bytes", "[capi_v2][value][cast][bit]") {
	EnvFixture fx;
	auto v = MakeValueFromText(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIT, "10101");
	REQUIRE(Render(v) == "10101");
	// The wire form is the padding-header byte plus the data bytes: five bits
	// leave three padding bits in one data byte.
	auto storage = ConsumeBlob(v);
	REQUIRE(storage.size() == 2);
	REQUIRE(static_cast<uint8_t>(storage[0]) == 3);
}

TEST_CASE("V2: V1-built leaf values read through the typed getters", "[capi_v2][value][typed][v1_v2_bridge]") {
	// Same identity-cast invariant as before: V1 and V2 value handles are
	// both bare heap duckdb::Value.
	auto v1_int = V1ValueToV2(duckdb_create_int64(-77));
	REQUIRE(ConsumeValue<int64_t>(v1_int) == -77);
	duckdb_v2_value_destroy(&v1_int);

	const uint8_t blob_bytes[3] = {1, 0, 2};
	auto v1_blob = V1ValueToV2(duckdb_create_blob(blob_bytes, 3));
	REQUIRE(ConsumeBlob(v1_blob) == std::string("\x01\x00\x02", 3));
	duckdb_v2_value_destroy(&v1_blob);
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
// BIGNUM — the codec pair translates between a magnitude + sign and the
// opaque storage bytes. Neither direction allocates on the caller's behalf.
// ===========================================================================

namespace {

// Encodes a magnitude + sign into storage bytes.
std::vector<uint8_t> BignumEncode(const uint8_t *magnitude, idx_t length, bool is_negative) {
	idx_t storage_len = 0;
	REQUIRE(duckdb_v2_bignum_encode(magnitude, length, is_negative, nullptr, 0, &storage_len, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	std::vector<uint8_t> storage(storage_len);
	REQUIRE(duckdb_v2_bignum_encode(magnitude, length, is_negative, storage.data(), storage.size(), &storage_len,
	                                nullptr) == DUCKDB_V2_ERROR_NONE);
	return storage;
}

// Decodes storage bytes back into an owned magnitude + sign.
std::vector<uint8_t> BignumDecode(const uint8_t *storage, idx_t storage_len, bool &out_is_negative) {
	idx_t mag_len = 0;
	REQUIRE(duckdb_v2_bignum_decode(storage, storage_len, nullptr, 0, &mag_len, &out_is_negative, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	std::vector<uint8_t> magnitude(mag_len);
	REQUIRE(duckdb_v2_bignum_decode(storage, storage_len, magnitude.data(), magnitude.size(), &mag_len,
	                                &out_is_negative, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(mag_len == magnitude.size());
	return magnitude;
}

// The storage bytes of a BIGNUM value, read through the byte-string getter.
std::vector<uint8_t> BignumMagnitude(duckdb_v2_value_handle v, bool &out_is_negative) {
	duckdb_v2_str storage = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_blob(v, &storage, nullptr) == DUCKDB_V2_ERROR_NONE);
	return BignumDecode(reinterpret_cast<const uint8_t *>(storage.ptr), storage.len, out_is_negative);
}

} // namespace

TEST_CASE("V2: bignum codec round-trips magnitude bytes and the sign flag", "[capi_v2][value][bignum]") {
	struct {
		std::vector<uint8_t> magnitude;
		bool is_negative;
	} cases[] = {
	    // 0x010203 = 66051, both signs.
	    {{0x01, 0x02, 0x03}, false},
	    {{0x01, 0x02, 0x03}, true},
	    // {0x80, 0x00} forces the encoder to bit-invert into {0x7f, 0xff}
	    // internally, while still reporting the original magnitude on read. A
	    // reader naively assuming "stored bytes == magnitude" would see 0x7fff
	    // (32767, positive) instead of -32768.
	    {{0x80, 0x00}, true},
	    // Core's encoding requires at least one data byte; zero is one 0x00.
	    {{0x00}, false},
	};
	for (auto &c : cases) {
		auto storage = BignumEncode(c.magnitude.data(), c.magnitude.size(), c.is_negative);
		bool is_negative = !c.is_negative;
		auto out = BignumDecode(storage.data(), storage.size(), is_negative);
		REQUIRE(is_negative == c.is_negative);
		REQUIRE(out == c.magnitude);
	}
}

TEST_CASE("V2: BIGNUM values are built from text and decode to a magnitude", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	struct {
		const char *text;
		std::vector<uint8_t> magnitude;
		bool is_negative;
	} cases[] = {
	    {"66051", {0x01, 0x02, 0x03}, false},
	    {"-66051", {0x01, 0x02, 0x03}, true},
	    {"-32768", {0x80, 0x00}, true},
	    {"0", {0x00}, false},
	};
	for (auto &c : cases) {
		auto v = MakeValueFromText(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM, c.text);
		REQUIRE(Render(v) == c.text);
		bool is_negative = !c.is_negative;
		auto magnitude = BignumMagnitude(v, is_negative);
		duckdb_v2_value_destroy(&v);
		REQUIRE(is_negative == c.is_negative);
		REQUIRE(magnitude == c.magnitude);
	}
}

TEST_CASE("V2: value_get_blob refuses a NULL BIGNUM", "[capi_v2][value][bignum]") {
	EnvFixture fx;
	// BIGNUM reads back as storage bytes, but a NULL has no payload to hand back.
	auto bn_lt = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM);
	duckdb_v2_value_handle nv = nullptr;
	REQUIRE(duckdb_v2_value_create_null_from_connection(fx.conn, bn_lt, &nv, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_str out = {nullptr, 0};
	REQUIRE(duckdb_v2_value_get_blob(nv, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&nv);
	duckdb_v2_logical_type_destroy(&bn_lt);
}

// ===========================================================================
// Error info propagation: a sample failure path attaches an info handle.
// ===========================================================================

TEST_CASE("V2: failure path populates error info", "[capi_v2][value][error]") {
	EnvFixture fx;
	duckdb_v2_value_handle v = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar_from_connection(fx.conn, duckdb_v2_str {nullptr, 4}, &v, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);
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
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
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
	                  {MakeTypeValue(fx.conn, list_type), MakeTypeValue(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	duckdb_v2_logical_type_destroy(&list_type);

	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, t, &v, nullptr) == DUCKDB_V2_ERROR_NONE);

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
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, t, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
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
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, int_type, &type_value, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
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
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, nullptr, &v, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(v == nullptr);

	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_connection_create_type_from_id(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, nullptr, nullptr, 0, &int_type,
	                                         nullptr);
	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, int_type, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	// The scope handle is mandatory too.
	REQUIRE(duckdb_v2_value_create_type_from_connection(nullptr, int_type, &v, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_type_from_context(nullptr, int_type, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_logical_type_handle out = nullptr;
	REQUIRE(duckdb_v2_value_get_type(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_value_create_type_from_connection(fx.conn, int_type, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_get_type(v, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&v);
	duckdb_v2_logical_type_destroy(&int_type);
}

// ===========================================================================
// Composite construction + descent
//
// One typed constructor per kind, scoped to a connection (a context inside a
// bind callback). The child type is resolved from the children the way a list
// literal resolves it, so a set of mixed-but-compatible types widens rather
// than failing; an explicit type overrides that, and is what the empty forms
// take.
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

// Runs a composite constructor over borrowed children, destroying them after.
// Reads the result first, then destroys, then asserts, so a failing REQUIRE
// cannot leak.
template <class CALL>
duckdb_v2_value_handle V2Build(std::vector<duckdb_v2_value_handle> children, CALL call) {
	duckdb_v2_value_handle v = nullptr;
	auto rc = call(children.empty() ? nullptr : children.data(), children.size(), &v, nullptr);
	for (auto &c : children) {
		duckdb_v2_value_destroy(&c);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	return v;
}

// Same, expecting failure: checks out nulling + err population.
template <class CALL>
DUCKDB_V2_ERROR V2BuildErr(std::vector<duckdb_v2_value_handle> children, CALL call) {
	auto v = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = call(children.empty() ? nullptr : children.data(), children.size(), &v, &err);
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

// The per-kind call shapes, with the element / entry type left to inference.
auto ListCall(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle child_type = nullptr) {
	return [conn, child_type](const duckdb_v2_value_handle *children, idx_t count, duckdb_v2_value_handle *out,
	                          duckdb_v2_error_info_handle *err) {
		return duckdb_v2_value_create_list_from_connection(conn, child_type, children, count, out, err);
	};
}

auto ArrayCall(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle child_type = nullptr) {
	return [conn, child_type](const duckdb_v2_value_handle *children, idx_t count, duckdb_v2_value_handle *out,
	                          duckdb_v2_error_info_handle *err) {
		return duckdb_v2_value_create_array_from_connection(conn, child_type, children, count, out, err);
	};
}

auto TupleCall(duckdb_v2_connection_handle conn) {
	return [conn](const duckdb_v2_value_handle *children, idx_t count, duckdb_v2_value_handle *out,
	              duckdb_v2_error_info_handle *err) {
		return duckdb_v2_value_create_tuple_from_connection(conn, children, count, out, err);
	};
}

// STRUCT and MAP take parallel arrays, so they get their own shapes.
duckdb_v2_value_handle V2Struct(duckdb_v2_connection_handle conn, const std::vector<const char *> &names,
                                std::vector<duckdb_v2_value_handle> children) {
	std::vector<duckdb_v2_identifier_t> name_views;
	for (auto *n : names) {
		name_views.push_back(Convert(n));
	}
	duckdb_v2_value_handle v = nullptr;
	auto rc = duckdb_v2_value_create_struct_from_connection(conn, name_views.empty() ? nullptr : name_views.data(),
	                                                        children.empty() ? nullptr : children.data(),
	                                                        children.size(), &v, nullptr);
	for (auto &c : children) {
		duckdb_v2_value_destroy(&c);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	return v;
}

duckdb_v2_value_handle V2Map(duckdb_v2_connection_handle conn, std::vector<duckdb_v2_value_handle> keys,
                             std::vector<duckdb_v2_value_handle> values) {
	duckdb_v2_value_handle v = nullptr;
	auto rc =
	    duckdb_v2_value_create_map_from_connection(conn, nullptr, nullptr, keys.empty() ? nullptr : keys.data(),
	                                               values.empty() ? nullptr : values.data(), keys.size(), &v, nullptr);
	for (auto &k : keys) {
		duckdb_v2_value_destroy(&k);
	}
	for (auto &val : values) {
		duckdb_v2_value_destroy(&val);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(v != nullptr);
	return v;
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

// The value's own type, rendered.
std::string V2TypeOf(duckdb_v2_value_handle v) {
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(v, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto text = Render(t);
	duckdb_v2_logical_type_destroy(&t);
	return text;
}

} // namespace

TEST_CASE("V2: LIST round-trips elements and NULLs", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto list = V2Build({V2I32(fx.conn, 1), V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER), V2I32(fx.conn, 3)},
	                    ListCall(fx.conn));
	REQUIRE(V2ChildCount(list) == 3);
	REQUIRE(V2TypeOf(list) == "INTEGER[]");
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
}

TEST_CASE("V2: LIST resolves the element type across the elements", "[capi_v2][value][composite]") {
	EnvFixture fx;
	// Mixed widths widen to the common type rather than truncating to the
	// first element's: the rule a list literal follows.
	auto widened = V2Build({V2I32(fx.conn, 7), MakeInt64Value(fx.conn, 1LL << 40)}, ListCall(fx.conn));
	REQUIRE(V2TypeOf(widened) == "BIGINT[]");
	REQUIRE(V2ChildText(widened, 1) == "1099511627776");
	duckdb_v2_value_destroy(&widened);

	// An explicit element type overrides the resolution, and the elements are
	// cast to it.
	auto bigint_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	auto declared = V2Build({V2I32(fx.conn, 7)}, ListCall(fx.conn, bigint_type));
	REQUIRE(V2TypeOf(declared) == "BIGINT[]");
	auto child = V2Child(declared, 0);
	REQUIRE(ConsumeValue<int64_t>(child) == 7);
	duckdb_v2_value_destroy(&declared);

	// The empty list takes its element type the same way.
	auto empty = V2Build({}, ListCall(fx.conn, bigint_type));
	REQUIRE(V2ChildCount(empty) == 0);
	REQUIRE(V2TypeOf(empty) == "BIGINT[]");
	duckdb_v2_value_handle child_out = nullptr;
	REQUIRE(duckdb_v2_value_get_child(empty, 0, &child_out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&empty);
	duckdb_v2_logical_type_destroy(&bigint_type);

	// Without one there is nothing to resolve.
	REQUIRE(V2BuildErr({}, ListCall(fx.conn)) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Elements with no common type surface the engine's error.
	REQUIRE(V2BuildErr({V2I32(fx.conn, 1), V2Varchar(fx.conn, "abc")}, ListCall(fx.conn)) != DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: ARRAY is sized by its element count", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto arr = V2Build({V2I32(fx.conn, 1), V2I32(fx.conn, 2), V2I32(fx.conn, 3)}, ArrayCall(fx.conn));
	REQUIRE(V2ChildCount(arr) == 3);
	REQUIRE(V2TypeOf(arr) == "INTEGER[3]");
	REQUIRE(V2ChildText(arr, 1) == "2");
	duckdb_v2_value_destroy(&arr);

	// The engine's minimum array size is 1, so there is no empty ARRAY, with
	// or without a declared element type.
	REQUIRE(V2BuildErr({}, ArrayCall(fx.conn)) == DUCKDB_V2_ERROR_INPUT_INVALID);
	auto int_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(V2BuildErr({}, ArrayCall(fx.conn, int_type)) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_destroy(&int_type);
}

TEST_CASE("V2: STRUCT takes parallel names and fields", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto s = V2Struct(fx.conn, {"id", "label"}, {V2I32(fx.conn, 42), V2Varchar(fx.conn, "joe")});
	REQUIRE(V2ChildCount(s) == 2);
	REQUIRE(V2TypeOf(s) == "STRUCT(id INTEGER, \"label\" VARCHAR)");
	auto id_child = V2Child(s, 0);
	REQUIRE(ConsumeValue<int32_t>(id_child) == 42);
	REQUIRE(V2ChildText(s, 1) == "joe");
	duckdb_v2_value_destroy(&s);

	// Each field keeps its own type, so a NULL field is a typed NULL.
	auto with_null =
	    V2Struct(fx.conn, {"id", "label"}, {V2I32(fx.conn, 1), V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)});
	auto null_field = V2Child(with_null, 1);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(null_field, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	duckdb_v2_value_destroy(&null_field);
	duckdb_v2_value_destroy(&with_null);

	// The empty struct is a real type.
	auto empty = V2Struct(fx.conn, {}, {});
	REQUIRE(V2ChildCount(empty) == 0);
	REQUIRE(Render(empty) == "{}");
	duckdb_v2_value_destroy(&empty);
}

TEST_CASE("V2: TUPLE takes positional fields", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto t = V2Build({V2I32(fx.conn, 42), V2Varchar(fx.conn, "joe")}, TupleCall(fx.conn));
	REQUIRE(V2ChildCount(t) == 2);
	REQUIRE(V2TypeOf(t) == "TUPLE(INTEGER, VARCHAR)");
	auto first = V2Child(t, 0);
	REQUIRE(ConsumeValue<int32_t>(first) == 42);
	REQUIRE(V2ChildText(t, 1) == "joe");
	duckdb_v2_value_destroy(&t);

	// The empty tuple is a real type too.
	auto empty = V2Build({}, TupleCall(fx.conn));
	REQUIRE(V2ChildCount(empty) == 0);
	REQUIRE(Render(empty) == "()");
	duckdb_v2_value_destroy(&empty);
}

TEST_CASE("V2: MAP takes parallel keys and values", "[capi_v2][value][composite]") {
	EnvFixture fx;
	auto map =
	    V2Map(fx.conn, {V2Varchar(fx.conn, "a"), V2Varchar(fx.conn, "b")}, {V2I32(fx.conn, 1), V2I32(fx.conn, 2)});
	// Children alternate key, value on the way back out.
	REQUIRE(V2ChildCount(map) == 4);
	REQUIRE(V2TypeOf(map) == "MAP(VARCHAR, INTEGER)");
	REQUIRE(V2ChildText(map, 0) == "a");
	REQUIRE(V2ChildText(map, 1) == "1");
	REQUIRE(V2ChildText(map, 2) == "b");
	REQUIRE(V2ChildText(map, 3) == "2");
	duckdb_v2_value_destroy(&map);

	// Key and value types resolve across their own arrays independently.
	auto widened = V2Map(fx.conn, {V2Varchar(fx.conn, "k")}, {MakeInt64Value(fx.conn, 9)});
	REQUIRE(V2TypeOf(widened) == "MAP(VARCHAR, BIGINT)");
	duckdb_v2_value_destroy(&widened);

	// The empty map takes its key and value types explicitly.
	auto key_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	auto value_type = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_value_handle empty = nullptr;
	REQUIRE(duckdb_v2_value_create_map_from_connection(fx.conn, key_type, value_type, nullptr, nullptr, 0, &empty,
	                                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2ChildCount(empty) == 0);
	REQUIRE(V2TypeOf(empty) == "MAP(VARCHAR, INTEGER)");
	duckdb_v2_value_destroy(&empty);

	// Without them there is nothing to resolve.
	auto out = reinterpret_cast<duckdb_v2_value_handle>(0x1);
	REQUIRE(duckdb_v2_value_create_map_from_connection(fx.conn, nullptr, nullptr, nullptr, nullptr, 0, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	duckdb_v2_logical_type_destroy(&key_type);
	duckdb_v2_logical_type_destroy(&value_type);

	// Duplicate and NULL keys are rejected by the engine.
	duckdb_v2_value_handle dup_keys[2] = {V2Varchar(fx.conn, "a"), V2Varchar(fx.conn, "a")};
	duckdb_v2_value_handle dup_values[2] = {V2I32(fx.conn, 1), V2I32(fx.conn, 2)};
	REQUIRE(duckdb_v2_value_create_map_from_connection(fx.conn, nullptr, nullptr, dup_keys, dup_values, 2, &out,
	                                                   nullptr) != DUCKDB_V2_ERROR_NONE);
	for (auto &k : dup_keys) {
		duckdb_v2_value_destroy(&k);
	}
	for (auto &v : dup_values) {
		duckdb_v2_value_destroy(&v);
	}

	duckdb_v2_value_handle null_key[1] = {V2NullOf(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)};
	duckdb_v2_value_handle one_value[1] = {V2I32(fx.conn, 1)};
	REQUIRE(duckdb_v2_value_create_map_from_connection(fx.conn, nullptr, nullptr, null_key, one_value, 1, &out,
	                                                   nullptr) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&null_key[0]);
	duckdb_v2_value_destroy(&one_value[0]);
}

TEST_CASE("V2: composite constructors null-arg refusals", "[capi_v2][value][composite]") {
	EnvFixture fx;
	duckdb_v2_value_handle out = nullptr;

	// The scope handle and the out slot are mandatory everywhere.
	REQUIRE(duckdb_v2_value_create_list_from_connection(nullptr, nullptr, nullptr, 0, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_list_from_context(nullptr, nullptr, nullptr, 0, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_value_create_tuple_from_connection(fx.conn, nullptr, 0, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	// A count with no array, and a null child inside one.
	REQUIRE(duckdb_v2_value_create_tuple_from_connection(fx.conn, nullptr, 1, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	const duckdb_v2_value_handle holed[1] = {nullptr};
	REQUIRE(duckdb_v2_value_create_tuple_from_connection(fx.conn, holed, 1, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);

	// STRUCT needs its names array whenever it has fields.
	auto one = V2I32(fx.conn, 1);
	const duckdb_v2_value_handle fields[1] = {one};
	REQUIRE(duckdb_v2_value_create_struct_from_connection(fx.conn, nullptr, fields, 1, &out, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&one);
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
