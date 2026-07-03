#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// Tests for the transparent duckdb_v2_string layout: read its fields directly
// (via the StrInlined / StrLength / StrData helpers below) and cross-validate
// against the V1 string_t accessors (duckdb_string_is_inlined, _t_length,
// _t_data) and the V2 wire-codec decoders (bit/bignum_decode).
// ---------------------------------------------------------------------------

namespace {

// Direct field reads on the transparent layout. The length field overlaps in
// both union members, so it is read through inlined regardless of form.
bool StrInlined(const duckdb_v2_string *s) {
	return s->value.inlined.length <= DUCKDB_V2_STRING_INLINE_LENGTH;
}
uint32_t StrLength(const duckdb_v2_string *s) {
	return s->value.inlined.length;
}
const char *StrData(const duckdb_v2_string *s) {
	return StrInlined(s) ? s->value.inlined.inlined : s->value.pointer.ptr;
}

// BIT wire format: data[0] is the padding-bit count; data[1..] is the payload.
uint8_t BitPadding(const duckdb_v2_string *s) {
	return StrLength(s) == 0 ? 0 : static_cast<uint8_t>(StrData(s)[0]);
}
uint64_t BitCount(const duckdb_v2_string *s) {
	uint32_t len = StrLength(s);
	return len == 0 ? 0 : static_cast<uint64_t>(len - 1) * 8 - static_cast<uint8_t>(StrData(s)[0]);
}
const uint8_t *BitGetData(const duckdb_v2_string *s) {
	return reinterpret_cast<const uint8_t *>(StrData(s)) + 1;
}

// BIGNUM sign encoding: MSB of data[0] clear = negative, set = positive.
bool BignumNegative(const duckdb_v2_string *s) {
	return StrLength(s) == 0 ? false : (static_cast<uint8_t>(StrData(s)[0]) & 0x80) == 0;
}

struct V2InlineFixture {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	V2InlineFixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~V2InlineFixture() {
		duckdb_v2_disconnect(&conn);
		duckdb_v2_close(&db);
		duckdb_v2_destroy_environment(&env);
	}
};

// Executes a single-column query, asserts row count, and holds chunk 0's view.
struct InlQueryRows {
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_vector_view view {};
	idx_t size = 0;

	InlQueryRows(duckdb_v2_connection_handle conn, const char *sql, idx_t expected_rows) {
		REQUIRE(V2Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		chunk = V2StepChunk(r);
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == expected_rows);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_get_view(vec, &view, nullptr);
	}
	~InlQueryRows() {
		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&r);
	}
	template <typename T>
	const T *as() const {
		return static_cast<const T *>(view.data);
	}
};

} // namespace

// ===========================================================================
// Transparent string reads vs V1 equivalents.
// "short" (5 chars, inlined) and repeat('x', 50) (pointer form).
// ===========================================================================

TEST_CASE("V2 string layout: direct reads match V1 equivalents", "[capi_v2][string_layout]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('short'), (repeat('x', 50))) t(s)", 2);
	const duckdb_v2_string *arr = qr.as<duckdb_v2_string>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = SelAt(qr.view.sel, row);
		const duckdb_v2_string *v2s = &arr[phys];
		// V1 uses the same binary layout; cast is safe.
		duckdb_string_t v1s = *reinterpret_cast<const duckdb_string_t *>(v2s);

		REQUIRE(StrInlined(v2s) == duckdb_string_is_inlined(v1s));
		REQUIRE(StrLength(v2s) == duckdb_string_t_length(v1s));
		REQUIRE(StrData(v2s) ==
		        duckdb_string_t_data(reinterpret_cast<duckdb_string_t *>(const_cast<duckdb_v2_string *>(v2s))));
	}

	// Pin expected values for the two rows.
	{
		REQUIRE(StrInlined(&arr[SelAt(qr.view.sel, 0)]));       // "short" fits inline
		REQUIRE_FALSE(StrInlined(&arr[SelAt(qr.view.sel, 1)])); // 50-char string does not

		REQUIRE(StrLength(&arr[SelAt(qr.view.sel, 0)]) == 5);
		REQUIRE(StrLength(&arr[SelAt(qr.view.sel, 1)]) == 50);

		REQUIRE(std::string(StrData(&arr[SelAt(qr.view.sel, 0)]), 5) == "short");
		REQUIRE(std::string(StrData(&arr[SelAt(qr.view.sel, 1)]), 50) == std::string(50, 'x'));
	}
}

// ===========================================================================
// Transparent BLOB direct reads.
// 4-byte blob (inlined) and 15-byte blob (pointer form).
// ===========================================================================

TEST_CASE("V2 string layout: BLOB direct reads", "[capi_v2][string_layout]") {
	V2InlineFixture fx;
	// 'ABCD' = 4 bytes (inlined, <= 12); 'ABCDEFGHIJKLMNO' = 15 bytes (pointer form).
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('ABCD'::BLOB), ('ABCDEFGHIJKLMNO'::BLOB)) t(b)", 2);
	const duckdb_v2_blob_t *arr = qr.as<duckdb_v2_blob_t>();

	REQUIRE(StrInlined(&arr[SelAt(qr.view.sel, 0)]));       // 4 bytes
	REQUIRE_FALSE(StrInlined(&arr[SelAt(qr.view.sel, 1)])); // 15 bytes
	REQUIRE(StrLength(&arr[SelAt(qr.view.sel, 0)]) == 4);
	REQUIRE(StrLength(&arr[SelAt(qr.view.sel, 1)]) == 15);
	REQUIRE(std::string(StrData(&arr[SelAt(qr.view.sel, 0)]), 4) == "ABCD");
	REQUIRE(std::string(StrData(&arr[SelAt(qr.view.sel, 1)]), 15) == "ABCDEFGHIJKLMNO");
}

// ===========================================================================
// Transparent BIT reads vs bit_decode.
//   '11111111' → 8 bits, padding=0, data[0]=0xFF
//   '101'      → 3 bits, padding=5
// ===========================================================================

TEST_CASE("V2 string layout: BIT reads match bit_decode", "[capi_v2][string_layout]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('11111111'::BIT), ('101'::BIT)) t(b)", 2);
	const duckdb_v2_bit_t *arr = qr.as<duckdb_v2_bit_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = SelAt(qr.view.sel, row);
		const uint8_t *dec_data = nullptr;
		idx_t dec_byte_len = 0;
		uint8_t dec_padding = 0;
		REQUIRE(duckdb_v2_bit_decode(&arr[phys], &dec_data, &dec_byte_len, &dec_padding, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);

		REQUIRE(BitPadding(&arr[phys]) == dec_padding);
		REQUIRE(BitCount(&arr[phys]) == dec_byte_len * 8 - dec_padding);
		REQUIRE(BitGetData(&arr[phys]) == dec_data);
	}

	// Pin expected values for both rows.
	{
		REQUIRE(BitPadding(&arr[SelAt(qr.view.sel, 0)]) == 0);
		REQUIRE(BitCount(&arr[SelAt(qr.view.sel, 0)]) == 8);
		REQUIRE(BitPadding(&arr[SelAt(qr.view.sel, 1)]) == 5);
		REQUIRE(BitCount(&arr[SelAt(qr.view.sel, 1)]) == 3);
	}
}

// ===========================================================================
// Transparent BIGNUM sign vs bignum_decode neg flag.
//   2^128 - 1  → positive
//   -256       → negative
// ===========================================================================

TEST_CASE("V2 string layout: BIGNUM sign matches bignum_decode", "[capi_v2][string_layout]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn,
	                "SELECT * FROM (VALUES "
	                "  (340282366920938463463374607431768211455::BIGNUM), "
	                "  (-256::BIGNUM)) t(b)",
	                2);
	const duckdb_v2_bignum_t *arr = qr.as<duckdb_v2_bignum_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = SelAt(qr.view.sel, row);
		uint8_t *mag = nullptr;
		idx_t mag_len = 0;
		bool dec_neg = false;
		REQUIRE(duckdb_v2_bignum_decode(&arr[phys], &mag, &mag_len, &dec_neg, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(BignumNegative(&arr[phys]) == dec_neg);
		free(mag);
	}

	// Pin expected signs.
	{
		REQUIRE_FALSE(BignumNegative(&arr[SelAt(qr.view.sel, 0)])); // 2^128-1, positive
		REQUIRE(BignumNegative(&arr[SelAt(qr.view.sel, 1)]));       // -256, negative
	}
}
