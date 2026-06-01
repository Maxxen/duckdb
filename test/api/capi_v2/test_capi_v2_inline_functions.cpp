#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// Tests for the duckdb_v2_*_is_inlined / _get_length / _get_data inline helpers.
//
// duckdb_v2_string_* helpers are cross-validated against the V1 equivalents
// (duckdb_string_is_inlined, duckdb_string_t_length, duckdb_string_t_data) — V1 names unchanged.
// Kind-specific helpers are cross-validated against the matching V2 bridge
// decoders (varchar_decode, blob_decode, bit_decode, bignum_decode).
// ---------------------------------------------------------------------------

namespace {

struct V2InlineFixture {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	V2InlineFixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);
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
		REQUIRE(duckdb_v2_connection_query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_result_get_chunk(r, 0, &chunk, nullptr);
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

idx_t InlSelAt(const duckdb_v2_sel_t *sel, idx_t i) {
	idx_t out = 0;
	REQUIRE(duckdb_v2_sel_at(sel, i, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	return out;
}

} // namespace

// ===========================================================================
// duckdb_v2_string_* helpers vs V1 equivalents.
// "short" (5 chars, inlined) and repeat('x', 50) (pointer form).
// ===========================================================================

TEST_CASE("V2 inline: duckdb_v2_string_* matches V1 equivalents", "[capi_v2][inline_functions]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('short'), (repeat('x', 50))) t(s)", 2);
	const duckdb_v2_string *arr = qr.as<duckdb_v2_string>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = InlSelAt(qr.view.sel, row);
		const duckdb_v2_string *v2s = &arr[phys];
		// V1 uses the same binary layout; cast is safe.
		duckdb_string_t v1s = *reinterpret_cast<const duckdb_string_t *>(v2s);

		bool v2_inlined = false;
		uint32_t v2_len = 0;
		const char *v2_data = nullptr;
		REQUIRE(duckdb_v2_string_is_inlined(v2s, &v2_inlined, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_string_get_length(v2s, &v2_len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_string_get_data(v2s, &v2_data, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(v2_inlined == duckdb_string_is_inlined(v1s));
		REQUIRE(v2_len == duckdb_string_t_length(v1s));
		REQUIRE(v2_data ==
		        duckdb_string_t_data(reinterpret_cast<duckdb_string_t *>(const_cast<duckdb_v2_string *>(v2s))));
	}

	// Pin expected values for the two rows.
	{
		bool inlined = false;
		uint32_t len = 0;
		const char *data = nullptr;
		REQUIRE(duckdb_v2_string_is_inlined(&arr[InlSelAt(qr.view.sel, 0)], &inlined, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(inlined); // "short" fits inline
		REQUIRE(duckdb_v2_string_is_inlined(&arr[InlSelAt(qr.view.sel, 1)], &inlined, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(inlined); // 50-char string does not

		REQUIRE(duckdb_v2_string_get_length(&arr[InlSelAt(qr.view.sel, 0)], &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 5);
		REQUIRE(duckdb_v2_string_get_length(&arr[InlSelAt(qr.view.sel, 1)], &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 50);

		REQUIRE(duckdb_v2_string_get_data(&arr[InlSelAt(qr.view.sel, 0)], &data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(data, 5) == "short");
		REQUIRE(duckdb_v2_string_get_data(&arr[InlSelAt(qr.view.sel, 1)], &data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(data, 50) == std::string(50, 'x'));
	}
}

// ===========================================================================
// duckdb_v2_varchar_* helpers vs varchar_decode.
// Same two rows; confirms varchar_* aliases produce identical results.
// ===========================================================================

TEST_CASE("V2 inline: duckdb_v2_varchar_* matches varchar_decode", "[capi_v2][inline_functions]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('short'), (repeat('x', 50))) t(s)", 2);
	const duckdb_v2_varchar_t *arr = qr.as<duckdb_v2_varchar_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = InlSelAt(qr.view.sel, row);
		const char *dec_data = nullptr;
		idx_t dec_len = 0;
		REQUIRE(duckdb_v2_varchar_decode(&arr[phys], &dec_data, &dec_len, nullptr) == DUCKDB_V2_ERROR_NONE);

		uint32_t inl_len = 0;
		const char *inl_data = nullptr;
		bool inl_inlined = false;
		REQUIRE(duckdb_v2_varchar_get_length(&arr[phys], &inl_len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_varchar_get_data(&arr[phys], &inl_data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_varchar_is_inlined(&arr[phys], &inl_inlined, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(inl_len == static_cast<uint32_t>(dec_len));
		REQUIRE(inl_data == dec_data);
		REQUIRE(inl_inlined == (dec_len <= 12));
	}
}

// ===========================================================================
// duckdb_v2_blob_* helpers vs blob_decode.
// 4-byte blob (inlined) and 15-byte blob (pointer form).
// ===========================================================================

TEST_CASE("V2 inline: duckdb_v2_blob_* matches blob_decode", "[capi_v2][inline_functions]") {
	V2InlineFixture fx;
	// 'ABCD' = 4 bytes (inlined, <= 12); 'ABCDEFGHIJKLMNO' = 15 bytes (pointer form).
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('ABCD'::BLOB), ('ABCDEFGHIJKLMNO'::BLOB)) t(b)", 2);
	const duckdb_v2_blob_t *arr = qr.as<duckdb_v2_blob_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = InlSelAt(qr.view.sel, row);
		const uint8_t *dec_data = nullptr;
		idx_t dec_len = 0;
		REQUIRE(duckdb_v2_blob_decode(&arr[phys], &dec_data, &dec_len, nullptr) == DUCKDB_V2_ERROR_NONE);

		uint32_t inl_len = 0;
		const char *inl_data = nullptr;
		bool inl_inlined = false;
		REQUIRE(duckdb_v2_blob_get_length(&arr[phys], &inl_len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_blob_get_data(&arr[phys], &inl_data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_blob_is_inlined(&arr[phys], &inl_inlined, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(inl_len == static_cast<uint32_t>(dec_len));
		REQUIRE(inl_data == reinterpret_cast<const char *>(dec_data));
		REQUIRE(inl_inlined == (dec_len <= 12));
	}

	// Pin expected values.
	{
		bool inlined = false;
		uint32_t len = 0;
		REQUIRE(duckdb_v2_blob_is_inlined(&arr[InlSelAt(qr.view.sel, 0)], &inlined, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(inlined); // 4 bytes
		REQUIRE(duckdb_v2_blob_is_inlined(&arr[InlSelAt(qr.view.sel, 1)], &inlined, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(inlined); // 15 bytes
		REQUIRE(duckdb_v2_blob_get_length(&arr[InlSelAt(qr.view.sel, 0)], &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 4);
		REQUIRE(duckdb_v2_blob_get_length(&arr[InlSelAt(qr.view.sel, 1)], &len, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(len == 15);
	}
}

// ===========================================================================
// duckdb_v2_bit_* helpers vs bit_decode.
//   '11111111' → 8 bits, padding=0, data[0]=0xFF
//   '101'      → 3 bits, padding=5
// ===========================================================================

TEST_CASE("V2 inline: duckdb_v2_bit_* matches bit_decode", "[capi_v2][inline_functions]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn, "SELECT * FROM (VALUES ('11111111'::BIT), ('101'::BIT)) t(b)", 2);
	const duckdb_v2_bit_t *arr = qr.as<duckdb_v2_bit_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = InlSelAt(qr.view.sel, row);
		const uint8_t *dec_data = nullptr;
		idx_t dec_byte_len = 0;
		uint8_t dec_padding = 0;
		REQUIRE(duckdb_v2_bit_decode(&arr[phys], &dec_data, &dec_byte_len, &dec_padding, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);

		uint8_t inl_padding = 0;
		uint64_t inl_count = 0;
		const uint8_t *inl_data = nullptr;
		REQUIRE(duckdb_v2_bit_padding(&arr[phys], &inl_padding, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_bit_count(&arr[phys], &inl_count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_bit_get_data(&arr[phys], &inl_data, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(inl_padding == dec_padding);
		REQUIRE(inl_count == dec_byte_len * 8 - dec_padding);
		REQUIRE(inl_data == dec_data);
	}

	// Pin expected values for both rows.
	{
		uint8_t padding = 0;
		uint64_t count = 0;
		REQUIRE(duckdb_v2_bit_padding(&arr[InlSelAt(qr.view.sel, 0)], &padding, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(padding == 0);
		REQUIRE(duckdb_v2_bit_count(&arr[InlSelAt(qr.view.sel, 0)], &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == 8);
		REQUIRE(duckdb_v2_bit_padding(&arr[InlSelAt(qr.view.sel, 1)], &padding, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(padding == 5);
		REQUIRE(duckdb_v2_bit_count(&arr[InlSelAt(qr.view.sel, 1)], &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == 3);
	}
}

// ===========================================================================
// duckdb_v2_bignum_is_negative vs bignum_decode neg flag.
//   2^128 - 1  → positive
//   -256       → negative
// ===========================================================================

TEST_CASE("V2 inline: duckdb_v2_bignum_is_negative matches bignum_decode", "[capi_v2][inline_functions]") {
	V2InlineFixture fx;
	InlQueryRows qr(fx.conn,
	                "SELECT * FROM (VALUES "
	                "  (340282366920938463463374607431768211455::BIGNUM), "
	                "  (-256::BIGNUM)) t(b)",
	                2);
	const duckdb_v2_bignum_t *arr = qr.as<duckdb_v2_bignum_t>();

	for (idx_t row = 0; row < qr.size; row++) {
		idx_t phys = InlSelAt(qr.view.sel, row);
		uint8_t *mag = nullptr;
		idx_t mag_len = 0;
		bool dec_neg = false;
		REQUIRE(duckdb_v2_bignum_decode(&arr[phys], &mag, &mag_len, &dec_neg, nullptr) == DUCKDB_V2_ERROR_NONE);

		bool inl_neg = false;
		REQUIRE(duckdb_v2_bignum_is_negative(&arr[phys], &inl_neg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(inl_neg == dec_neg);
		free(mag);
	}

	// Pin expected signs.
	{
		bool neg = false;
		REQUIRE(duckdb_v2_bignum_is_negative(&arr[InlSelAt(qr.view.sel, 0)], &neg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(neg); // 2^128-1, positive
		REQUIRE(duckdb_v2_bignum_is_negative(&arr[InlSelAt(qr.view.sel, 1)], &neg, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(neg); // -256, negative
	}
}
