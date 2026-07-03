#include "capi_v2_test_helpers.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 string-heap write surface: vector_get_string_heap + string_heap_allocate.
// Borrow the heap, allocate vector-lifetime bytes, assemble a duckdb_v2_string
// over the transparent layout, and place it via the mutable data array. Type
// fixtures come from V1 helpers cast to V2; intermediates die before any REQUIRE.
// ---------------------------------------------------------------------------

namespace {

// Build a single-column chunk of the given V1 type and borrow its vector.
struct StringChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_vector_handle vec = nullptr;

	explicit StringChunk(duckdb_type type) {
		auto v1 = duckdb_create_logical_type(type);
		duckdb_v2_logical_type_handle types[1] = {V1ToV2(v1)};
		auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
		duckdb_destroy_logical_type(&v1);
		REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	~StringChunk() {
		duckdb_v2_data_chunk_destroy(&chunk);
	}
};

// Inline-ness is a direct field read on the transparent layout.
bool IsInlined(const duckdb_v2_string &s) {
	return s.value.inlined.length <= DUCKDB_V2_STRING_INLINE_LENGTH;
}

} // namespace

// ---------------------------------------------------------------------------
// vector_get_string_heap
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_get_string_heap on string-backed kinds", "[capi_v2][string_heap]") {
	for (auto type : {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_BIT, DUCKDB_TYPE_BIGNUM}) {
		StringChunk fixture(type);
		duckdb_v2_string_heap_handle heap = nullptr;
		REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(heap != nullptr);

		// The heap is stable across calls on the same vector.
		duckdb_v2_string_heap_handle heap2 = nullptr;
		REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap2, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(heap2 == heap);
	}
}

TEST_CASE("V2: vector_get_string_heap rejects non-string vector", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_INTEGER);
	duckdb_v2_string_heap_handle heap = reinterpret_cast<duckdb_v2_string_heap_handle>(0x1);
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	// out_heap is nulled on the INVALID_INPUT path.
	REQUIRE(heap == nullptr);
}

TEST_CASE("V2: vector_get_string_heap null args", "[capi_v2][string_heap]") {
	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(nullptr, &heap, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

// ---------------------------------------------------------------------------
// string_heap_allocate: the raw primitive
// ---------------------------------------------------------------------------

TEST_CASE("V2: string_heap_allocate write-in-place", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	REQUIRE(duckdb_v2_vector_set_size(fixture.vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Generate bytes in place (no intermediate buffer), then assemble and place.
	const idx_t len = 100;
	uint8_t *bytes = nullptr;
	REQUIRE(duckdb_v2_string_heap_allocate(heap, len, &bytes, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(bytes != nullptr);
	std::memset(bytes, 'x', len);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(fixture.vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *slots = static_cast<duckdb_v2_string *>(raw);

	slots[0] = V2StringFromHeapBytes(bytes, len);
	REQUIRE_FALSE(IsInlined(slots[0]));

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(fixture.vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);
	duckdb_v2_str out = {nullptr, 0};
	REQUIRE(duckdb_v2_varchar_decode(&arr[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == std::string(len, 'x'));
}

TEST_CASE("V2: string_heap_allocate byte_len 0 is valid", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	// No size gating: 0 bytes is valid and succeeds.
	uint8_t *bytes = nullptr;
	REQUIRE(duckdb_v2_string_heap_allocate(heap, 0, &bytes, nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: string_heap_allocate null args", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Null heap: out_ptr is nulled on the INVALID_INPUT path.
	uint8_t *bytes = reinterpret_cast<uint8_t *>(0x1);
	REQUIRE(duckdb_v2_string_heap_allocate(nullptr, 4, &bytes, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(bytes == nullptr);
	// Null out_ptr.
	REQUIRE(duckdb_v2_string_heap_allocate(heap, 4, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

// ---------------------------------------------------------------------------
// Assemble + place via V2MakeString (the canonical write path)
// ---------------------------------------------------------------------------

TEST_CASE("V2: inline vs non-inline placement", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	REQUIRE(duckdb_v2_vector_set_size(fixture.vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(fixture.vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *slots = static_cast<duckdb_v2_string *>(raw);

	DUCKDB_V2_API_CALL_t rc = DUCKDB_V2_ERROR_NONE;
	// Inlined (<= 12 bytes): self-contained, no allocation.
	slots[0] = V2MakeString(heap, "hi", 2, rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(IsInlined(slots[0]));

	// Non-inlined (> 12 bytes): copied into the heap.
	std::string long_str(100, 'x');
	slots[1] = V2MakeString(heap, long_str.data(), long_str.size(), rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(IsInlined(slots[1]));

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(fixture.vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);
	duckdb_v2_str out = {nullptr, 0};
	REQUIRE(duckdb_v2_varchar_decode(&arr[0], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == "hi");
	REQUIRE(duckdb_v2_varchar_decode(&arr[1], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == long_str);
}

TEST_CASE("V2: empty string is inlined", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_API_CALL_t rc = DUCKDB_V2_ERROR_NONE;
	// {NULL, 0} and {"", 0} both yield an inlined empty string.
	duckdb_v2_string a = V2MakeString(heap, nullptr, 0, rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_string b = V2MakeString(heap, "", 0, rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	for (auto *s : {&a, &b}) {
		REQUIRE(IsInlined(*s));
		duckdb_v2_str out = {reinterpret_cast<const char *>(0x1), 99};
		REQUIRE(duckdb_v2_varchar_decode(s, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(out.len == 0);
	}
}

TEST_CASE("V2: BLOB with embedded nulls (inline + heap)", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_BLOB);
	REQUIRE(duckdb_v2_vector_set_size(fixture.vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(fixture.vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *slots = static_cast<duckdb_v2_string *>(raw);

	DUCKDB_V2_API_CALL_t rc = DUCKDB_V2_ERROR_NONE;
	// 5-byte blob: inlined, embedded null preserved.
	const char small[] = "\xDE\xAD\x00\xBE\xEF";
	slots[0] = V2MakeString(heap, small, 5, rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(IsInlined(slots[0]));

	// 20-byte blob with embedded nulls: heap path.
	std::string big(20, '\0');
	big[0] = static_cast<char>(0xDE);
	big[10] = static_cast<char>(0xAD);
	big[19] = static_cast<char>(0xEF);
	slots[1] = V2MakeString(heap, big.data(), big.size(), rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(IsInlined(slots[1]));

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(fixture.vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_blob_t *>(view.data);

	const uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_blob_decode(&arr[0], &out_data, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 5);
	REQUIRE(out_data[0] == 0xDE);
	REQUIRE(out_data[2] == 0x00);
	REQUIRE(out_data[4] == 0xEF);

	REQUIRE(duckdb_v2_blob_decode(&arr[1], &out_data, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 20);
	REQUIRE(out_data[0] == 0xDE);
	REQUIRE(out_data[5] == 0x00);
	REQUIRE(out_data[10] == 0xAD);
	REQUIRE(out_data[19] == 0xEF);
}

// ---------------------------------------------------------------------------
// Constant vector: the heap surface works the same; placement targets slot 0
// ---------------------------------------------------------------------------

TEST_CASE("V2: string heap write on constant vector", "[capi_v2][string_heap]") {
	StringChunk fixture(DUCKDB_TYPE_VARCHAR);

	duckdb_v2_value_handle value = V2VarcharValue("init");
	REQUIRE(duckdb_v2_vector_make_constant(fixture.vec, value, 3, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_string_heap_handle heap = nullptr;
	REQUIRE(duckdb_v2_vector_get_string_heap(fixture.vec, &heap, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(fixture.vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *slots = static_cast<duckdb_v2_string *>(raw);

	// A heap-backed value (> 12 bytes) so the constant path exercises allocate.
	const std::string constant = "a constant value longer than twelve bytes";
	DUCKDB_V2_API_CALL_t rc = DUCKDB_V2_ERROR_NONE;
	slots[0] = V2MakeString(heap, constant.data(), constant.size(), rc, nullptr);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(IsInlined(slots[0]));

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(fixture.vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);
	duckdb_v2_str out = {nullptr, 0};
	REQUIRE(duckdb_v2_varchar_decode(&arr[SelAt(view.sel, 0)], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == constant);
	REQUIRE(duckdb_v2_varchar_decode(&arr[SelAt(view.sel, 2)], &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == constant);
}
