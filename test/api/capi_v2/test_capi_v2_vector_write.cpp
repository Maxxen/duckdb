#include "capi_v2_test_helpers.hpp"
#include "capi_v2_internal.hpp"

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 vector write API tests.
//
// Type fixtures are built via V1 helpers and cast to V2 via the handle
// identity invariant. Intermediates are destroyed before any REQUIRE to
// avoid leaks on Catch2 assertion failure.
//
// There is no chunk-level size setter: each vector carries its own logical
// size, set via vector_set_size (the child of a LIST/MAP is sized the same
// way, through its borrowed handle). data_chunk_get_size remains for the
// read path (chunks fetched from a result), where the engine sets the
// cardinality; a manually-built write chunk reports 0 there.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// data_chunk_create
// ---------------------------------------------------------------------------

TEST_CASE("V2: data_chunk_create basic", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_logical_type_ptr types[2] = {V1ToV2(int_type), V1ToV2(varchar_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&varchar_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);

	idx_t vec_count = 0;
	REQUIRE(duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vec_count == 2);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vec != nullptr);

	// A fresh vector starts empty; sizing is per-vector.
	idx_t size = 99;
	REQUIRE(duckdb_v2_vector_get_size(vec, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 0);

	REQUIRE(duckdb_v2_vector_set_size(vec, 10, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_get_size(vec, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 10);

	DUCKDB_V2_VECTOR_TYPE vtype = DUCKDB_V2_VECTOR_TYPE_OTHER;
	REQUIRE(duckdb_v2_vector_get_vector_type(vec, &vtype, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vtype == DUCKDB_V2_VECTOR_TYPE_FLAT);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk == nullptr);
}

TEST_CASE("V2: data_chunk_create null args", "[capi_v2][vector_write]") {
	duckdb_v2_data_chunk_ptr chunk = nullptr;

	// Null types array — out_chunk should be zeroed.
	REQUIRE(duckdb_v2_data_chunk_create(nullptr, 2, &chunk, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(chunk == nullptr);

	// Null out_chunk.
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};
	auto rc = duckdb_v2_data_chunk_create(types, 1, nullptr, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: data_chunk_create with null element in types", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[2] = {V1ToV2(int_type), nullptr};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(chunk == nullptr);
}

// ---------------------------------------------------------------------------
// vector_set_size / vector_get_size
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_set_size null arg", "[capi_v2][vector_write]") {
	REQUIRE(duckdb_v2_vector_set_size(nullptr, 10, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: per-column sizing", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto double_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	duckdb_v2_logical_type_ptr types[2] = {V1ToV2(int_type), V1ToV2(double_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&double_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	for (idx_t col = 0; col < 2; col++) {
		duckdb_v2_vector_ptr vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, col, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_set_size(vec, 42, nullptr) == DUCKDB_V2_ERROR_NONE);
		idx_t size = 0;
		REQUIRE(duckdb_v2_vector_get_size(vec, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(size == 42);
	}

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// vector_set_size beyond the default capacity must auto-reserve.
TEST_CASE("V2: vector_set_size auto-reserves", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_set_size(vec, 5000, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t size = 0;
	REQUIRE(duckdb_v2_vector_get_size(vec, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 5000);

	// Writing up to the new size must not crash.
	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<int32_t *>(raw)[4999] = 42;

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// vector_flatten / vector_make_constant / vector_make_sequence
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_make_constant from value", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr value = nullptr;
	REQUIRE(duckdb_v2_value_create_int32(42, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_constant(vec, value, 5, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_VECTOR_TYPE vtype = DUCKDB_V2_VECTOR_TYPE_OTHER;
	REQUIRE(duckdb_v2_vector_get_vector_type(vec, &vtype, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vtype == DUCKDB_V2_VECTOR_TYPE_CONSTANT);

	// make_constant fills the vector from the value across all logical rows.
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 5);
	REQUIRE(view.sel != nullptr);
	const auto *rdata = static_cast<const int32_t *>(view.data);
	REQUIRE(rdata[SelAt(view.sel, 0)] == 42);
	REQUIRE(rdata[SelAt(view.sel, 4)] == 42);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_flatten resets constant", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr value = nullptr;
	REQUIRE(duckdb_v2_value_create_int32(7, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_constant(vec, value, 3, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_flatten(vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_VECTOR_TYPE vtype = DUCKDB_V2_VECTOR_TYPE_OTHER;
	REQUIRE(duckdb_v2_vector_get_vector_type(vec, &vtype, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vtype == DUCKDB_V2_VECTOR_TYPE_FLAT);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.sel == nullptr);
	// Flattening preserves the logical contents.
	const auto *rdata = static_cast<const int32_t *>(view.data);
	REQUIRE(rdata[0] == 7);
	REQUIRE(rdata[2] == 7);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_make_sequence", "[capi_v2][vector_write]") {
	auto bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(bigint_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&bigint_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_make_sequence(vec, 10, 3, 4, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_flatten(vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 4);

	const auto *data = static_cast<const int64_t *>(view.data);
	REQUIRE(data[0] == 10);
	REQUIRE(data[1] == 13);
	REQUIRE(data[2] == 16);
	REQUIRE(data[3] == 19);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_make_* null args", "[capi_v2][vector_write]") {
	REQUIRE(duckdb_v2_vector_flatten(nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_vector_make_constant(nullptr, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_vector_make_sequence(nullptr, 0, 1, 10, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: vector_make_constant null value", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	// A non-null vector with a null value must be rejected.
	REQUIRE(duckdb_v2_vector_make_constant(vec, nullptr, 5, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// vector_flat_get_validity_mutable
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_flat_get_validity_mutable + set nulls", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 4, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *data = static_cast<int32_t *>(raw);
	data[0] = 10;
	data[1] = 20;
	data[2] = 30;
	data[3] = 40;

	uint64_t *validity = nullptr;
	REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(validity != nullptr);

	validity[0] &= ~(UINT64_C(1) << 1);
	validity[0] &= ~(UINT64_C(1) << 3);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.validity != nullptr);

	bool is_valid = false;
	REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, 0, &is_valid, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_valid);
	REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, 1, &is_valid, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(is_valid);
	REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, 2, &is_valid, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_valid);
	REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, 3, &is_valid, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(is_valid);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_flat_get_validity_mutable null args", "[capi_v2][vector_write]") {
	uint64_t *validity = nullptr;
	REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(nullptr, &validity, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: vector_flat_get_validity_mutable rejects SEQUENCE vector", "[capi_v2][vector_write]") {
	auto i64_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(i64_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&i64_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_sequence(vec, 0, 1, 10, nullptr) == DUCKDB_V2_ERROR_NONE);

	uint64_t *validity = nullptr;
	REQUIRE(duckdb_v2_vector_flat_get_validity_mutable(vec, &validity, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// vector_constant_set_valid
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_constant_set_valid toggles validity", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr value = nullptr;
	REQUIRE(duckdb_v2_value_create_int32(77, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_constant(vec, value, 3, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	// Mark the single constant element NULL — every logical row reads NULL.
	REQUIRE(duckdb_v2_vector_constant_set_valid(vec, false, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.sel != nullptr);
	for (idx_t i = 0; i < 3; i++) {
		bool is_valid = true;
		REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, SelAt(view.sel, i), &is_valid, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(is_valid);
	}

	// Flip it back to valid.
	REQUIRE(duckdb_v2_vector_constant_set_valid(vec, true, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool is_valid = false;
	REQUIRE(duckdb_v2_validity_row_is_valid(view.validity, SelAt(view.sel, 0), &is_valid, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_valid);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_constant_set_valid rejects FLAT vector", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	// FLAT (the default) is not a constant vector.
	REQUIRE(duckdb_v2_vector_constant_set_valid(vec, false, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_vector_constant_set_valid(nullptr, false, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// vector_assign_string
// ---------------------------------------------------------------------------

TEST_CASE("V2: vector_assign_string short + long", "[capi_v2][vector_write]") {
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(varchar_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&varchar_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, "hi", 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	std::string long_str(100, 'x');
	REQUIRE(duckdb_v2_vector_assign_string(vec, 1, long_str.c_str(), long_str.size(), nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);

	const char *out_data = nullptr;
	idx_t out_len = 0;

	REQUIRE(duckdb_v2_varchar_decode(&arr[0], &out_data, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 2);
	REQUIRE(std::string(out_data, out_len) == "hi");

	REQUIRE(duckdb_v2_varchar_decode(&arr[1], &out_data, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 100);
	REQUIRE(std::string(out_data, out_len) == long_str);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_assign_string null args", "[capi_v2][vector_write]") {
	REQUIRE(duckdb_v2_vector_assign_string(nullptr, 0, "x", 1, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2: vector_assign_string on non-string vector rejects", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Should fail — INTEGER vector has no string heap.
	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, "x", 1, nullptr) != DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_assign_string constant index must be 0", "[capi_v2][vector_write]") {
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(varchar_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&varchar_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr value = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar("init", 4, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_constant(vec, value, 1, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, "ok", 2, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(vec, 1, "bad", 3, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_assign_string with BLOB", "[capi_v2][vector_write]") {
	auto blob_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(blob_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&blob_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char blob_data[] = "\xDE\xAD\x00\xBE\xEF";
	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, blob_data, 5, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_blob_t *>(view.data);

	const uint8_t *out_data = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_blob_decode(&arr[0], &out_data, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 5);
	REQUIRE(out_data[0] == 0xDE);
	REQUIRE(out_data[2] == 0x00);
	REQUIRE(out_data[4] == 0xEF);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_assign_string empty string", "[capi_v2][vector_write]") {
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(varchar_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&varchar_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, "", 0, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);

	const char *out_str = nullptr;
	idx_t out_len = 99;
	REQUIRE(duckdb_v2_varchar_decode(&arr[0], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 0);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector_assign_string on constant vector", "[capi_v2][vector_write]") {
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(varchar_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&varchar_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_value_ptr value = nullptr;
	REQUIRE(duckdb_v2_value_create_varchar("init", 4, &value, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_make_constant(vec, value, 3, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_value_destroy(&value) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_assign_string(vec, 0, "constant", 8, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto *arr = static_cast<const duckdb_v2_varchar_t *>(view.data);
	const char *out_str = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_varchar_decode(&arr[SelAt(view.sel, 0)], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(out_str, out_len) == "constant");
	REQUIRE(duckdb_v2_varchar_decode(&arr[SelAt(view.sel, 2)], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(out_str, out_len) == "constant");

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// LIST child management via vector_set_size / vector_get_size on the child
// ---------------------------------------------------------------------------

TEST_CASE("V2: list vector write round-trip", "[capi_v2][vector_write]") {
	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	duckdb_destroy_logical_type(&int_v1);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(list_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&list_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 3, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr child = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Size the child (auto-reserves), then populate it.
	REQUIRE(duckdb_v2_vector_set_size(child, 6, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t child_size = 0;
	REQUIRE(duckdb_v2_vector_get_size(child, &child_size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(child_size == 6);

	void *child_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(child, &child_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *child_data = static_cast<int32_t *>(child_raw);
	child_data[0] = 10;
	child_data[1] = 20;
	child_data[2] = 30;
	child_data[3] = 40;
	child_data[4] = 50;
	child_data[5] = 60;

	void *parent_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &parent_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *entries = static_cast<duckdb_v2_list_entry *>(parent_raw);
	entries[0] = {0, 2};
	entries[1] = {2, 1};
	entries[2] = {3, 3};

	duckdb_v2_vector_view parent_view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &parent_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *read_entries = static_cast<const duckdb_v2_list_entry *>(parent_view.data);
	REQUIRE(read_entries[0].offset == 0);
	REQUIRE(read_entries[0].length == 2);
	REQUIRE(read_entries[2].offset == 3);
	REQUIRE(read_entries[2].length == 3);

	duckdb_v2_vector_view child_view {};
	REQUIRE(duckdb_v2_vector_get_view(child, &child_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *read_child = static_cast<const int32_t *>(child_view.data);
	REQUIRE(read_child[0] == 10);
	REQUIRE(read_child[5] == 60);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: list child set_size auto-reserves", "[capi_v2][vector_write]") {
	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	duckdb_destroy_logical_type(&int_v1);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(list_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&list_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr child = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Set child size to 5000 without reserving first — should auto-reserve.
	REQUIRE(duckdb_v2_vector_set_size(child, 5000, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t child_size = 0;
	REQUIRE(duckdb_v2_vector_get_size(child, &child_size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(child_size == 5000);

	// Writing to the child should not crash.
	void *child_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(child, &child_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<int32_t *>(child_raw)[4999] = 42;

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// Struct vector write
// ---------------------------------------------------------------------------

TEST_CASE("V2: struct vector write via children", "[capi_v2][vector_write]") {
	duckdb_logical_type members[2];
	members[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	members[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	const char *names[2] = {"a", "b"};
	auto struct_v1 = duckdb_create_struct_type(members, names, 2);
	duckdb_destroy_logical_type(&members[0]);
	duckdb_destroy_logical_type(&members[1]);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(struct_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&struct_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Sizing a STRUCT vector propagates the size to its field vectors.
	REQUIRE(duckdb_v2_vector_set_size(vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr field_a = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &field_a, nullptr) == DUCKDB_V2_ERROR_NONE);
	void *a_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(field_a, &a_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<int32_t *>(a_raw)[0] = 100;
	static_cast<int32_t *>(a_raw)[1] = 200;

	duckdb_v2_vector_ptr field_b = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 1, &field_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(field_b, 0, "hello", 5, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(field_b, 1, "world", 5, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view_a {};
	REQUIRE(duckdb_v2_vector_get_view(field_a, &view_a, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(static_cast<const int32_t *>(view_a.data)[0] == 100);
	REQUIRE(static_cast<const int32_t *>(view_a.data)[1] == 200);

	duckdb_v2_vector_view view_b {};
	REQUIRE(duckdb_v2_vector_get_view(field_b, &view_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *b_data = static_cast<const duckdb_v2_varchar_t *>(view_b.data);
	const char *out_str = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_varchar_decode(&b_data[0], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(out_str, out_len) == "hello");

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// End-to-end: FLAT integer vector write + read round-trip
// ---------------------------------------------------------------------------

TEST_CASE("V2: flat integer write + read round-trip", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *data = static_cast<int32_t *>(raw);
	for (int32_t i = 0; i < 100; i++) {
		data[i] = i * 7;
	}
	REQUIRE(duckdb_v2_vector_set_size(vec, 100, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 100);
	REQUIRE(view.sel == nullptr);
	auto *rdata = static_cast<const int32_t *>(view.data);
	for (idx_t i = 0; i < 100; i++) {
		REQUIRE(rdata[i] == static_cast<int32_t>(i * 7));
	}

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// Chunk survives after type handles are destroyed
// ---------------------------------------------------------------------------

TEST_CASE("V2: chunk outlives type handles", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<int32_t *>(raw)[0] = 999;

	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(static_cast<const int32_t *>(view.data)[0] == 999);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

TEST_CASE("V2: data_chunk_create zero columns", "[capi_v2][vector_write]") {
	duckdb_v2_data_chunk_ptr chunk = nullptr;
	duckdb_v2_logical_type_ptr empty_types[1] = {nullptr};

	REQUIRE(duckdb_v2_data_chunk_create(empty_types, 0, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);

	idx_t vec_count = 99;
	REQUIRE(duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vec_count == 0);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: vector with zero rows", "[capi_v2][vector_write]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(int_type)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&int_type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_set_size(vec, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t size = 99;
	REQUIRE(duckdb_v2_vector_get_size(vec, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 0);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// Incremental list append (unknown total size)
// ---------------------------------------------------------------------------

TEST_CASE("V2: incremental list append", "[capi_v2][vector_write]") {
	auto int_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto list_v1 = duckdb_create_list_type(int_v1);
	duckdb_destroy_logical_type(&int_v1);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(list_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&list_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr child = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *parent_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &parent_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *entries = static_cast<duckdb_v2_list_entry *>(parent_raw);

	// Row 0: [1, 2, 3] — grow the child (auto-reserves), then write.
	idx_t offset = 99;
	REQUIRE(duckdb_v2_vector_get_size(child, &offset, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(offset == 0);
	REQUIRE(duckdb_v2_vector_set_size(child, 3, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *child_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(child, &child_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *child_data = static_cast<int32_t *>(child_raw);
	child_data[0] = 1;
	child_data[1] = 2;
	child_data[2] = 3;
	entries[0] = {0, 3};

	// Row 1: [4, 5]
	REQUIRE(duckdb_v2_vector_get_size(child, &offset, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(offset == 3);
	REQUIRE(duckdb_v2_vector_set_size(child, 5, nullptr) == DUCKDB_V2_ERROR_NONE);
	child_data[3] = 4;
	child_data[4] = 5;
	entries[1] = {3, 2};

	duckdb_v2_vector_view child_view {};
	REQUIRE(duckdb_v2_vector_get_view(child, &child_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *read = static_cast<const int32_t *>(child_view.data);
	REQUIRE(read[0] == 1);
	REQUIRE(read[4] == 5);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// LIST<VARCHAR>
// ---------------------------------------------------------------------------

TEST_CASE("V2: LIST<VARCHAR> write", "[capi_v2][vector_write]") {
	auto varchar_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto list_v1 = duckdb_create_list_type(varchar_v1);
	duckdb_destroy_logical_type(&varchar_v1);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(list_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&list_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr child = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_set_size(child, 2, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(child, 0, "alpha", 5, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string long_str(200, 'z');
	REQUIRE(duckdb_v2_vector_assign_string(child, 1, long_str.c_str(), long_str.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	void *parent_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &parent_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<duckdb_v2_list_entry *>(parent_raw)[0] = {0, 2};

	duckdb_v2_vector_view child_view {};
	REQUIRE(duckdb_v2_vector_get_view(child, &child_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto *arr = static_cast<const duckdb_v2_varchar_t *>(child_view.data);

	const char *out_str = nullptr;
	idx_t out_len = 0;
	REQUIRE(duckdb_v2_varchar_decode(&arr[0], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(out_str, out_len) == "alpha");
	REQUIRE(duckdb_v2_varchar_decode(&arr[1], &out_str, &out_len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_len == 200);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// MAP write
// ---------------------------------------------------------------------------

TEST_CASE("V2: MAP write via child vectors", "[capi_v2][vector_write]") {
	auto key_v1 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	auto val_v1 = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto map_v1 = duckdb_create_map_type(key_v1, val_v1);
	duckdb_destroy_logical_type(&key_v1);
	duckdb_destroy_logical_type(&val_v1);

	duckdb_v2_logical_type_ptr types[1] = {V1ToV2(map_v1)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 1, &chunk, nullptr);
	duckdb_destroy_logical_type(&map_v1);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_ptr keys = nullptr;
	duckdb_v2_vector_ptr values = nullptr;
	REQUIRE(duckdb_v2_vector_get_child(vec, 0, &keys, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_get_child(vec, 1, &values, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_vector_set_size(keys, 2, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_set_size(values, 2, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *keys_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(keys, &keys_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<int32_t *>(keys_raw)[0] = 1;
	static_cast<int32_t *>(keys_raw)[1] = 2;

	REQUIRE(duckdb_v2_vector_assign_string(values, 0, "one", 3, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_vector_assign_string(values, 1, "two", 3, nullptr) == DUCKDB_V2_ERROR_NONE);

	void *parent_raw = nullptr;
	REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &parent_raw, nullptr) == DUCKDB_V2_ERROR_NONE);
	static_cast<duckdb_v2_list_entry *>(parent_raw)[0] = {0, 2};

	duckdb_v2_vector_view key_view {};
	REQUIRE(duckdb_v2_vector_get_view(keys, &key_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(static_cast<const int32_t *>(key_view.data)[0] == 1);
	REQUIRE(static_cast<const int32_t *>(key_view.data)[1] == 2);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// Multiple primitive types
// ---------------------------------------------------------------------------

TEST_CASE("V2: write multiple primitive types", "[capi_v2][vector_write]") {
	auto bool_t = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
	auto i8_t = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
	auto i16_t = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
	auto i64_t = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	auto f32_t = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
	auto f64_t = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	duckdb_v2_logical_type_ptr types[6] = {V1ToV2(bool_t), V1ToV2(i8_t),  V1ToV2(i16_t),
	                                       V1ToV2(i64_t),  V1ToV2(f32_t), V1ToV2(f64_t)};

	duckdb_v2_data_chunk_ptr chunk = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 6, &chunk, nullptr);
	duckdb_destroy_logical_type(&bool_t);
	duckdb_destroy_logical_type(&i8_t);
	duckdb_destroy_logical_type(&i16_t);
	duckdb_destroy_logical_type(&i64_t);
	duckdb_destroy_logical_type(&f32_t);
	duckdb_destroy_logical_type(&f64_t);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);

	auto write_and_check = [&](idx_t col, auto write_val) {
		using T = decltype(write_val);
		duckdb_v2_vector_ptr vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, col, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_set_size(vec, 1, nullptr) == DUCKDB_V2_ERROR_NONE);
		void *raw = nullptr;
		REQUIRE(duckdb_v2_vector_get_data_mutable(vec, &raw, nullptr) == DUCKDB_V2_ERROR_NONE);
		static_cast<T *>(raw)[0] = write_val;
		duckdb_v2_vector_view view {};
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(static_cast<const T *>(view.data)[0] == write_val);
	};

	write_and_check(0, true);
	write_and_check(1, static_cast<int8_t>(-42));
	write_and_check(2, static_cast<int16_t>(1000));
	write_and_check(3, static_cast<int64_t>(123456789012LL));
	write_and_check(4, 3.14f);
	write_and_check(5, 2.718281828);

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
}
