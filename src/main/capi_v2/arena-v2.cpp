#include "capi_v2_internal.hpp"

#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"

// Bump allocation of bytes owned by another object. vector_get_arena is the
// single string-ness check; arena_allocate then hands out raw vector-lifetime
// bytes. The caller assembles and places the duckdb_v2_bytes (see the C++ API).

DUCKDB_V2_ERROR duckdb_v2_vector_get_arena(duckdb_v2_vector_handle vector, duckdb_v2_arena_handle *out_arena,
                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_arena) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_arena");
		}
		*out_arena = nullptr;
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_arena");
		}
		auto *vec = duckdb::ToVector(vector);
		// Physical VARCHAR backs VARCHAR / BLOB / BIT / BIGNUM: the one string-ness check.
		if (vec->GetType().InternalType() != duckdb::PhysicalType::VARCHAR) {
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_arena: vector is not a string-backed type");
		}
		auto &heap = duckdb::StringVector::GetStringHeap(*vec);
		*out_arena = reinterpret_cast<_duckdb_v2_arena *>(&heap);
	});
}

DUCKDB_V2_ERROR duckdb_v2_arena_allocate(duckdb_v2_arena_handle arena, idx_t byte_len, uint8_t **out_ptr,
                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_ptr) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arena_allocate");
		}
		*out_ptr = nullptr;
		if (!arena) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arena_allocate");
		}
		auto *heap = duckdb::ToArena(arena);
		// Raw arena bytes with the owner's (vector's) lifetime; no string_t semantics, no gating.
		*out_ptr = heap->GetAllocator().Allocate(byte_len);
	});
}
