#include "capi_v2_internal.hpp"

#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"

// Write side of string-backed vectors. vector_get_string_heap is the single
// string-ness check; string_heap_allocate then hands out raw vector-lifetime
// bytes. The caller assembles and places the duckdb_v2_string (see the C++ API).

DUCKDB_V2_ERROR duckdb_v2_vector_get_string_heap(duckdb_v2_vector_handle vector, duckdb_v2_string_heap_handle *out_heap,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_heap) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_string_heap");
		}
		*out_heap = nullptr;
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_string_heap");
		}
		auto *vec = duckdb::ToVector(vector);
		// Physical VARCHAR backs VARCHAR / BLOB / BIT / BIGNUM: the one string-ness check.
		if (vec->GetType().InternalType() != duckdb::PhysicalType::VARCHAR) {
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_string_heap: vector is not a string-backed type");
		}
		auto &heap = duckdb::StringVector::GetStringHeap(*vec);
		*out_heap = reinterpret_cast<_duckdb_v2_string_heap *>(&heap);
	});
}

DUCKDB_V2_ERROR duckdb_v2_string_heap_allocate(duckdb_v2_string_heap_handle heap, idx_t byte_len, uint8_t **out_ptr,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_ptr) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_string_heap_allocate");
		}
		*out_ptr = nullptr;
		if (!heap) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_string_heap_allocate");
		}
		auto *heap_ptr = duckdb::ToStringHeap(heap);
		// Raw arena bytes with the heap's (vector's) lifetime; no string_t semantics, no gating.
		*out_ptr = heap_ptr->GetAllocator().Allocate(byte_len);
	});
}
