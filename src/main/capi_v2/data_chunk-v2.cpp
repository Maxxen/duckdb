#include "capi_v2_internal.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/main/materialized_query_result.hpp"

// ---------------------------------------------------------------------------
// DataChunk construction
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_create(const duckdb_v2_logical_type_handle *types, idx_t column_count,
                                                 duckdb_v2_data_chunk_handle *out_chunk,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_chunk) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create");
		}
		*out_chunk = nullptr;
		if (!types) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create");
		}
		duckdb::vector<duckdb::LogicalType> logical_types;
		logical_types.reserve(column_count);
		for (idx_t i = 0; i < column_count; i++) {
			if (!types[i]) {
				throw duckdb::InvalidInputException("null logical type at index %llu", i);
			}
			logical_types.push_back(*duckdb::ToLogicalType(types[i]));
		}
		auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), logical_types);
		*out_chunk = reinterpret_cast<_duckdb_v2_data_chunk *>(chunk.release());
	});
}

// ---------------------------------------------------------------------------
// Result-side accessors
// ---------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_result_chunk_count(duckdb_v2_result_handle result, idx_t *out_count,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_chunk_count");
		}
		auto *r = duckdb::ToResult(result);
		if (r->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
			*out_count = 0;
			return;
		}
		*out_count = r->Collection().ChunkCount();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_chunk(duckdb_v2_result_handle result, idx_t index,
                                                duckdb_v2_data_chunk_handle *out_chunk,
                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !out_chunk) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_get_chunk");
		}
		*out_chunk = nullptr;
		auto *r = duckdb::ToResult(result);
		if (r->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
			throw duckdb::InvalidInputException("duckdb_v2_result_get_chunk: result is not a QUERY_RESULT");
		}
		auto &collection = r->Collection();
		if (index >= collection.ChunkCount()) {
			throw duckdb::InvalidInputException("chunk index out of range");
		}
		auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
		collection.FetchChunk(index, *chunk);
		*out_chunk = reinterpret_cast<_duckdb_v2_data_chunk *>(chunk.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_destroy(duckdb_v2_data_chunk_handle *chunk) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!chunk) {
			return;
		}
		if (*chunk) {
			delete duckdb::ToDataChunk(*chunk);
			*chunk = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_size(duckdb_v2_data_chunk_handle chunk, idx_t *out_size,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!chunk || !out_size) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_size");
		}
		*out_size = duckdb::ToDataChunk(chunk)->size();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_vector_count(duckdb_v2_data_chunk_handle chunk, idx_t *out_count,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!chunk || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_vector_count");
		}
		*out_count = duckdb::ToDataChunk(chunk)->ColumnCount();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_vector(duckdb_v2_data_chunk_handle chunk, idx_t index,
                                                     duckdb_v2_vector_handle *out_vector,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!chunk || !out_vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_vector");
		}
		*out_vector = nullptr;
		auto *c = duckdb::ToDataChunk(chunk);
		if (index >= c->ColumnCount()) {
			throw duckdb::InvalidInputException("vector index out of range");
		}
		*out_vector = reinterpret_cast<_duckdb_v2_vector *>(&c->data[index]);
	});
}
