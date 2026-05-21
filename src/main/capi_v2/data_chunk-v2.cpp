#include "capi_v2_internal.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

// Exception policy:
//   - result_get_chunk allocates a new DataChunk and runs
//     ColumnDataCollection::FetchChunk through it: both can std::bad_alloc,
//     and Initialize / FetchChunk walk the type tree which may raise
//     InternalException on shape mismatches that core today would treat
//     as bugs. Wrap with the standard try/catch pair.
//   - The remaining accessors read pre-validated state on a DataChunk that
//     was successfully constructed and are unwrapped.

DUCKDB_V2_API_CALL_t duckdb_v2_result_chunk_count(duckdb_v2_result_ptr result, idx_t *out_count,
                                                  duckdb_v2_error_info_ptr *err) {
	if (!result || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_result_chunk_count");
	}
	auto *r = duckdb::ToResult(result);
	if (r->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
		*out_count = 0;
		return duckdb::ClearErrorInfo(err);
	}
	*out_count = r->Collection().ChunkCount();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_result_get_chunk(duckdb_v2_result_ptr result, idx_t index,
                                                duckdb_v2_data_chunk_ptr *out_chunk, duckdb_v2_error_info_ptr *err) {
	if (!result || !out_chunk) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "null argument to duckdb_v2_result_get_chunk");
	}
	*out_chunk = nullptr;
	auto *r = duckdb::ToResult(result);
	if (r->properties.return_type != duckdb::StatementReturnType::QUERY_RESULT) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "duckdb_v2_result_get_chunk: result is not a QUERY_RESULT");
	}
	auto &collection = r->Collection();
	if (index >= collection.ChunkCount()) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "chunk index out of range");
	}
	try {
		auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
		collection.FetchChunk(index, *chunk);
		*out_chunk = static_cast<duckdb_v2_data_chunk_ptr>(chunk.release());
		return duckdb::ClearErrorInfo(err);
	} catch (std::exception &e) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_API_ERROR, "unknown error in duckdb_v2_result_get_chunk");
	}
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_destroy(duckdb_v2_data_chunk_ptr *chunk, duckdb_v2_error_info_ptr *err) {
	if (!chunk) {
		return duckdb::ClearErrorInfo(err);
	}
	if (*chunk) {
		delete duckdb::ToDataChunk(*chunk);
		*chunk = nullptr;
	}
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_size(duckdb_v2_data_chunk_ptr chunk, idx_t *out_size,
                                                   duckdb_v2_error_info_ptr *err) {
	if (!chunk || !out_size) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_data_chunk_get_size");
	}
	*out_size = duckdb::ToDataChunk(chunk)->size();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_vector_count(duckdb_v2_data_chunk_ptr chunk, idx_t *out_count,
                                                           duckdb_v2_error_info_ptr *err) {
	if (!chunk || !out_count) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_data_chunk_get_vector_count");
	}
	*out_count = duckdb::ToDataChunk(chunk)->ColumnCount();
	return duckdb::ClearErrorInfo(err);
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_get_vector(duckdb_v2_data_chunk_ptr chunk, idx_t index,
                                                     duckdb_v2_vector_ptr *out_vector, duckdb_v2_error_info_ptr *err) {
	if (!chunk || !out_vector) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT,
		                            "null argument to duckdb_v2_data_chunk_get_vector");
	}
	*out_vector = nullptr;
	auto *c = duckdb::ToDataChunk(chunk);
	if (index >= c->ColumnCount()) {
		return duckdb::SetErrorInfo(err, DUCKDB_V2_ERROR_INVALID_INPUT, "vector index out of range");
	}
	*out_vector = static_cast<duckdb_v2_vector_ptr>(&c->data[index]);
	return duckdb::ClearErrorInfo(err);
}
