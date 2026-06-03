#include "capi_v2_internal.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_create(duckdb_v2_context_handle context,
                                                             const duckdb_v2_logical_type_handle *types_array,
                                                             idx_t types_count,
                                                             duckdb_v2_column_data_collection_handle *out_collection,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out_collection) {
			throw duckdb::InvalidInputException("Output collection pointer cannot be null.");
		}
		if (!types_array) {
			throw duckdb::InvalidInputException("Types array pointer cannot be null.");
		}

		std::vector<duckdb::LogicalType> types;
		types.reserve(types_count);
		for (idx_t i = 0; i < types_count; i++) {
			if (!types_array[i]) {
				throw duckdb::InvalidInputException("Null logical type pointer at index %llu", i);
			}
			types.push_back(*duckdb::ToLogicalType(types_array[i]));
		}

		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);
		auto collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(ctx, std::move(types));
		*out_collection = reinterpret_cast<duckdb_v2_column_data_collection_handle>(collection.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_destroy(duckdb_v2_column_data_collection_handle *collection) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (collection && *collection) {
			delete reinterpret_cast<duckdb::ColumnDataCollection *>(*collection);
			*collection = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_combine(duckdb_v2_column_data_collection_handle target,
                                                              duckdb_v2_column_data_collection_handle *source,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!target) {
			throw duckdb::InvalidInputException("Target collection pointer cannot be null.");
		}
		if (!source || !*source) {
			throw duckdb::InvalidInputException("Source collection pointer cannot be null.");
		}

		auto &target_cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(target);
		auto source_cdc = reinterpret_cast<duckdb::ColumnDataCollection *>(*source);
		target_cdc.Combine(*source_cdc);

		// Delete the source collection and set the pointer to null to indicate it has been consumed
		delete source_cdc;
		*source = nullptr;
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Accessors
//----------------------------------------------------------------------------------------------------------------------
DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_row_count(duckdb_v2_column_data_collection_handle collection,
                                                                idx_t *out_row_count,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!out_row_count) {
			throw duckdb::InvalidInputException("Output row count pointer cannot be null.");
		}
		const auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		*out_row_count = cdc.Count();
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Appending
//----------------------------------------------------------------------------------------------------------------------
DUCKDB_V2_API_CALL_t
duckdb_v2_column_data_collection_append_state_create(duckdb_v2_column_data_collection_handle collection,
                                                     duckdb_v2_column_data_collection_append_state_handle *out_state,
                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!out_state) {
			throw duckdb::InvalidInputException("Output state pointer cannot be null.");
		}
		*out_state = nullptr;
		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto state = duckdb::make_uniq<duckdb::ColumnDataAppendState>();
		cdc.InitializeAppend(*state);
		*out_state = reinterpret_cast<duckdb_v2_column_data_collection_append_state_handle>(state.release());
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_column_data_collection_append_state_destroy(duckdb_v2_column_data_collection_append_state_handle *state) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (state && *state) {
			delete reinterpret_cast<duckdb::ColumnDataAppendState *>(*state);
			*state = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_append(duckdb_v2_column_data_collection_handle collection,
                                                             duckdb_v2_column_data_collection_append_state_handle state,
                                                             duckdb_v2_data_chunk_handle chunk,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!state) {
			throw duckdb::InvalidInputException("State pointer cannot be null.");
		}
		if (!chunk) {
			throw duckdb::InvalidInputException("Chunk pointer cannot be null.");
		}
		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto &append_state = *reinterpret_cast<duckdb::ColumnDataAppendState *>(state);

		cdc.Append(append_state, *duckdb::ToDataChunk(chunk));
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Scanning
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t
duckdb_v2_column_data_collection_scan_state_create(duckdb_v2_column_data_collection_handle collection,
                                                   duckdb_v2_column_data_collection_scan_state_handle *out_state,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!out_state) {
			throw duckdb::InvalidInputException("Output state pointer cannot be null.");
		}
		*out_state = nullptr;
		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto state = duckdb::make_uniq<duckdb::ColumnDataScanState>();
		cdc.InitializeScan(*state);
		*out_state = reinterpret_cast<duckdb_v2_column_data_collection_scan_state_handle>(state.release());
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_column_data_collection_scan_state_destroy(duckdb_v2_column_data_collection_scan_state_handle *state) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (state && *state) {
			delete reinterpret_cast<duckdb::ColumnDataScanState *>(*state);
			*state = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_scan(duckdb_v2_column_data_collection_handle collection,
                                                           duckdb_v2_column_data_collection_scan_state_handle state,
                                                           duckdb_v2_data_chunk_handle out_chunk,
                                                           bool *did_produce_chunk, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!state) {
			throw duckdb::InvalidInputException("State pointer cannot be null.");
		}
		if (!out_chunk) {
			throw duckdb::InvalidInputException("Output chunk pointer cannot be null.");
		}
		if (!did_produce_chunk) {
			throw duckdb::InvalidInputException("Output did_produce_chunk pointer cannot be null.");
		}

		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto &scan_state = *reinterpret_cast<duckdb::ColumnDataScanState *>(state);

		auto &chunk = *duckdb::ToDataChunk(out_chunk);
		*did_produce_chunk = cdc.Scan(scan_state, chunk);
	});
}
//----------------------------------------------------------------------------------------------------------------------
// Parallel Scanning
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_shared_scan_state_create(
    duckdb_v2_column_data_collection_handle collection,
    duckdb_v2_column_data_collection_shared_scan_state_handle *out_state, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!out_state) {
			throw duckdb::InvalidInputException("Output state pointer cannot be null.");
		}
		*out_state = nullptr;
		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto state = duckdb::make_uniq<duckdb::ColumnDataParallelScanState>();
		cdc.InitializeScan(*state);
		*out_state = reinterpret_cast<duckdb_v2_column_data_collection_shared_scan_state_handle>(state.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_shared_scan_state_destroy(
    duckdb_v2_column_data_collection_shared_scan_state_handle *state) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (state && *state) {
			delete reinterpret_cast<duckdb::ColumnDataParallelScanState *>(*state);
			*state = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_worker_scan_state_create(
    duckdb_v2_column_data_collection_handle collection,
    duckdb_v2_column_data_collection_worker_scan_state_handle *out_state, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!out_state) {
			throw duckdb::InvalidInputException("Output state pointer cannot be null.");
		}

		// We don't use the collection pointer yet. but keep it as an argument for future-compat
		auto state = duckdb::make_uniq<duckdb::ColumnDataLocalScanState>();
		*out_state = reinterpret_cast<duckdb_v2_column_data_collection_worker_scan_state_handle>(state.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_column_data_collection_worker_scan_state_destroy(
    duckdb_v2_column_data_collection_worker_scan_state_handle *state) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (state && *state) {
			delete reinterpret_cast<duckdb::ColumnDataLocalScanState *>(*state);
			*state = nullptr;
		}
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_column_data_collection_parallel_scan(duckdb_v2_column_data_collection_handle collection,
                                               duckdb_v2_column_data_collection_shared_scan_state_handle shared_state,
                                               duckdb_v2_column_data_collection_worker_scan_state_handle worker_state,
                                               duckdb_v2_data_chunk_handle out_chunk, bool *did_produce_chunk,
                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!collection) {
			throw duckdb::InvalidInputException("Collection pointer cannot be null.");
		}
		if (!shared_state) {
			throw duckdb::InvalidInputException("Shared state pointer cannot be null.");
		}
		if (!worker_state) {
			throw duckdb::InvalidInputException("Worker state pointer cannot be null.");
		}
		if (!out_chunk) {
			throw duckdb::InvalidInputException("Output chunk pointer cannot be null.");
		}
		if (!did_produce_chunk) {
			throw duckdb::InvalidInputException("Output did_produce_chunk pointer cannot be null.");
		}

		auto &cdc = *reinterpret_cast<duckdb::ColumnDataCollection *>(collection);
		auto &shared_scan_state = *reinterpret_cast<duckdb::ColumnDataParallelScanState *>(shared_state);
		auto &local_scan_state = *reinterpret_cast<duckdb::ColumnDataLocalScanState *>(worker_state);
		auto &chunk = *duckdb::ToDataChunk(out_chunk);

		*did_produce_chunk = cdc.Scan(shared_scan_state, local_scan_state, chunk);
	});
}
