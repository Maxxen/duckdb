#include "capi_v2_internal.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

#include <cerrno>

namespace duckdb {
namespace {

// batch_size == 0 selects this default. Matches pyarrow's dataset scanner
// default (2^17), and equals 64 * STANDARD_VECTOR_SIZE in a default build, so
// it coalesces a clean 64 engine chunks per Arrow array.
constexpr idx_t kDefaultArrowBatchSize = 131072;

ClientContext &ToContextRef(duckdb_v2_context_handle context) {
	return *reinterpret_cast<ClientContext *>(context);
}

ArrowTableSchema &ToArrowTableSchema(duckdb_v2_arrow_conversion_plan_handle plan) {
	return *reinterpret_cast<ArrowTableSchema *>(plan);
}

// ---------------------------------------------------------------------------
// result_to_arrow_stream
// ---------------------------------------------------------------------------

// A ChunkScanState that pulls chunks from a V2 ResultWrapperV2 via its blocking
// fetch, so the engine's ArrowUtil::TryFetchChunk (offset tracking + appender
// coalescing) can drive a V2 streaming result. End-of-stream is signalled the
// same way QueryResultChunkScanState signals it: a null/empty current chunk.
class ResultWrapperChunkScanState : public ChunkScanState {
public:
	explicit ResultWrapperChunkScanState(ResultWrapperV2 &wrapper) : wrapper(wrapper) {
	}

	bool LoadNextChunk(ErrorData &error) override {
		if (finished) {
			current_chunk = nullptr;
			return true;
		}
		try {
			current_chunk = wrapper.FetchChunkBlocking();
		} catch (std::exception &ex) {
			scan_error = ErrorData(ex);
			has_scan_error = true;
			finished = true;
			current_chunk = nullptr;
			error = scan_error;
			return false;
		}
		offset = 0;
		if (!current_chunk) {
			finished = true;
		}
		return true;
	}
	bool HasError() const override {
		return has_scan_error;
	}
	ErrorData &GetError() override {
		return scan_error;
	}
	const vector<LogicalType> &Types() const override {
		return wrapper.types;
	}
	const vector<string> &Names() const override {
		return wrapper.names;
	}

private:
	ResultWrapperV2 &wrapper;
	ErrorData scan_error;
	bool has_scan_error = false;
};

// Backing object for the ArrowArrayStream's private_data. Owns the V2 result
// state machine (which holds the transaction and the connection busy slot),
// the scan state that drives it, and the schema cached at creation under the
// live transaction (the schema-cache-under-txn invariant). get_schema returns
// the cached schema and never touches the catalog.
struct ResultArrowStream {
	unique_ptr<ResultWrapperV2> wrapper;
	unique_ptr<ChunkScanState> scan_state;
	ArrowSchema cached_schema {};
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
	ClientProperties client_properties;
	idx_t batch_size = kDefaultArrowBatchSize;
	ErrorData last_error;

	~ResultArrowStream() {
		if (cached_schema.release) {
			cached_schema.release(&cached_schema);
		}
		// scan_state holds a reference into *wrapper; drop it first.
		scan_state.reset();
	}
};

int ArrowStreamGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	if (!stream->release || !stream->private_data) {
		return EINVAL;
	}
	auto &self = *static_cast<ResultArrowStream *>(stream->private_data);
	// Guard the whole body: an exception (e.g. OOM building an ErrorData) must not
	// cross the C ABI.
	try {
		if (!self.cached_schema.release) {
			self.last_error = ErrorData("arrow stream: schema is unavailable");
			return EINVAL;
		}
		// Hand out an independently-owned deep copy: per the Arrow C Data Interface
		// contract the consumer owns the schema returned by get_schema and releases
		// it independently of the stream. The cached schema was built once under the
		// producing transaction (the schema-cache invariant); deep-copying it is
		// pure and never re-touches the catalog.
		if (duckdb_nanoarrow::ArrowSchemaDeepCopy(&self.cached_schema, out) != NANOARROW_OK) {
			self.last_error = ErrorData("arrow stream: failed to copy schema");
			return ENOMEM;
		}
		return 0;
	} catch (std::exception &ex) {
		// last_error update is itself best-effort under OOM; the return code is
		// authoritative.
		try {
			self.last_error = ErrorData(ex);
		} catch (...) { // NOLINT
		}
		return EIO;
	} catch (...) {
		return EIO;
	}
}

int ArrowStreamGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	if (!stream->release || !stream->private_data) {
		return EINVAL;
	}
	auto &self = *static_cast<ResultArrowStream *>(stream->private_data);
	out->release = nullptr;
	try {
		idx_t result_count = 0;
		ErrorData error;
		if (!ArrowUtil::TryFetchChunk(*self.scan_state, self.client_properties, self.batch_size, out, result_count,
		                              error, self.extension_types)) {
			self.last_error = error;
			return EIO;
		}
		if (result_count == 0) {
			// End of stream: a released (null) array.
			out->release = nullptr;
		}
	} catch (std::exception &ex) {
		self.last_error = ErrorData(ex);
		return EIO;
	}
	return 0;
}

const char *ArrowStreamGetLastError(ArrowArrayStream *stream) {
	if (!stream->release || !stream->private_data) {
		return "arrow stream was released";
	}
	auto &self = *static_cast<ResultArrowStream *>(stream->private_data);
	return self.last_error.Message().c_str();
}

void ArrowStreamRelease(ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	auto self = static_cast<ResultArrowStream *>(stream->private_data);
	stream->private_data = nullptr;
	if (!self) {
		return;
	}
	// Mirror result_destroy: close the engine result and roll back any injected
	// group transaction before freeing. A release callback must not throw
	// across the C ABI, so swallow exceptions here.
	self->scan_state.reset();
	try {
		if (self->wrapper) {
			self->wrapper->Finalize();
		}
	} catch (...) { // NOLINT: best-effort cleanup
	}
	// Frees the cached schema (destructor) and the wrapper (which releases the
	// connection busy slot in ~ResultWrapperV2).
	delete self;
}

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_result_to_arrow_stream(duckdb_v2_result_handle *result, idx_t batch_size,
                                                      struct ArrowArrayStream *out_stream,
                                                      duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!result || !*result || !out_stream) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_result_to_arrow_stream");
		}
		// Adopt the result by transfer; the slot is consumed from here on, on
		// success and on failure alike (the local unique_ptr frees it on throw).
		auto wrapper = duckdb::unique_ptr<duckdb::ResultWrapperV2>(duckdb::ToResult(*result));
		*result = nullptr;
		try {
			// Ensure the principal fragment's metadata is available so the schema
			// can be built under the live transaction. Non-expanding statements
			// already have it; expanding statements may need to advance to the
			// principal fragment (no principal rows are produced before it is
			// prepared, so stepping here never drops data).
			while (!wrapper->metadata_available) {
				duckdb::unique_ptr<duckdb::DataChunk> discard;
				auto status = wrapper->Step(discard);
				if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
					wrapper->Wait();
					continue;
				}
				if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
					throw duckdb::InternalException(
					    "arrow stream: a row was produced before result metadata was available");
				}
				break; // FINISHED / CANCELLED: no row-producing fragment.
			}
			if (!wrapper->context) {
				throw duckdb::InvalidInputException("result is not associated with an active context");
			}
			auto &ctx = *wrapper->context;

			auto self = duckdb::make_uniq<duckdb::ResultArrowStream>();
			self->batch_size = batch_size == 0 ? duckdb::kDefaultArrowBatchSize : batch_size;
			self->client_properties = ctx.GetClientProperties();
			// Build and cache the schema and the extension type map under the live
			// transaction (the schema-cache-under-txn invariant).
			self->extension_types = duckdb::ArrowTypeExtensionData::GetExtensionTypes(ctx, wrapper->types);
			duckdb::ArrowConverter::ToArrowSchema(&self->cached_schema, wrapper->types, wrapper->names,
			                                      self->client_properties);
			self->scan_state = duckdb::make_uniq<duckdb::ResultWrapperChunkScanState>(*wrapper);
			self->wrapper = std::move(wrapper);

			out_stream->get_schema = duckdb::ArrowStreamGetSchema;
			out_stream->get_next = duckdb::ArrowStreamGetNext;
			out_stream->get_last_error = duckdb::ArrowStreamGetLastError;
			out_stream->release = duckdb::ArrowStreamRelease;
			out_stream->private_data = self.release();
		} catch (...) {
			// A throw before ownership transferred (the move into self->wrapper) leaves
			// the result with us. Finalize it -- promptly Close the streaming result and
			// roll back any injected group transaction -- so a failed export cleans up
			// exactly like result_destroy instead of relying on lazy teardown. This is a
			// promptness/parity property, not a leak fix: the local unique_ptr frees the
			// result either way (LSan-verified, no leak delta).
			if (wrapper) {
				try {
					wrapper->Finalize();
				} catch (...) { // NOLINT: best-effort cleanup; never mask the original error
				}
			}
			throw;
		}
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_logical_types_to_arrow_schema(duckdb_v2_context_handle context,
                                                             const duckdb_v2_logical_type_handle *types,
                                                             const duckdb_v2_str *names, idx_t count,
                                                             struct ArrowSchema *out_schema,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context || !out_schema) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_logical_types_to_arrow_schema");
		}
		if (count > 0 && (!types || !names)) {
			throw duckdb::InvalidInputException("null types/names array with nonzero count");
		}
		auto &ctx = duckdb::ToContextRef(context);
		duckdb::vector<duckdb::LogicalType> schema_types;
		duckdb::vector<duckdb::string> schema_names;
		schema_types.reserve(count);
		schema_names.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			if (!types[i]) {
				throw duckdb::InvalidInputException("null logical type at index %llu", i);
			}
			schema_types.push_back(*duckdb::ToLogicalType(types[i]));
			schema_names.push_back(duckdb::ToString(names[i]));
		}
		auto properties = ctx.GetClientProperties();
		duckdb::ArrowConverter::ToArrowSchema(out_schema, schema_types, schema_names, properties);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_data_chunk_to_arrow_array(duckdb_v2_context_handle context,
                                                         duckdb_v2_data_chunk_handle chunk,
                                                         struct ArrowArray *out_array,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context || !chunk || !out_array) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_to_arrow_array");
		}
		auto &ctx = duckdb::ToContextRef(context);
		auto &dchunk = *duckdb::ToDataChunk(chunk);
		auto properties = ctx.GetClientProperties();
		auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(ctx, dchunk.GetTypes());
		duckdb::ArrowConverter::ToArrowArray(dchunk, out_array, properties, extension_type_cast);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_arrow_conversion_plan_create(duckdb_v2_context_handle context,
                                                            struct ArrowSchema *schema,
                                                            duckdb_v2_arrow_conversion_plan_handle *out_plan,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context || !schema || !out_plan) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arrow_conversion_plan_create");
		}
		*out_plan = nullptr;
		auto &ctx = duckdb::ToContextRef(context);
		auto arrow_table = duckdb::make_uniq<duckdb::ArrowTableSchema>();
		duckdb::ArrowTableFunction::PopulateArrowTableSchema(ctx, *arrow_table, *schema);
		*out_plan = reinterpret_cast<duckdb_v2_arrow_conversion_plan_handle>(arrow_table.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_arrow_array_to_data_chunk(duckdb_v2_context_handle context, struct ArrowArray *array,
                                                         duckdb_v2_arrow_conversion_plan_handle plan,
                                                         duckdb_v2_data_chunk_handle *out_chunk,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context || !array || !plan || !out_chunk) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arrow_array_to_data_chunk");
		}
		*out_chunk = nullptr;
		auto &ctx = duckdb::ToContextRef(context);
		auto &arrow_table = duckdb::ToArrowTableSchema(plan);
		auto &types = arrow_table.GetTypes();

		auto dchunk = duckdb::make_uniq<duckdb::DataChunk>();
		dchunk->Initialize(duckdb::Allocator::DefaultAllocator(), types, duckdb::NumericCast<idx_t>(array->length));

		auto &arrow_types = arrow_table.GetColumns();
		// Guard against an array whose layout disagrees with the conversion plan
		// before indexing array->children, which would otherwise be out of bounds.
		if (duckdb::NumericCast<idx_t>(array->n_children) != dchunk->ColumnCount()) {
			throw duckdb::InvalidInputException(
			    "Arrow array child count does not match the conversion plan column count");
		}
		dchunk->SetChildCardinality(duckdb::NumericCast<idx_t>(array->length));

		// One shared owner for the whole foreign array, transferred once. The
		// chunk's zero-copy vectors keep it alive; a column that copies still leaves
		// it owned here (released when this wrapper drops, at function end or with the
		// chunk). This mirrors the engine scan path: a per-column wrapper would free
		// the shared parent after the first copying column and dangle the rest.
		auto owned_array = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
		owned_array->arrow_array = *array;
		array->release = nullptr;
		auto &parent_array = owned_array->arrow_array;
		if (dchunk->ColumnCount() > 0 && !parent_array.children) {
			throw duckdb::InvalidInputException("Arrow array has null children");
		}
		for (idx_t i = 0; i < dchunk->ColumnCount(); i++) {
			auto *child_array = parent_array.children[i];
			// Validate the foreign child before handing it to the engine, so a
			// malformed array fails as INVALID_INPUT rather than crashing inside.
			if (!child_array || !child_array->release) {
				throw duckdb::InvalidInputException("Arrow array child is null or already released");
			}
			if (child_array->length != parent_array.length) {
				throw duckdb::InvalidInputException("Arrow array child length does not match the array length");
			}
			auto arrow_type = arrow_types.at(i);
			auto array_physical_type = arrow_type->GetPhysicalType();
			auto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(ctx);
			array_state->owned_data = owned_array;
			switch (array_physical_type) {
			case duckdb::ArrowArrayPhysicalType::DICTIONARY_ENCODED:
				if (!child_array->dictionary) {
					throw duckdb::InvalidInputException("Dictionary-encoded Arrow array has no dictionary");
				}
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(
				    dchunk->data[i], *child_array, 0, *array_state, dchunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::RUN_END_ENCODED:
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(
				    dchunk->data[i], *child_array, 0, *array_state, dchunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::DEFAULT:
				duckdb::ArrowToDuckDBConversion::SetValidityMask(dchunk->data[i], *child_array, 0, dchunk->size(),
				                                                 parent_array.offset, -1);
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDB(dchunk->data[i], *child_array, 0, *array_state,
				                                                     dchunk->size(), *arrow_type);
				break;
			default:
				throw duckdb::NotImplementedException("Only default Arrow physical types are currently supported");
			}
		}
		*out_chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(dchunk.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_arrow_conversion_plan_get_schema(duckdb_v2_arrow_conversion_plan_handle plan,
                                                                duckdb_v2_schema_handle *out_schema,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!plan || !out_schema) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arrow_conversion_plan_get_schema");
		}
		*out_schema = nullptr;
		auto &arrow_table = duckdb::ToArrowTableSchema(plan);
		auto &types = arrow_table.GetTypes();
		auto &names = arrow_table.GetNames();
		D_ASSERT(names.size() == types.size());
		// Copy into the standard owned schema wrapper (the same shape
		// statement_bind produces); the caller destroys it via schema_destroy.
		auto wrapper = duckdb::make_uniq<duckdb::SchemaWrapperV2>();
		for (duckdb::idx_t i = 0; i < types.size(); i++) {
			wrapper->fields.push_back({names[i], types[i]});
		}
		*out_schema = reinterpret_cast<duckdb_v2_schema_handle>(wrapper.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_arrow_conversion_plan_destroy(duckdb_v2_arrow_conversion_plan_handle *plan) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (plan && *plan) {
			delete reinterpret_cast<duckdb::ArrowTableSchema *>(*plan);
			*plan = nullptr;
		}
	});
}
