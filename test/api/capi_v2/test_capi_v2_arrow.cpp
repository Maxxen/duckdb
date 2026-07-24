#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 Arrow C Data Interface tests.
//
// Surface under test:
//   - duckdb_v2_logical_types_to_arrow_schema  (DuckDB types -> ArrowSchema)
//   - duckdb_v2_data_chunk_to_arrow_array      (DataChunk    -> ArrowArray)
//   - duckdb_v2_arrow_conversion_plan_create  (ArrowSchema  -> bound schema)
//   - duckdb_v2_arrow_array_to_data_chunk      (ArrowArray   -> DataChunk)
//   - duckdb_v2_arrow_conversion_plan_destroy
//   - duckdb_v2_result_to_arrow_stream         (result       -> ArrowArrayStream)
//
// The chunk converters key on a `context`, which only a callback provides. The
// value round-trip tests exercise them through two harnesses registered on the
// connection: arrow_roundtrip(x), a scalar function that pushes its input chunk
// through chunk -> Arrow array -> chunk and returns it unchanged (a value
// survives iff arrow_roundtrip(x) IS NOT DISTINCT FROM x), and
// arrow_roundtrip_range(count), a table function that constructs deterministic
// chunks, round-trips them internally, and emits the re-imported rows. Both run
// entirely in SQL, so the assertions are ordinary QueryBool checks.
//
// The standard Arrow structs (ArrowSchema / ArrowArray / ArrowArrayStream)
// come from duckdb_v2.h directly (this file pulls in no Arrow header).
// ---------------------------------------------------------------------------

namespace {

// Borrowed vector at column `col` of a chunk.
duckdb_v2_vector_handle ChunkVector(duckdb_v2_data_chunk_handle chunk, idx_t col) {
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, col, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vec != nullptr);
	return vec;
}

idx_t ChunkSize(duckdb_v2_data_chunk_handle chunk) {
	idx_t n = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &n, nullptr) == DUCKDB_V2_ERROR_NONE);
	return n;
}

// Resolve a logical row index through the vector's selection vector.
idx_t ResolveRow(const duckdb_v2_vector_view &view, idx_t row) {
	return view.sel ? SelAt(view.sel, row) : row;
}

// Steps the first chunk out of a single-statement query. The returned chunk is
// owned and independent of the result, which is destroyed before returning.
duckdb_v2_data_chunk_handle QueryOneChunk(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, sql, &r) == DUCKDB_V2_ERROR_NONE);
	auto chunk = V2StepChunk(r);
	duckdb_v2_result_destroy(&r);
	return chunk;
}

// Drives a stream's get_next to exhaustion, returning the total Arrow row
// count and the number of arrays produced.
struct StreamStats {
	int64_t rows = 0;
	idx_t arrays = 0;
	int64_t first_array_rows = 0;
};
StreamStats DrainStream(ArrowArrayStream &stream) {
	StreamStats stats;
	while (true) {
		ArrowArray array {};
		REQUIRE(stream.get_next(&stream, &array) == 0);
		if (!array.release) {
			break;
		}
		if (stats.arrays == 0) {
			stats.first_array_rows = array.length;
		}
		stats.rows += array.length;
		stats.arrays++;
		array.release(&array);
	}
	return stats;
}

// Search the values of an Arrow schema's metadata blob for a substring. The
// blob is: int32 n_keys, then n_keys * (int32 keylen, key, int32 vallen, val).
bool MetadataValueContains(const char *metadata, const std::string &needle) {
	if (!metadata) {
		return false;
	}
	int32_t n_keys = 0;
	std::memcpy(&n_keys, metadata, sizeof(int32_t));
	metadata += sizeof(int32_t);
	for (int32_t i = 0; i < n_keys; i++) {
		int32_t key_len = 0;
		std::memcpy(&key_len, metadata, sizeof(int32_t));
		metadata += sizeof(int32_t) + key_len;
		int32_t val_len = 0;
		std::memcpy(&val_len, metadata, sizeof(int32_t));
		metadata += sizeof(int32_t);
		if (std::string(metadata, static_cast<size_t>(val_len)).find(needle) != std::string::npos) {
			return true;
		}
		metadata += val_len;
	}
	return false;
}

// ---------------------------------------------------------------------------
// arrow_roundtrip(x): a scalar function that passes its input straight through
// the Arrow chunk converters (DuckDB chunk -> Arrow array -> DuckDB chunk) and
// returns it unchanged. Registered with an ANY return type; the bind callback
// reads the argument type, sets the concrete return type to match, and builds
// the reusable conversion plan. This exercises the exec-phase converters on
// every type (nested included) with a single function: a value survives iff
// arrow_roundtrip(x) IS NOT DISTINCT FROM x.
// ---------------------------------------------------------------------------

void ArrowRtPlanDestroy(void *p) {
	auto plan = reinterpret_cast<duckdb_v2_arrow_conversion_plan_handle>(p);
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
}

void ArrowRtBind(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_bind_arguments_handle args = nullptr;
	if (duckdb_v2_scalar_function_bind_get_arguments(info, &args, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_logical_type_handle arg_type = nullptr;
	if (duckdb_v2_bind_arguments_get_type(args, 0, &arg_type, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// The result type follows the input type.
	if (duckdb_v2_scalar_function_bind_set_return_type(info, arg_type, err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_destroy(&arg_type);
		return;
	}
	// Build the single-column Arrow schema + conversion plan for this type.
	ArrowSchema schema {};
	duckdb_v2_str name = V2Str("x");
	auto rc = duckdb_v2_logical_types_to_arrow_schema(ctx, &arg_type, &name, 1, &schema, err);
	duckdb_v2_logical_type_destroy(&arg_type);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	rc = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, err);
	schema.release(&schema);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// The plan rides the bind data and is destroyed when the binding is torn down.
	duckdb_v2_opaque bind_data {plan, ArrowRtPlanDestroy, nullptr};
	duckdb_v2_scalar_function_bind_set_bind_data(info, bind_data, err);
}

void ArrowRtExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                 duckdb_v2_error_info_handle *err) {
	void *bind_data = nullptr;
	if (duckdb_v2_scalar_function_exec_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto plan = reinterpret_cast<duckdb_v2_arrow_conversion_plan_handle>(bind_data);
	duckdb_v2_data_chunk_handle in = nullptr;
	if (duckdb_v2_scalar_function_exec_get_input(info, &in, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// Export the input chunk to Arrow, then import it back through the plan.
	ArrowArray array {};
	if (duckdb_v2_data_chunk_to_arrow_array(ctx, in, &array, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_data_chunk_handle imported = nullptr;
	auto rc = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &imported, err);
	if (array.release) {
		array.release(&array);
	}
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// Reference the re-imported column into the result (zero-copy; the shared
	// buffers keep the data alive after the imported chunk is destroyed).
	duckdb_v2_vector_handle imported_vec = nullptr;
	duckdb_v2_vector_handle result = nullptr;
	if (duckdb_v2_data_chunk_get_vector(imported, 0, &imported_vec, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_scalar_function_exec_get_result(info, &result, err) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_vector_reference(result, imported_vec, err);
	}
	duckdb_v2_data_chunk_destroy(&imported);
}

// Registers arrow_roundtrip(x) on the connection.
void RegisterArrowRoundtrip(duckdb_v2_connection_handle conn) {
	duckdb_v2_scalar_function_builder_handle b = nullptr;
	REQUIRE(duckdb_v2_scalar_function_builder_create(&b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle any_type = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_ANY, &any_type, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_builder_set_name(b, V2Str("arrow_roundtrip"), nullptr) == DUCKDB_V2_ERROR_NONE);
	V2ScalarSignature(b, [&](duckdb_v2_function_signature_handle sig) {
		V2SigParam(sig, "x", any_type);
		V2SigReturn(sig, any_type);
	});
	REQUIRE(duckdb_v2_scalar_function_builder_set_bind_callback(b, ArrowRtBind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_builder_set_exec_callback(b, ArrowRtExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_builder_register_with_connection(conn, b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_builder_destroy(&b) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&any_type);
}

// ---------------------------------------------------------------------------
// arrow_roundtrip_range(count BIGINT): a table function that emits `count` rows
// of (i BIGINT, s VARCHAR) where i = row index and s = 'r' || i, with every
// emitted chunk having been pushed through the Arrow chunk converters internally
// (construct chunk -> Arrow array -> chunk) before being referenced into the
// output. This drives the exec-phase converters over multi-column, multi-chunk
// output: the STRUCTURAL edges (chunking of an oversized array, multi-column
// import) surface as ordinary SQL over SELECT * FROM arrow_roundtrip_range(N).
// ---------------------------------------------------------------------------

struct ArrowRtRangeBindData {
	int64_t count = 0;
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
};

struct ArrowRtRangeGlobalState {
	int64_t emitted = 0;
};

void ArrowRtRangeBind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	// count parameter (slot 0).
	duckdb_v2_bind_arguments_handle args = nullptr;
	if (duckdb_v2_table_function_bind_get_arguments(info, &args, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_value_handle count_val = nullptr;
	if (duckdb_v2_bind_arguments_fold(args, ctx, 0, &count_val, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int64_t count = V2LeafPayload<int64_t>(count_val);
	duckdb_v2_value_destroy(&count_val);

	// Output columns: i BIGINT, s VARCHAR.
	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("i"), bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("s"), varchar_type, err);

	// Build the two-column Arrow schema + conversion plan for the output types.
	duckdb_v2_logical_type_handle types[2] = {bigint_type, varchar_type};
	duckdb_v2_str names[2] = {V2Str("i"), V2Str("s")};
	ArrowSchema schema {};
	auto rc = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, 2, &schema, err);
	duckdb_v2_logical_type_destroy(&bigint_type);
	duckdb_v2_logical_type_destroy(&varchar_type);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	rc = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, err);
	schema.release(&schema);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	auto *bd = new ArrowRtRangeBindData {count, plan};
	duckdb_v2_table_function_bind_set_bind_data(info,
	                                            {bd,
	                                             [](void *p) {
		                                             auto *d = static_cast<ArrowRtRangeBindData *>(p);
		                                             duckdb_v2_arrow_conversion_plan_destroy(&d->plan);
		                                             delete d;
	                                             },
	                                             nullptr},
	                                            err);
	duckdb_v2_table_function_bind_set_cardinality(info, static_cast<idx_t>(count), true, err);
}

void ArrowRtRangeInitGlobal(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                            duckdb_v2_error_info_handle *err) {
	auto *state = new ArrowRtRangeGlobalState();
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<ArrowRtRangeGlobalState *>(p); }, nullptr}, err);
}

void ArrowRtRangeExec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle out = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &out, err);

	void *raw_bd = nullptr;
	duckdb_v2_table_function_exec_get_bind_data(info, &raw_bd, err);
	auto *bd = static_cast<ArrowRtRangeBindData *>(raw_bd);

	void *raw_gs = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &raw_gs, err);
	auto &state = *static_cast<ArrowRtRangeGlobalState *>(raw_gs);

	if (state.emitted >= bd->count) {
		// End of data: size the first output vector to 0 (the bridge reads
		// cardinality from output vector 0).
		duckdb_v2_vector_handle out0 = nullptr;
		duckdb_v2_data_chunk_get_vector(out, 0, &out0, err);
		duckdb_v2_vector_set_size(out0, 0, err);
		return;
	}

	int64_t remaining = bd->count - state.emitted;
	idx_t n = remaining < 2048 ? static_cast<idx_t>(remaining) : 2048;

	// Construct a fresh input chunk of BIGINT i, VARCHAR s and fill it.
	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, err);
	duckdb_v2_logical_type_handle types[2] = {bigint_type, varchar_type};
	duckdb_v2_data_chunk_handle in = nullptr;
	duckdb_v2_data_chunk_create(types, 2, &in, err);
	duckdb_v2_logical_type_destroy(&bigint_type);
	duckdb_v2_logical_type_destroy(&varchar_type);

	duckdb_v2_vector_handle in_i = nullptr, in_s = nullptr;
	duckdb_v2_data_chunk_get_vector(in, 0, &in_i, err);
	duckdb_v2_data_chunk_get_vector(in, 1, &in_s, err);
	int64_t *i_data = nullptr;
	duckdb_v2_vector_get_data_mutable(in_i, reinterpret_cast<void **>(&i_data), err);
	for (idx_t row = 0; row < n; row++) {
		int64_t value = state.emitted + static_cast<int64_t>(row);
		i_data[row] = value;
		auto s = "r" + std::to_string(value);
		V2VectorAssignString(in_s, row, s.c_str(), s.size(), err);
	}
	duckdb_v2_vector_set_size(in_i, n, err);
	duckdb_v2_vector_set_size(in_s, n, err);

	// Round-trip the constructed chunk through the Arrow converters.
	ArrowArray array {};
	duckdb_v2_data_chunk_to_arrow_array(ctx, in, &array, err);
	duckdb_v2_data_chunk_handle imported = nullptr;
	duckdb_v2_arrow_array_to_data_chunk(ctx, &array, bd->plan, &imported, err);
	if (array.release) {
		array.release(&array);
	}
	duckdb_v2_data_chunk_destroy(&in);

	// Reference the re-imported columns into the output (zero-copy; the shared
	// buffers keep the data alive after the imported chunk is destroyed).
	duckdb_v2_vector_handle imp_i = nullptr, imp_s = nullptr, out_i = nullptr, out_s = nullptr;
	duckdb_v2_data_chunk_get_vector(imported, 0, &imp_i, err);
	duckdb_v2_data_chunk_get_vector(imported, 1, &imp_s, err);
	duckdb_v2_data_chunk_get_vector(out, 0, &out_i, err);
	duckdb_v2_data_chunk_get_vector(out, 1, &out_s, err);
	duckdb_v2_vector_reference(out_i, imp_i, err);
	duckdb_v2_vector_reference(out_s, imp_s, err);
	duckdb_v2_data_chunk_destroy(&imported);

	duckdb_v2_vector_set_size(out_i, n, err);
	state.emitted += static_cast<int64_t>(n);
}

// Registers arrow_roundtrip_range(count BIGINT) on the connection.
void RegisterArrowRoundtripRange(duckdb_v2_connection_handle conn) {
	duckdb_v2_table_function_builder_handle b = nullptr;
	REQUIRE(duckdb_v2_table_function_builder_create(&b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_name(b, V2Str("arrow_roundtrip_range"), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, nullptr);
	V2TableSignature(b, [&](duckdb_v2_function_signature_handle sig) { V2SigParam(sig, "n", bigint_type); });
	duckdb_v2_logical_type_destroy(&bigint_type);
	REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(b, ArrowRtRangeBind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(b, ArrowRtRangeInitGlobal, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(b, ArrowRtRangeExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_register_with_connection(conn, b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_builder_destroy(&b);
}

// Runs a single-row, single-BOOLEAN query and returns the value (false if NULL).
bool QueryBool(duckdb_v2_connection_handle conn, const char *sql) {
	auto chunk = QueryOneChunk(conn, sql);
	REQUIRE(chunk != nullptr);
	REQUIRE(ChunkSize(chunk) == 1);
	auto vec = ChunkVector(chunk, 0);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	const auto row = ResolveRow(view, 0);
	// The queries below aggregate with bool_and over a non-empty set, so the
	// single result row is never NULL.
	const bool value = reinterpret_cast<const bool *>(view.data)[row];
	duckdb_v2_data_chunk_destroy(&chunk);
	return value;
}

} // namespace

// ===========================================================================
// Flat round-trip: DuckDB chunk -> Arrow (schema + array) -> DuckDB chunk,
// with full value comparison over INTEGER / DOUBLE / VARCHAR.
// ===========================================================================

TEST_CASE("V2 arrow: flat round-trip preserves values", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);
	// Every flat value survives chunk -> Arrow array -> chunk unchanged (value
	// and type): arrow_roundtrip(x) IS NOT DISTINCT FROM x for all rows.
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(a) IS NOT DISTINCT FROM a "
	                           "AND arrow_roundtrip(b) IS NOT DISTINCT FROM b "
	                           "AND arrow_roundtrip(c) IS NOT DISTINCT FROM c) "
	                           "FROM (SELECT i::INTEGER a, i::DOUBLE b, ('r' || i) c FROM range(5) t(i)) s"));
}

// ===========================================================================
// Nested round-trip: LIST / STRUCT / MAP / UNION survive the round-trip with
// correct types and spot-checked values.
// ===========================================================================

TEST_CASE("V2 arrow: nested round-trip preserves structure", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);

	// LIST / STRUCT / MAP / UNION (plus an empty list and a NULL row) all survive
	// chunk -> Arrow array -> chunk unchanged. IS NOT DISTINCT FROM compares nested
	// structure and NULLs, so equality alone proves the round-trip.
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(x) IS NOT DISTINCT FROM x) "
	                           "FROM (VALUES ([1, 2, 3]), ([]), (NULL::INTEGER[])) t(x)"));
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(x) IS NOT DISTINCT FROM x) "
	                           "FROM (VALUES ({'a': 1, 'b': 2}), (NULL::STRUCT(a INTEGER, b INTEGER))) t(x)"));
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(x) IS NOT DISTINCT FROM x) "
	                           "FROM (VALUES (MAP([1, 2], [10, 20])), (MAP([], []))) t(x)"));
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(x) IS NOT DISTINCT FROM x) "
	                           "FROM (SELECT union_value(num := i) AS x FROM range(3) t(i)) s"));
}

// ===========================================================================
// result_to_arrow_stream: ownership transfer, schema, and correct row data.
// ===========================================================================

TEST_CASE("V2 arrow: result_to_arrow_stream yields all rows and a stable schema", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	// The result is consumed by transfer.
	REQUIRE(r == nullptr);
	REQUIRE(stream.release != nullptr);

	// Schema is available and stable.
	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.release != nullptr);
	REQUIRE(schema.n_children == 1);
	schema.release(&schema);

	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 100000);
	// Default batch size (131072) exceeds the row count: a single array.
	REQUIRE(stats.arrays == 1);

	stream.release(&stream);
	REQUIRE(stream.release == nullptr);
}

// ===========================================================================
// batch_size coalescing: a batch_size larger than STANDARD_VECTOR_SIZE (2048)
// coalesces many engine chunks into each Arrow array.
// ===========================================================================

TEST_CASE("V2 arrow: result_to_arrow_stream coalesces chunks to batch_size", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r) == DUCKDB_V2_ERROR_NONE);

	ArrowArrayStream stream {};
	// 10000 rows per array, well above the 2048-row engine chunk: each array
	// spans ~5 engine chunks. 100000 / 10000 = 10 arrays.
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 10000, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 100000);
	REQUIRE(stats.arrays == 10);
	REQUIRE(stats.first_array_rows == 10000);

	stream.release(&stream);
}

// ===========================================================================
// Partial consumption: stepping some chunks first, then exporting, yields
// only the remaining rows.
// ===========================================================================

TEST_CASE("V2 arrow: result_to_arrow_stream over a partially consumed result", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(5000) t(i)", &r) == DUCKDB_V2_ERROR_NONE);

	// Consume the first engine chunk (2048 rows).
	auto first = V2StepChunk(r);
	REQUIRE(first != nullptr);
	idx_t consumed = ChunkSize(first);
	REQUIRE(consumed == 2048);
	duckdb_v2_data_chunk_destroy(&first);

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The stream must continue from where consumption left off, not restart. Read
	// the first remaining value (a BIGINT column inside the struct array): it must
	// be the consumed offset, not 0.
	ArrowArray firstbatch {};
	REQUIRE(stream.get_next(&stream, &firstbatch) == 0);
	REQUIRE(firstbatch.release != nullptr);
	REQUIRE(firstbatch.n_children == 1);
	const auto *col = firstbatch.children[0];
	const auto *values = reinterpret_cast<const int64_t *>(col->buffers[1]);
	REQUIRE(values[col->offset] == static_cast<int64_t>(consumed));
	int64_t total = firstbatch.length;
	firstbatch.release(&firstbatch);

	total += DrainStream(stream).rows;
	REQUIRE(total == static_cast<int64_t>(5000 - consumed));

	stream.release(&stream);
}

// ===========================================================================
// Ownership: while the stream is live it holds the connection's busy slot;
// release frees it for the connection's next query.
// ===========================================================================

TEST_CASE("V2 arrow: stream owns the connection cursor until released", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10) t(i)", &r) == DUCKDB_V2_ERROR_NONE);

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The connection is busy: a new query is refused while the stream is live.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r2) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(r2 == nullptr);

	// Releasing the stream frees the connection.
	stream.release(&stream);
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r2) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_destroy(&r2);
}

// ===========================================================================
// get_schema stays valid (and dictionary-carrying) across a full drain. This
// is the schema-stability half of the schema-cache invariant; an ENUM gives the
// schema a dictionary. NOTE: ENUM schema construction does not touch the
// catalog, so this does not exercise the catalog-dependent #475 path (extension
// types whose populate_arrow_schema reads the catalog). That faithful
// regression lives in the Arrow type-extension test.
// ===========================================================================

TEST_CASE("V2 arrow: get_schema is valid after the result is drained", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	V2ExecSQL(fx.conn, "CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok')");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 'happy'::mood AS m", &r) == DUCKDB_V2_ERROR_NONE);

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Schema before draining: an ENUM is dictionary-encoded.
	ArrowSchema before {};
	REQUIRE(stream.get_schema(&stream, &before) == 0);
	REQUIRE(before.release != nullptr);
	REQUIRE(before.n_children == 1);
	REQUIRE(before.children[0]->dictionary != nullptr);
	before.release(&before);

	// Drain to completion: the producing transaction ends here.
	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 1);

	// Schema after draining must still succeed and still carry the dictionary,
	// because it was cached at creation, not rebuilt against the closed result.
	ArrowSchema after {};
	REQUIRE(stream.get_schema(&stream, &after) == 0);
	REQUIRE(after.release != nullptr);
	REQUIRE(after.n_children == 1);
	REQUIRE(after.children[0]->dictionary != nullptr);
	after.release(&after);

	stream.release(&stream);
}

// ===========================================================================
// Oversized import: an Arrow array longer than STANDARD_VECTOR_SIZE imports
// into a single data chunk (the chunk is sized to the array length).
// ===========================================================================

TEST_CASE("V2 arrow: importing an oversized array yields one chunk", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	RegisterArrowRoundtripRange(fx.conn);

	// arrow_roundtrip_range internally exports each constructed chunk to an Arrow
	// array and re-imports it. 5000 > STANDARD_VECTOR_SIZE (2048) exercises the
	// chunking of a multi-chunk range; the full set of generated rows (i in
	// 0..4999, each with s = 'r' || i) must survive the round-trip.
	REQUIRE(QueryBool(fx.conn, "SELECT count(*) = 5000 AND count(DISTINCT i) = 5000 "
	                           "AND min(i) = 0 AND max(i) = 4999 AND bool_and(s = 'r' || i) "
	                           "FROM arrow_roundtrip_range(5000)"));
}

// ===========================================================================
// NULL values survive both conversion directions (validity mask handling).
// ===========================================================================

TEST_CASE("V2 arrow: NULL values survive the round-trip", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);

	// A NULL and a non-NULL flat value both survive (validity mask handling): the
	// interleaved NULL/value rows compare equal after the round-trip.
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                           "FROM (VALUES (NULL::BIGINT), (1)) t(v)"));
	// NULLs nested inside a list and a struct also survive.
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                           "FROM (VALUES ([1, NULL, 3]), (NULL::INTEGER[])) t(v)"));
	REQUIRE(QueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                           "FROM (VALUES ({'a': NULL::INTEGER, 'b': 2}), "
	                           "             (NULL::STRUCT(a INTEGER, b INTEGER))) t(v)"));
}

// ===========================================================================
// A released stream rejects get_schema / get_next.
// ===========================================================================

TEST_CASE("V2 arrow: a released stream rejects further operations", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(10) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto get_schema = stream.get_schema;
	auto get_next = stream.get_next;
	stream.release(&stream);
	REQUIRE(stream.release == nullptr);

	// The released stream nulls release/private_data; the surviving callbacks
	// must report failure rather than touch freed state.
	ArrowSchema schema {};
	REQUIRE(get_schema(&stream, &schema) != 0);
	ArrowArray array {};
	REQUIRE(get_next(&stream, &array) != 0);
}

// ===========================================================================
// Faithful #475 regression: a built-in GEOMETRY(crs) column. GEOMETRY is a
// distinct built-in type whose Arrow schema export runs WriteCRS -> TryConvert
// -> Transaction::Get -- the exact catalog/transaction-touching path of the
// original bug. The schema must be built and cached at stream creation under
// the live transaction; get_schema must then stay valid after the transaction
// is gone. CRS 'OGC:CRS84' resolves against a built-in default coordinate
// system, so this needs no spatial extension.
// ===========================================================================

TEST_CASE("V2 arrow: GEOMETRY(crs) schema is cached under the transaction (#475)", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT NULL::GEOMETRY('OGC:CRS84') AS g", &r) == DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Schema built and cached at creation, under the live transaction (the
	// catalog-touching WriteCRS ran once, here). The metadata must actually carry
	// the resolved CRS, not just be present (a silent WriteCRS no-op would still
	// emit empty geoarrow metadata).
	ArrowSchema before {};
	REQUIRE(stream.get_schema(&stream, &before) == 0);
	REQUIRE(before.n_children == 1);
	REQUIRE(before.children[0]->metadata != nullptr);
	REQUIRE(MetadataValueContains(before.children[0]->metadata, "CRS84"));
	before.release(&before);

	// Drain fully, then run another query so the producing transaction is
	// definitely gone before the post-drain get_schema (the stream releases its
	// transaction on stream.release(), not on drain, so kick the connection).
	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 1);
	V2ExecSQL(fx.conn, "SELECT 1");

	// A lazy get_schema would re-run WriteCRS -> TryConvert with no active
	// transaction and crash ("ActiveTransaction called without active
	// transaction"); the cached schema makes get_schema pure and valid.
	ArrowSchema after {};
	REQUIRE(stream.get_schema(&stream, &after) == 0);
	REQUIRE(after.n_children == 1);
	REQUIRE(after.children[0]->metadata != nullptr);
	REQUIRE(MetadataValueContains(after.children[0]->metadata, "CRS84"));
	after.release(&after);

	stream.release(&stream);
}

// ===========================================================================
// Multi-column import: a single foreign array imports into more than one output
// column, all backed by one shared owner. A per-column owner would free the
// parent after the first column and dangle the rest; the shared owner keeps it
// alive for the whole loop. arrow_roundtrip_range emits two columns (BIGINT i,
// VARCHAR s) re-imported from the same round-tripped array. (Run under ASAN.)
// ===========================================================================

TEST_CASE("V2 arrow: multi-column import with a leading extension-cast column", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	RegisterArrowRoundtripRange(fx.conn);

	// Both re-imported columns of the shared array survive: i in 0..9 with the
	// matching s = 'r' || i. Reading both columns is what would dangle the trailing
	// column under a per-column owner.
	REQUIRE(QueryBool(fx.conn, "SELECT count(*) = 10 AND bool_and(s = 'r' || i) "
	                           "AND min(i) = 0 AND max(i) = 9 "
	                           "FROM arrow_roundtrip_range(10)"));
}

// ===========================================================================
// Stream edge cases: empty result, a runtime error surfacing through get_next,
// and batch_size == 1.
// ===========================================================================

TEST_CASE("V2 arrow: an empty result yields a valid schema and no rows", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(0) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.n_children == 1);
	schema.release(&schema);

	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 0);
	REQUIRE(stats.arrays == 0); // end-of-stream on the first get_next
	stream.release(&stream);
}

TEST_CASE("V2 arrow: a runtime error surfaces from the stream", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	// Schema is fine (INTEGER); the conversion of 'x' fails during execution.
	REQUIRE(V2Query(fx.conn, "SELECT s::INTEGER AS n FROM (VALUES ('1'), ('2'), ('x')) t(s)", &r) ==
	        DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	bool saw_error = false;
	while (true) {
		ArrowArray a {};
		if (stream.get_next(&stream, &a) != 0) {
			saw_error = true;
			break;
		}
		if (!a.release) {
			break; // clean end-of-stream (not expected for this query)
		}
		a.release(&a);
	}
	REQUIRE(saw_error);
	const char *msg = stream.get_last_error(&stream);
	REQUIRE(msg != nullptr);
	REQUIRE(std::string(msg).size() > 0);
	stream.release(&stream);
}

// A creation-time throw (during the metadata-advance step of an expanding
// statement) runs the Finalize-on-throw path: it promptly Closes the adopted
// streaming result and rolls back the injected group transaction, matching
// duckdb_v2_result_destroy. A non-constant DEFAULT that divides by zero expands
// the ALTER into a wrapped transaction whose UPDATE errors when stepped. This
// asserts the connection and catalog recover: the half-applied ADD COLUMN is
// gone and the connection runs again.
//
// This is a parity/smoke check, NOT an isolation of the Finalize. The Finalize
// only makes the cleanup prompt: without it, the abandoned active-query state is
// released and the injected transaction rolled back at context teardown anyway.
// Verified under LeakSanitizer (Linux): toggling the Finalize gives no leak
// delta (the wrapper uniquely owns the result, so destroying it already drops
// its ClientContext ref). These assertions pass with or without it; they guard
// only against a total break (a bricked connection or a leaked half-applied DDL).
TEST_CASE("V2 arrow: a creation-time error rolls back the injected transaction", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	V2ExecSQL(fx.conn, "INSERT INTO t VALUES (0)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT (10 // a)", &r) == DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 0, &stream, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr); // consumed by transfer even on failure

	// The injected ADD COLUMN was rolled back: column c must not exist, so a
	// query referencing it fails at bind.
	duckdb_v2_result_handle probe = nullptr;
	auto probe_rc = V2Query(fx.conn, "SELECT c FROM t", &probe);
	if (probe) {
		duckdb_v2_result_destroy(&probe);
	}
	REQUIRE(probe_rc != DUCKDB_V2_ERROR_NONE);

	// And the connection is usable for a normal query.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT 1", &r2) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(V2DrainRowCount(r2) == 1);
	duckdb_v2_result_destroy(&r2);
}

TEST_CASE("V2 arrow: batch_size of 1 yields one row per array", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i FROM range(3) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 1, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto stats = DrainStream(stream);
	REQUIRE(stats.rows == 3);
	REQUIRE(stats.arrays == 3);
	REQUIRE(stats.first_array_rows == 1);
	stream.release(&stream);
}

// ===========================================================================
// Error paths: null arguments and a destroyed/empty conversion plan.
// ===========================================================================

TEST_CASE("V2 arrow: error paths reject invalid arguments", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	ArrowSchema schema {};
	ArrowArray array {};
	ArrowArrayStream stream {};

	// Null context.
	REQUIRE(duckdb_v2_logical_types_to_arrow_schema(nullptr, nullptr, nullptr, 0, &schema, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_data_chunk_to_arrow_array(nullptr, nullptr, &array, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_arrow_conversion_plan_create(nullptr, &schema, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	// Null result to the stream exporter; the slot stays null.
	REQUIRE(duckdb_v2_result_to_arrow_stream(nullptr, 0, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_result_handle null_result = nullptr;
	REQUIRE(duckdb_v2_result_to_arrow_stream(&null_result, 0, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Import with a null array and with a null conversion plan. The null-argument
	// checks fire before the context is used, so a null context here still yields
	// INPUT_INVALID (there is no valid context to hand at test scope).
	duckdb_v2_data_chunk_handle dst = nullptr;
	duckdb_v2_arrow_conversion_plan_handle empty = nullptr;
	REQUIRE(duckdb_v2_arrow_array_to_data_chunk(nullptr, nullptr, empty, &dst, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	ArrowArray dummy {};
	REQUIRE(duckdb_v2_arrow_array_to_data_chunk(nullptr, &dummy, nullptr, &dst, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	// Destroy is null-safe.
	REQUIRE(duckdb_v2_arrow_conversion_plan_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_arrow_conversion_plan_handle null_handle = nullptr;
	REQUIRE(duckdb_v2_arrow_conversion_plan_destroy(&null_handle) == DUCKDB_V2_ERROR_NONE);
}
