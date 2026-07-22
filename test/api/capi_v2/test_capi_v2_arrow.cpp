#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

#include <functional>
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
// The converters key on a `context`, obtained via
// connection_execute_with_context. Catch2 assertions throw, and a throw
// across the C callback boundary would be swallowed by the bridge's error
// handler, so the in-callback bodies only perform C API calls and stash their
// return codes / outputs; all REQUIREs run after the callback returns.
//
// The standard Arrow structs (ArrowSchema / ArrowArray / ArrowArrayStream)
// come from duckdb_v2.h directly (this file pulls in no Arrow header).
// ---------------------------------------------------------------------------

namespace {

using CtxBody = std::function<void(duckdb_v2_context_handle, duckdb_v2_error_info_handle *)>;

// Runs `body` with an active context + transaction on `conn`. Returns the
// connection_execute_with_context return code; `body` stashes its own
// per-call codes/outputs for the caller to assert after this returns.
DUCKDB_V2_ERROR V2RunWithContext(duckdb_v2_connection_handle conn, CtxBody body) {
	duckdb_v2_error_info_handle err = nullptr;
	auto trampoline = [](duckdb_v2_context_handle ctx, void *ud, duckdb_v2_error_info_handle *e) {
		(*static_cast<CtxBody *>(ud))(ctx, e);
	};
	auto rc = duckdb_v2_connection_execute_with_context(conn, trampoline, &body, &err);
	duckdb_v2_error_info_destroy(&err);
	return rc;
}

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

DUCKDB_V2_LOGICAL_TYPE_ID VectorTypeId(duckdb_v2_vector_handle vec) {
	duckdb_v2_logical_type_handle lt = nullptr;
	REQUIRE(duckdb_v2_vector_get_logical_type(vec, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(lt, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&lt);
	return id;
}

// Resolve a logical row index through the vector's selection vector.
idx_t ResolveRow(const duckdb_v2_vector_view &view, idx_t row) {
	return view.sel ? SelAt(view.sel, row) : row;
}

template <class T>
T GetScalar(duckdb_v2_vector_handle vec, idx_t row) {
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	return reinterpret_cast<const T *>(view.data)[ResolveRow(view, row)];
}

std::string GetVarchar(duckdb_v2_vector_handle vec, idx_t row) {
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	auto storage = reinterpret_cast<const duckdb_v2_varchar_t *>(view.data);
	return V2StrTo(V2StringView(storage[ResolveRow(view, row)]));
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

// A no-op Arrow array release (for hand-built malformed test arrays): just
// marks the struct released so the bridge's adoption wrapper is satisfied.
void NoopArrayRelease(ArrowArray *array) {
	array->release = nullptr;
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

} // namespace

// ===========================================================================
// Flat round-trip: DuckDB chunk -> Arrow (schema + array) -> DuckDB chunk,
// with full value comparison over INTEGER / DOUBLE / VARCHAR.
// ===========================================================================

TEST_CASE("V2 arrow: flat round-trip preserves values", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	auto src = QueryOneChunk(fx.conn, "SELECT i::INTEGER AS a, i::DOUBLE AS b, ('r' || i) AS c FROM range(5) t(i)");
	REQUIRE(src != nullptr);
	REQUIRE(ChunkSize(src) == 5);

	const idx_t COLS = 3;
	duckdb_v2_logical_type_handle types[COLS] = {nullptr, nullptr, nullptr};
	for (idx_t i = 0; i < COLS; i++) {
		REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(src, i), &types[i], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_str names[COLS] = {V2Str("a"), V2Str("b"), V2Str("c")};

	ArrowSchema schema {};
	ArrowArray array {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_array = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;

	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, COLS, &schema, e);
		c_array = duckdb_v2_data_chunk_to_arrow_array(ctx, src, &array, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &dst, e);
	});

	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_array == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(plan != nullptr);
	REQUIRE(dst != nullptr);

	// The Arrow schema describes three columns.
	REQUIRE(schema.n_children == 3);

	// The re-imported chunk equals the source.
	REQUIRE(ChunkSize(dst) == 5);
	REQUIRE(VectorTypeId(ChunkVector(dst, 0)) == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	REQUIRE(VectorTypeId(ChunkVector(dst, 1)) == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
	REQUIRE(VectorTypeId(ChunkVector(dst, 2)) == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	for (idx_t row = 0; row < 5; row++) {
		REQUIRE(GetScalar<int32_t>(ChunkVector(dst, 0), row) == static_cast<int32_t>(row));
		REQUIRE(GetScalar<double>(ChunkVector(dst, 1), row) == static_cast<double>(row));
		REQUIRE(GetVarchar(ChunkVector(dst, 2), row) == ("r" + std::to_string(row)));
	}

	// Export filled the schema and array; the import adopted the array's data
	// (its release was nulled), so only the schema is freed here.
	REQUIRE(array.release == nullptr);
	REQUIRE(schema.release != nullptr);
	schema.release(&schema);
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	REQUIRE(plan == nullptr);
	duckdb_v2_data_chunk_destroy(&dst);
	duckdb_v2_data_chunk_destroy(&src);
	for (idx_t i = 0; i < COLS; i++) {
		duckdb_v2_logical_type_destroy(&types[i]);
	}
}

// ===========================================================================
// Nested round-trip: LIST / STRUCT / MAP / UNION survive the round-trip with
// correct types and spot-checked values.
// ===========================================================================

TEST_CASE("V2 arrow: nested round-trip preserves structure", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	// range() yields BIGINT, so the list child and struct.x fields are int64.
	const char *sql = "SELECT [i, i + 1] AS lst, "
	                  "       {'x': i, 'y': ('s' || i)} AS strct, "
	                  "       MAP([i], [i * 10]) AS mp, "
	                  "       union_value(num := i) AS un "
	                  "FROM range(3) t(i)";
	auto src = QueryOneChunk(fx.conn, sql);
	REQUIRE(src != nullptr);
	REQUIRE(ChunkSize(src) == 3);

	const idx_t COLS = 4;
	duckdb_v2_logical_type_handle types[COLS] = {nullptr, nullptr, nullptr, nullptr};
	for (idx_t i = 0; i < COLS; i++) {
		REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(src, i), &types[i], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_str names[COLS] = {V2Str("lst"), V2Str("strct"), V2Str("mp"), V2Str("un")};

	ArrowSchema schema {};
	ArrowArray array {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_array = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;

	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, COLS, &schema, e);
		c_array = duckdb_v2_data_chunk_to_arrow_array(ctx, src, &array, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &dst, e);
	});

	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_array == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(dst != nullptr);
	REQUIRE(schema.n_children == 4);

	REQUIRE(ChunkSize(dst) == 3);
	REQUIRE(VectorTypeId(ChunkVector(dst, 0)) == DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	REQUIRE(VectorTypeId(ChunkVector(dst, 1)) == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);
	REQUIRE(VectorTypeId(ChunkVector(dst, 2)) == DUCKDB_V2_LOGICAL_TYPE_ID_MAP);
	REQUIRE(VectorTypeId(ChunkVector(dst, 3)) == DUCKDB_V2_LOGICAL_TYPE_ID_UNION);

	// LIST: each row is [i, i+1]; the flattened child holds 3 * 2 = 6 ints.
	{
		auto list_vec = ChunkVector(dst, 0);
		duckdb_v2_vector_handle child = nullptr;
		REQUIRE(duckdb_v2_vector_get_child(list_vec, 0, &child, nullptr) == DUCKDB_V2_ERROR_NONE);
		idx_t child_size = 0;
		REQUIRE(duckdb_v2_vector_get_size(child, &child_size, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(child_size == 6);
		REQUIRE(GetScalar<int64_t>(child, 0) == 0);
		REQUIRE(GetScalar<int64_t>(child, 1) == 1);
		REQUIRE(GetScalar<int64_t>(child, 5) == 3); // last element of [2, 3]
	}

	// STRUCT: field 0 (x) is INTEGER i; field 1 (y) is 's' || i.
	{
		auto struct_vec = ChunkVector(dst, 1);
		duckdb_v2_vector_handle fx_x = nullptr, fx_y = nullptr;
		REQUIRE(duckdb_v2_vector_get_child(struct_vec, 0, &fx_x, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_get_child(struct_vec, 1, &fx_y, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (idx_t row = 0; row < 3; row++) {
			REQUIRE(GetScalar<int64_t>(fx_x, row) == static_cast<int64_t>(row));
			REQUIRE(GetVarchar(fx_y, row) == ("s" + std::to_string(row)));
		}
	}

	// MAP: each row is {i: i*10}. V2 exposes child[0] = keys, child[1] = values,
	// each flattened across the 3 single-entry maps.
	{
		auto map_vec = ChunkVector(dst, 2);
		duckdb_v2_vector_handle keys = nullptr, values = nullptr;
		REQUIRE(duckdb_v2_vector_get_child(map_vec, 0, &keys, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_get_child(map_vec, 1, &values, nullptr) == DUCKDB_V2_ERROR_NONE);
		idx_t key_size = 0;
		REQUIRE(duckdb_v2_vector_get_size(keys, &key_size, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(key_size == 3);
		for (idx_t entry = 0; entry < 3; entry++) {
			REQUIRE(GetScalar<int64_t>(keys, entry) == static_cast<int64_t>(entry));
			REQUIRE(GetScalar<int64_t>(values, entry) == static_cast<int64_t>(entry * 10));
		}
	}

	// UNION: union_value(num := i) -> UNION(num BIGINT). V2 exposes child[0] =
	// tag, child[1..N] = members; the num member carries [0, 1, 2].
	{
		auto union_vec = ChunkVector(dst, 3);
		idx_t child_count = 0;
		REQUIRE(duckdb_v2_vector_get_child_count(union_vec, &child_count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(child_count == 2); // tag + one member
		duckdb_v2_vector_handle tag = nullptr, member = nullptr;
		REQUIRE(duckdb_v2_vector_get_child(union_vec, 0, &tag, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_vector_get_child(union_vec, 1, &member, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (idx_t row = 0; row < 3; row++) {
			REQUIRE(GetScalar<uint8_t>(tag, row) == 0); // single member -> tag 0 every row
			REQUIRE(GetScalar<int64_t>(member, row) == static_cast<int64_t>(row));
		}
	}

	REQUIRE(array.release == nullptr);
	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&dst);
	duckdb_v2_data_chunk_destroy(&src);
	for (idx_t i = 0; i < COLS; i++) {
		duckdb_v2_logical_type_destroy(&types[i]);
	}
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
	// A second connection supplies the import context, so it is free while the
	// stream still holds the first connection.
	duckdb_v2_connection_handle conn2 = nullptr;
	REQUIRE(duckdb_v2_connect(fx.db, &conn2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(fx.conn, "SELECT i::BIGINT AS v FROM range(5000) t(i)", &r) == DUCKDB_V2_ERROR_NONE);

	ArrowArrayStream stream {};
	// One array of all 5000 rows.
	REQUIRE(duckdb_v2_result_to_arrow_stream(&r, 100000, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);

	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	ArrowArray array {};
	REQUIRE(stream.get_next(&stream, &array) == 0);
	REQUIRE(array.release != nullptr);
	REQUIRE(array.length == 5000);

	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;
	auto rc = V2RunWithContext(conn2, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &dst, e);
	});

	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(dst != nullptr);
	// One chunk holds all 5000 rows, above STANDARD_VECTOR_SIZE.
	REQUIRE(ChunkSize(dst) == 5000);
	REQUIRE(GetScalar<int64_t>(ChunkVector(dst, 0), 0) == 0);
	REQUIRE(GetScalar<int64_t>(ChunkVector(dst, 0), 2500) == 2500);
	REQUIRE(GetScalar<int64_t>(ChunkVector(dst, 0), 4999) == 4999);
	REQUIRE(array.release == nullptr); // adopted by the chunk

	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&dst);
	stream.release(&stream);
	duckdb_v2_disconnect(&conn2);
}

// ===========================================================================
// NULL values survive both conversion directions (validity mask handling).
// ===========================================================================

TEST_CASE("V2 arrow: NULL values survive the round-trip", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	// Rows: NULL, 1, NULL, 3.
	auto src =
	    QueryOneChunk(fx.conn, "SELECT (CASE WHEN i % 2 = 0 THEN NULL ELSE i END)::INTEGER AS v FROM range(4) t(i)");
	REQUIRE(src != nullptr);
	REQUIRE(ChunkSize(src) == 4);

	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(src, 0), &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str name = V2Str("v");

	ArrowSchema schema {};
	ArrowArray array {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_array = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;

	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, &type, &name, 1, &schema, e);
		c_array = duckdb_v2_data_chunk_to_arrow_array(ctx, src, &array, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &dst, e);
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_array == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ChunkSize(dst) == 4);

	auto vec = ChunkVector(dst, 0);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(RowValid(view, ResolveRow(view, 0)));
	REQUIRE(RowValid(view, ResolveRow(view, 1)));
	REQUIRE_FALSE(RowValid(view, ResolveRow(view, 2)));
	REQUIRE(RowValid(view, ResolveRow(view, 3)));
	REQUIRE(GetScalar<int32_t>(vec, 1) == 1);
	REQUIRE(GetScalar<int32_t>(vec, 3) == 3);

	REQUIRE(array.release == nullptr);
	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&dst);
	duckdb_v2_data_chunk_destroy(&src);
	duckdb_v2_logical_type_destroy(&type);
}

// ===========================================================================
// ENUM schema round-trips both directions (DuckDB type -> Arrow schema ->
// conversion plan), carrying its dictionary.
// ===========================================================================

TEST_CASE("V2 arrow: ENUM schema round-trips both directions", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	V2ExecSQL(fx.conn, "CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok')");

	auto chunk = QueryOneChunk(fx.conn, "SELECT 'happy'::mood AS m");
	REQUIRE(chunk != nullptr);
	REQUIRE(VectorTypeId(ChunkVector(chunk, 0)) == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM);

	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(chunk, 0), &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str name = V2Str("m");

	ArrowSchema schema {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_conv = DUCKDB_V2_ERROR_API;
	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, &type, &name, 1, &schema, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	// ENUM is dictionary-encoded on the Arrow side, and round-trips back to a
	// conversion plan.
	REQUIRE(schema.n_children == 1);
	REQUIRE(schema.children[0]->dictionary != nullptr);
	REQUIRE(plan != nullptr);

	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_logical_type_destroy(&type);
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
// An array whose child count disagrees with the conversion plan is rejected
// (rather than indexing out of bounds).
// ===========================================================================

TEST_CASE("V2 arrow: array/schema layout mismatch is rejected", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	auto two_col = QueryOneChunk(fx.conn, "SELECT i::INTEGER AS a, i::INTEGER AS b FROM range(3) t(i)");
	auto one_col = QueryOneChunk(fx.conn, "SELECT i::INTEGER AS a FROM range(3) t(i)");
	REQUIRE(two_col != nullptr);
	REQUIRE(one_col != nullptr);

	duckdb_v2_logical_type_handle types[2] = {nullptr, nullptr};
	for (idx_t i = 0; i < 2; i++) {
		REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(two_col, i), &types[i], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_str names[2] = {V2Str("a"), V2Str("b")};

	ArrowSchema schema2 {};
	ArrowArray array1 {};
	duckdb_v2_arrow_conversion_plan_handle conv2 = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_conv = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_array = DUCKDB_V2_ERROR_API, c_mismatch = DUCKDB_V2_ERROR_NONE;
	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, 2, &schema2, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema2, &conv2, e);
		c_array = duckdb_v2_data_chunk_to_arrow_array(ctx, one_col, &array1, e);
		// 1-child array against a 2-column conversion plan.
		c_mismatch = duckdb_v2_arrow_array_to_data_chunk(ctx, &array1, conv2, &dst, e);
	});
	(void)rc;
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_array == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_mismatch == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(dst == nullptr);

	// The mismatch is detected before ownership transfer, so array1 is intact.
	if (array1.release) {
		array1.release(&array1);
	}
	if (schema2.release) {
		schema2.release(&schema2);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&conv2);
	duckdb_v2_data_chunk_destroy(&two_col);
	duckdb_v2_data_chunk_destroy(&one_col);
	for (idx_t i = 0; i < 2; i++) {
		duckdb_v2_logical_type_destroy(&types[i]);
	}
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
// Multi-column import where the first column is an Arrow type extension with an
// arrow_to_duckdb cast (BOOLEAN under arrow_lossless_conversion -> the bool8
// extension). That import path is the ONLY one that does NOT capture the shared
// array into the output vector (it early-returns through the cast), so under a
// per-column owner column 0's wrapper drops at the end of its iteration and
// frees the parent array that the trailing zero-copy INTEGER column aliases (the
// C1 use-after-free). The single shared owner keeps the parent alive for the
// whole loop. (Run under ASAN to catch a regression.)
// ===========================================================================

TEST_CASE("V2 arrow: multi-column import with a leading extension-cast column", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	// Lossless conversion routes BOOLEAN through the bool8 extension (int8 + an
	// arrow_to_duckdb cast), the capture-skipping import path.
	V2ExecSQL(fx.conn, "SET arrow_lossless_conversion=true");

	auto src = QueryOneChunk(fx.conn, "SELECT (i % 2 = 0) AS b, i::INTEGER AS n FROM range(5) t(i)");
	REQUIRE(src != nullptr);
	REQUIRE(ChunkSize(src) == 5);

	duckdb_v2_logical_type_handle types[2] = {nullptr, nullptr};
	for (idx_t i = 0; i < 2; i++) {
		REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(src, i), &types[i], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_str names[2] = {V2Str("b"), V2Str("n")};

	ArrowSchema schema {};
	ArrowArray array {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_array = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;
	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, 2, &schema, e);
		c_array = duckdb_v2_data_chunk_to_arrow_array(ctx, src, &array, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &array, plan, &dst, e);
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_array == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ChunkSize(dst) == 5);
	// Read BOTH columns: column 1 would dangle under the old per-column owner.
	for (idx_t row = 0; row < 5; row++) {
		REQUIRE(GetScalar<bool>(ChunkVector(dst, 0), row) == (row % 2 == 0));
		REQUIRE(GetScalar<int32_t>(ChunkVector(dst, 1), row) == static_cast<int32_t>(row));
	}

	REQUIRE(array.release == nullptr);
	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&dst);
	duckdb_v2_data_chunk_destroy(&src);
	for (idx_t i = 0; i < 2; i++) {
		duckdb_v2_logical_type_destroy(&types[i]);
	}
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
// A 0-column conversion plan imports a childless array into an empty chunk
// (the ColumnCount()==0 path is handled by the shared owner dropping at return).
// ===========================================================================

TEST_CASE("V2 arrow: a 0-column import yields an empty chunk", "[capi_v2][arrow]") {
	V2EnvFixture fx;
	ArrowSchema schema {};
	duckdb_v2_arrow_conversion_plan_handle conv = nullptr;
	duckdb_v2_data_chunk_handle dst = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_conv = DUCKDB_V2_ERROR_API, c_chunk = DUCKDB_V2_ERROR_API;
	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, nullptr, nullptr, 0, &schema, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &conv, e);
		ArrowArray empty {};
		empty.length = 0;
		empty.n_children = 0;
		empty.release = NoopArrayRelease;
		c_chunk = duckdb_v2_arrow_array_to_data_chunk(ctx, &empty, conv, &dst, e);
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_chunk == DUCKDB_V2_ERROR_NONE);
	REQUIRE(dst != nullptr);
	REQUIRE(ChunkSize(dst) == 0);

	if (schema.release) {
		schema.release(&schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&conv);
	duckdb_v2_data_chunk_destroy(&dst);
}

// ===========================================================================
// A malformed foreign array is rejected with INVALID_INPUT (not a crash inside
// the engine): null children, a null child, a released child, a child whose
// length disagrees, and a dictionary-encoded column with no dictionary.
// ===========================================================================

TEST_CASE("V2 arrow: malformed import arrays are rejected", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	auto int_chunk = QueryOneChunk(fx.conn, "SELECT 0::INTEGER AS n");
	REQUIRE(int_chunk != nullptr);
	duckdb_v2_logical_type_handle int_type = nullptr;
	REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(int_chunk, 0), &int_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	V2ExecSQL(fx.conn, "CREATE TYPE mood AS ENUM ('a', 'b')");
	auto enum_chunk = QueryOneChunk(fx.conn, "SELECT 'a'::mood AS m");
	REQUIRE(enum_chunk != nullptr);
	duckdb_v2_logical_type_handle enum_type = nullptr;
	REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(enum_chunk, 0), &enum_type, nullptr) == DUCKDB_V2_ERROR_NONE);

	ArrowSchema int_schema {}, enum_schema {};
	duckdb_v2_arrow_conversion_plan_handle int_conv = nullptr, enum_conv = nullptr;
	duckdb_v2_str int_name = V2Str("n"), enum_name = V2Str("m");
	DUCKDB_V2_ERROR c_int_s = DUCKDB_V2_ERROR_API, c_int_c = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_enum_s = DUCKDB_V2_ERROR_API, c_enum_c = DUCKDB_V2_ERROR_API;
	DUCKDB_V2_ERROR c_null_children = DUCKDB_V2_ERROR_NONE, c_null_child = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR c_released = DUCKDB_V2_ERROR_NONE, c_length = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR c_dict = DUCKDB_V2_ERROR_NONE;

	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_int_s = duckdb_v2_logical_types_to_arrow_schema(ctx, &int_type, &int_name, 1, &int_schema, e);
		c_int_c = duckdb_v2_arrow_conversion_plan_create(ctx, &int_schema, &int_conv, e);
		c_enum_s = duckdb_v2_logical_types_to_arrow_schema(ctx, &enum_type, &enum_name, 1, &enum_schema, e);
		c_enum_c = duckdb_v2_arrow_conversion_plan_create(ctx, &enum_schema, &enum_conv, e);

		duckdb_v2_data_chunk_handle dst = nullptr;

		// 1. n_children matches but children pointer is null.
		ArrowArray a1 {};
		a1.length = 3;
		a1.n_children = 1;
		a1.children = nullptr;
		a1.release = NoopArrayRelease;
		c_null_children = duckdb_v2_arrow_array_to_data_chunk(ctx, &a1, int_conv, &dst, e);

		// 2. a null child.
		ArrowArray *kids2[1] = {nullptr};
		ArrowArray a2 {};
		a2.length = 3;
		a2.n_children = 1;
		a2.children = kids2;
		a2.release = NoopArrayRelease;
		c_null_child = duckdb_v2_arrow_array_to_data_chunk(ctx, &a2, int_conv, &dst, e);

		// 3. a child with a null (released) release.
		ArrowArray child3 {};
		child3.length = 3;
		child3.release = nullptr;
		ArrowArray *kids3[1] = {&child3};
		ArrowArray a3 {};
		a3.length = 3;
		a3.n_children = 1;
		a3.children = kids3;
		a3.release = NoopArrayRelease;
		c_released = duckdb_v2_arrow_array_to_data_chunk(ctx, &a3, int_conv, &dst, e);

		// 4. a child whose length disagrees with the array length.
		ArrowArray child4 {};
		child4.length = 2;
		child4.release = NoopArrayRelease;
		ArrowArray *kids4[1] = {&child4};
		ArrowArray a4 {};
		a4.length = 3;
		a4.n_children = 1;
		a4.children = kids4;
		a4.release = NoopArrayRelease;
		c_length = duckdb_v2_arrow_array_to_data_chunk(ctx, &a4, int_conv, &dst, e);

		// 5. a dictionary-encoded (ENUM) column with no dictionary.
		ArrowArray child5 {};
		child5.length = 3;
		child5.release = NoopArrayRelease;
		child5.dictionary = nullptr;
		ArrowArray *kids5[1] = {&child5};
		ArrowArray a5 {};
		a5.length = 3;
		a5.n_children = 1;
		a5.children = kids5;
		a5.release = NoopArrayRelease;
		c_dict = duckdb_v2_arrow_array_to_data_chunk(ctx, &a5, enum_conv, &dst, e);
	});

	(void)rc;
	REQUIRE(c_int_s == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_int_c == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_enum_s == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_enum_c == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_null_children == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(c_null_child == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(c_released == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(c_length == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(c_dict == DUCKDB_V2_ERROR_INPUT_INVALID);

	if (int_schema.release) {
		int_schema.release(&int_schema);
	}
	if (enum_schema.release) {
		enum_schema.release(&enum_schema);
	}
	duckdb_v2_arrow_conversion_plan_destroy(&int_conv);
	duckdb_v2_arrow_conversion_plan_destroy(&enum_conv);
	duckdb_v2_logical_type_destroy(&int_type);
	duckdb_v2_logical_type_destroy(&enum_type);
	duckdb_v2_data_chunk_destroy(&int_chunk);
	duckdb_v2_data_chunk_destroy(&enum_chunk);
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

	// Import with a null/empty conversion plan and null array.
	duckdb_v2_data_chunk_handle dst = nullptr;
	duckdb_v2_arrow_conversion_plan_handle empty = nullptr;
	DUCKDB_V2_ERROR c_null_array = DUCKDB_V2_ERROR_NONE, c_null_conv = DUCKDB_V2_ERROR_NONE;
	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_null_array = duckdb_v2_arrow_array_to_data_chunk(ctx, nullptr, empty, &dst, e);
		ArrowArray dummy {};
		c_null_conv = duckdb_v2_arrow_array_to_data_chunk(ctx, &dummy, nullptr, &dst, e);
	});
	// The callback itself does not fail; the conversions report INVALID_INPUT
	// on the err slot, which the bridge surfaces as a callback error.
	(void)rc;
	REQUIRE(c_null_array == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(c_null_conv == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Destroy is null-safe.
	REQUIRE(duckdb_v2_arrow_conversion_plan_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_arrow_conversion_plan_handle null_handle = nullptr;
	REQUIRE(duckdb_v2_arrow_conversion_plan_destroy(&null_handle) == DUCKDB_V2_ERROR_NONE);
}

// ===========================================================================
// conversion_plan_get_schema: an arbitrary ArrowSchema becomes DuckDB
// (name, logical type) pairs without parsing Arrow format strings.
// ===========================================================================

TEST_CASE("V2 arrow: conversion plan exposes names and logical types", "[capi_v2][arrow]") {
	V2EnvFixture fx;

	// Nested + string + temporal kinds.
	auto src = QueryOneChunk(fx.conn, "SELECT 1::INTEGER AS i, 'x' AS s, "
	                                  "TIMESTAMP '2020-01-01 00:00:00' AS ts, "
	                                  "[1, 2] AS lst, {'x': 1} AS st");
	REQUIRE(src != nullptr);

	const idx_t COLS = 5;
	duckdb_v2_logical_type_handle types[COLS] = {nullptr, nullptr, nullptr, nullptr, nullptr};
	for (idx_t i = 0; i < COLS; i++) {
		REQUIRE(duckdb_v2_vector_get_logical_type(ChunkVector(src, i), &types[i], nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_str names[COLS] = {V2Str("i"), V2Str("s"), V2Str("ts"), V2Str("lst"), V2Str("st")};

	ArrowSchema schema {};
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	duckdb_v2_schema_handle out_schema = nullptr;
	DUCKDB_V2_ERROR c_schema = DUCKDB_V2_ERROR_API, c_conv = DUCKDB_V2_ERROR_API, c_get = DUCKDB_V2_ERROR_API;

	auto rc = V2RunWithContext(fx.conn, [&](duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *e) {
		c_schema = duckdb_v2_logical_types_to_arrow_schema(ctx, types, names, COLS, &schema, e);
		c_conv = duckdb_v2_arrow_conversion_plan_create(ctx, &schema, &plan, e);
		c_get = duckdb_v2_arrow_conversion_plan_get_schema(plan, &out_schema, e);
	});
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_schema == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_conv == DUCKDB_V2_ERROR_NONE);
	REQUIRE(c_get == DUCKDB_V2_ERROR_NONE);
	REQUIRE(out_schema != nullptr);

	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(out_schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == COLS);

	const char *expected_names[COLS] = {"i", "s", "ts", "lst", "st"};
	const DUCKDB_V2_LOGICAL_TYPE_ID expected_ids[COLS] = {
	    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
	    DUCKDB_V2_LOGICAL_TYPE_ID_LIST, DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT};
	for (idx_t i = 0; i < COLS; i++) {
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_logical_type_handle type = nullptr; // borrowed from out_schema
		REQUIRE(duckdb_v2_schema_get_field(out_schema, i, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(V2StrTo(name) == expected_names[i]);
		DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(id == expected_ids[i]);
	}

	// Null args are rejected.
	duckdb_v2_schema_handle misuse = nullptr;
	REQUIRE(duckdb_v2_arrow_conversion_plan_get_schema(nullptr, &misuse, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(misuse == nullptr);
	REQUIRE(duckdb_v2_arrow_conversion_plan_get_schema(plan, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_schema_destroy(&out_schema);
	REQUIRE(out_schema == nullptr);
	schema.release(&schema);
	duckdb_v2_arrow_conversion_plan_destroy(&plan);
	duckdb_v2_data_chunk_destroy(&src);
	for (idx_t i = 0; i < COLS; i++) {
		duckdb_v2_logical_type_destroy(&types[i]);
	}
}
