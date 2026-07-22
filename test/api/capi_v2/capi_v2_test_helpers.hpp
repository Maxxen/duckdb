#pragma once

#include "catch.hpp"
#include "duckdb_v2.h"
#include "duckdb.h"

#include <cstring>
#include <string>
#include <type_traits>
#include <utility>

inline duckdb_v2_logical_type_handle V1ToV2(duckdb_logical_type t) {
	return reinterpret_cast<duckdb_v2_logical_type_handle>(t);
}

// Owned V2-native primitive type fixture; destroy via logical_type_destroy.
// Composites go through V2CreateType and its per-kind sugar below, which
// build via logical_type_create in a context scope.
inline duckdb_v2_logical_type_handle V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID id) {
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(id, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
	return t;
}

// Runs fn(ctx) inside the connection's context scope: the external path to a
// duckdb_v2_context_handle. The scope call itself must succeed; failures of
// calls made inside fn are asserted by fn.
template <class FN>
inline void V2WithContext(duckdb_v2_connection_handle conn, FN &&fn) {
	using Fn = typename std::decay<FN>::type;
	Fn body = std::forward<FN>(fn);
	auto trampoline = [](duckdb_v2_context_handle ctx, void *user_data, duckdb_v2_error_info_handle *) {
		(*static_cast<Fn *>(user_data))(ctx);
	};
	REQUIRE(duckdb_v2_connection_execute_with_context(conn, trampoline, &body, nullptr) == DUCKDB_V2_ERROR_NONE);
}

// Build a borrowed string view from a null-terminated C string. A null
// pointer yields the empty view {NULL, 0}.
inline duckdb_v2_str V2Str(const char *s) {
	return duckdb_v2_str {s, s ? std::strlen(s) : 0};
}
inline duckdb_v2_str V2Str(const std::string &s) {
	return duckdb_v2_str {s.data(), s.size()};
}
// Materialize a borrowed view as a std::string for comparison/printing.
inline std::string V2StrTo(duckdb_v2_str s) {
	return s.ptr ? std::string(s.ptr, s.len) : std::string();
}

// Content comparison of a borrowed view against a std::string / C literal.
// Used directly in REQUIRE(...) so getters can return a duckdb_v2_str and be
// asserted without unpacking. An empty view ({NULL, 0}) compares equal to "".
inline bool operator==(duckdb_v2_str a, const std::string &b) {
	return a.len == b.size() && (a.len == 0 || std::memcmp(a.ptr, b.data(), a.len) == 0);
}
inline bool operator==(const std::string &a, duckdb_v2_str b) {
	return b == a;
}
inline bool operator==(duckdb_v2_str a, const char *b) {
	return a == std::string(b ? b : "");
}
inline bool operator==(const char *a, duckdb_v2_str b) {
	return b == std::string(a ? a : "");
}
inline bool operator!=(duckdb_v2_str a, const std::string &b) {
	return !(a == b);
}
inline bool operator!=(const std::string &a, duckdb_v2_str b) {
	return !(b == a);
}
inline bool operator!=(duckdb_v2_str a, const char *b) {
	return !(a == b);
}
inline bool operator!=(const char *a, duckdb_v2_str b) {
	return !(b == a);
}

// Leaf-value helpers over the generic payload codec (the per-kind value
// constructors and getters are gone; the committed physical layout is the
// contract). All REQUIRE success; error paths are tested directly.
inline duckdb_v2_value_handle V2LeafValue(DUCKDB_V2_LOGICAL_TYPE_ID id, const void *data, idx_t len) {
	duckdb_v2_logical_type_handle type = nullptr;
	duckdb_v2_logical_type_create_from_id(id, &type, nullptr);
	duckdb_v2_value_handle value = nullptr;
	auto rc = duckdb_v2_value_create_from_data(type, data, len, &value, nullptr);
	duckdb_v2_logical_type_destroy(&type);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(value != nullptr);
	return value;
}
template <class T>
inline duckdb_v2_value_handle V2LeafValue(DUCKDB_V2_LOGICAL_TYPE_ID id, T payload) {
	return V2LeafValue(id, &payload, sizeof(T));
}
inline duckdb_v2_value_handle V2Int32Value(int32_t payload) {
	return V2LeafValue(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, payload);
}
inline duckdb_v2_value_handle V2Int64Value(int64_t payload) {
	return V2LeafValue(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, payload);
}
inline duckdb_v2_value_handle V2VarcharValue(const char *s) {
	return V2LeafValue(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, s, s ? std::strlen(s) : 0);
}
// Fixed-size payload read; REQUIREs the committed layout size.
template <class T>
inline T V2LeafPayload(duckdb_v2_value_handle value) {
	const void *data = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_data(value, &data, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(len == sizeof(T));
	T out;
	std::memcpy(&out, data, sizeof(T));
	return out;
}
// Wire-bytes payload read as a std::string (VARCHAR / BLOB / BIT).
inline std::string V2LeafBytes(duckdb_v2_value_handle value) {
	const void *data = nullptr;
	idx_t len = 0;
	REQUIRE(duckdb_v2_value_get_data(value, &data, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
	return len ? std::string(static_cast<const char *>(data), len) : std::string();
}
// Consuming forms: read, destroy the owned value, then assert, so a failing
// REQUIRE cannot leak it.
template <class T>
inline T V2LeafPayloadConsume(duckdb_v2_value_handle &value) {
	const void *data = nullptr;
	idx_t len = 0;
	auto rc = duckdb_v2_value_get_data(value, &data, &len, nullptr);
	T out {};
	const bool size_ok = (len == sizeof(T));
	if (rc == DUCKDB_V2_ERROR_NONE && size_ok) {
		std::memcpy(&out, data, sizeof(T));
	}
	duckdb_v2_value_destroy(&value);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size_ok);
	return out;
}
inline std::string V2LeafBytesConsume(duckdb_v2_value_handle &value) {
	const void *data = nullptr;
	idx_t len = 0;
	auto rc = duckdb_v2_value_get_data(value, &data, &len, nullptr);
	std::string out =
	    (rc == DUCKDB_V2_ERROR_NONE && len) ? std::string(static_cast<const char *>(data), len) : std::string();
	duckdb_v2_value_destroy(&value);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return out;
}
// Assemble a non-inlined duckdb_v2_string over already-written heap bytes (no
// allocation, no payload copy): the single hand-assembler for write-in-place flows.
inline duckdb_v2_string V2StringFromHeapBytes(uint8_t *bytes, idx_t len) {
	duckdb_v2_string storage {};
	storage.value.pointer.length = static_cast<uint32_t>(len);
	storage.value.pointer.ptr = reinterpret_cast<char *>(bytes);
	std::memcpy(storage.value.pointer.prefix, bytes, 4);
	return storage;
}

// Assemble a duckdb_v2_string from raw bytes, using string_heap_allocate for the
// non-inlined path. Mirrors the C++ StringHeap::Add. `rc` gets the allocate result.
inline duckdb_v2_string V2MakeString(duckdb_v2_string_heap_handle heap, const char *data, idx_t len,
                                     DUCKDB_V2_ERROR &rc, duckdb_v2_error_info_handle *err) {
	duckdb_v2_string storage {};
	rc = DUCKDB_V2_ERROR_NONE;
	if (len <= DUCKDB_V2_STRING_INLINE_LENGTH) {
		storage.value.inlined.length = static_cast<uint32_t>(len);
		if (len > 0) {
			std::memcpy(storage.value.inlined.inlined, data, len);
		}
		return storage;
	}
	uint8_t *bytes = nullptr;
	rc = duckdb_v2_string_heap_allocate(heap, len, &bytes, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return storage;
	}
	std::memcpy(bytes, data, len);
	return V2StringFromHeapBytes(bytes, len);
}

// Borrow the heap, assemble the value, place it in slot `index`. Mirrors the C++
// Vector::AssignString; used by the many tests that need one string in a slot.
inline DUCKDB_V2_ERROR V2VectorAssignString(duckdb_v2_vector_handle vec, idx_t index, const char *data, idx_t len,
                                            duckdb_v2_error_info_handle *err) {
	duckdb_v2_string_heap_handle heap = nullptr;
	auto rc = duckdb_v2_vector_get_string_heap(vec, &heap, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	auto storage = V2MakeString(heap, data, len, rc, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	void *raw = nullptr;
	rc = duckdb_v2_vector_get_data_mutable(vec, &raw, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	static_cast<duckdb_v2_string *>(raw)[index] = storage;
	return DUCKDB_V2_ERROR_NONE;
}

// Resolve a logical row through a selection vector. The inline expression
// `sel ? sel[i] : i` is the documented V2 contract; there is no bridge call.
inline idx_t SelAt(const duckdb_v2_sel_t *sel, idx_t i) {
	return sel ? static_cast<idx_t>(sel[i]) : i;
}

// Read a transparent duckdb_v2_string as a borrowed view (the C++ suite
// keeps its own SlotBytes; this header includes V1). The length field
// shares offset 0 across both union arms; the data is inlined or heap-backed
// by the DUCKDB_V2_STRING_INLINE_LENGTH cutoff.
inline duckdb_v2_str V2StringView(const duckdb_v2_string &s) {
	uint32_t len = s.value.inlined.length;
	const char *ptr = len <= DUCKDB_V2_STRING_INLINE_LENGTH ? s.value.inlined.inlined : s.value.pointer.ptr;
	return duckdb_v2_str {ptr, len};
}

// TYPE value wrapping a borrowed type handle.
inline duckdb_v2_value_handle V2TypeValueOf(duckdb_v2_logical_type_handle t) {
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_value_create_type(t, &v, nullptr) == DUCKDB_V2_ERROR_NONE);
	return v;
}

// TYPE value wrapping an owned primitive built from `id`.
inline duckdb_v2_value_handle V2TypeValueOfId(DUCKDB_V2_LOGICAL_TYPE_ID id) {
	duckdb_v2_logical_type_handle t = nullptr;
	duckdb_v2_logical_type_create_from_id(id, &t, nullptr);
	duckdb_v2_value_handle v = nullptr;
	auto rc = duckdb_v2_value_create_type(t, &v, nullptr);
	duckdb_v2_logical_type_destroy(&t);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	return v;
}

// Runs logical_type_create inside a context scope. Destroys the borrowed
// param values. `names` entries may be nullptr (positional); pass nullptr
// for all-positional.
inline duckdb_v2_logical_type_handle V2CreateType(duckdb_v2_connection_handle conn, const char *name,
                                                  const std::vector<const char *> *names,
                                                  std::vector<duckdb_v2_value_handle> values) {
	std::vector<duckdb_v2_str> name_views;
	if (names) {
		for (auto *n : *names) {
			name_views.push_back(V2Str(n));
		}
	}
	duckdb_v2_logical_type_handle t = nullptr;
	DUCKDB_V2_ERROR rc = DUCKDB_V2_ERROR_NONE;
	V2WithContext(conn, [&](duckdb_v2_context_handle ctx) {
		rc = duckdb_v2_logical_type_create(ctx, V2Str(name), names ? name_views.data() : nullptr,
		                                   values.empty() ? nullptr : values.data(), values.size(), &t, nullptr);
	});
	for (auto &v : values) {
		duckdb_v2_value_destroy(&v);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(t != nullptr);
	return t;
}

// Per-kind sugar over V2CreateType for the common composite fixtures.
inline duckdb_v2_logical_type_handle V2ListType(duckdb_v2_connection_handle conn, DUCKDB_V2_LOGICAL_TYPE_ID elem) {
	return V2CreateType(conn, "list", nullptr, {V2TypeValueOfId(elem)});
}
inline duckdb_v2_logical_type_handle V2MapType(duckdb_v2_connection_handle conn, DUCKDB_V2_LOGICAL_TYPE_ID key,
                                               DUCKDB_V2_LOGICAL_TYPE_ID value) {
	return V2CreateType(conn, "map", nullptr, {V2TypeValueOfId(key), V2TypeValueOfId(value)});
}
inline duckdb_v2_logical_type_handle V2StructType(duckdb_v2_connection_handle conn,
                                                  const std::vector<const char *> &names,
                                                  const std::vector<DUCKDB_V2_LOGICAL_TYPE_ID> &ids) {
	std::vector<duckdb_v2_value_handle> values;
	for (auto id : ids) {
		values.push_back(V2TypeValueOfId(id));
	}
	return V2CreateType(conn, "struct", &names, std::move(values));
}

// True if row `idx` of a vector view is non-NULL. A null validity pointer means
// "all valid"; otherwise the validity bit at `idx` decides. `idx` must already
// be resolved through the selection vector.
inline bool RowValid(const duckdb_v2_vector_view &view, idx_t idx) {
	if (!view.validity) {
		return true;
	}
	return (view.validity[idx / 64] & (uint64_t(1) << (idx % 64))) != 0;
}

// Parses a single-statement SQL string and starts it on the connection:
// the string-taking equivalent of statement_execute for tests
// (parse_sql + statement_iterator_next + statement_execute). FAILs when
// the input contains more than one statement, so a stray semicolon in a
// test never truncates silently; multi-statement semantics have their
// own iterator tests. An input with no statements propagates as
// INVALID_INPUT from statement_execute, via the NULL statement handle.
inline DUCKDB_V2_ERROR V2Query(duckdb_v2_connection_handle conn, const char *sql, duckdb_v2_result_handle *out_result,
                               duckdb_v2_error_info_handle *err = nullptr) {
	if (out_result) {
		*out_result = nullptr;
	}
	duckdb_v2_statement_iterator_handle iter = nullptr;
	auto rc = duckdb_v2_parse_sql(conn, sql, &iter, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	duckdb_v2_sql_statement_handle stmt = nullptr;
	rc = duckdb_v2_statement_iterator_next(iter, &stmt, err);
	if (rc == DUCKDB_V2_ERROR_NONE && stmt) {
		// Guard single-statement intent before running anything. Destroy the
		// fixtures before any REQUIRE/FAIL so an assertion failure cannot leak.
		duckdb_v2_sql_statement_handle extra = nullptr;
		auto extra_rc = duckdb_v2_statement_iterator_next(iter, &extra, nullptr);
		if (extra_rc != DUCKDB_V2_ERROR_NONE || extra) {
			duckdb_v2_sql_statement_destroy(&extra);
			duckdb_v2_sql_statement_destroy(&stmt);
			duckdb_v2_statement_iterator_destroy(&iter);
			REQUIRE(extra_rc == DUCKDB_V2_ERROR_NONE);
			FAIL("V2Query input contains more than one statement: " << sql);
		}
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_statement_execute(conn, stmt, nullptr, nullptr, 0, out_result, err);
	}
	// statement_execute is non-consuming: the statement is always still alive
	// (it executes a copy), so destroy it unconditionally.
	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);
	return rc;
}

// Drains the next chunk out of a streaming result via the step primitive
// (step, waiting between steps, until CHUNK or a terminal state). Returns a
// caller-owned chunk, or nullptr at end-of-stream. Tests prefer this step
// loop over result_fetch_chunk so the primitive surface gets the coverage;
// the convenience functions have their own focused tests.
inline duckdb_v2_data_chunk_handle V2StepChunk(duckdb_v2_result_handle r) {
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		REQUIRE(duckdb_v2_result_step(r, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		switch (status) {
		case DUCKDB_V2_RESULT_STEP_STATUS_CHUNK:
			REQUIRE(chunk != nullptr);
			return chunk;
		case DUCKDB_V2_RESULT_STEP_STATUS_FINISHED:
			REQUIRE(chunk == nullptr);
			return nullptr;
		case DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
			FAIL("unexpected CANCELLED status while draining a result");
			return nullptr;
		case DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
			REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
			break;
		default:
			break;
		}
	}
}

// Drains a result to exhaustion via the step primitive, destroying each
// chunk; returns the total row count.
inline idx_t V2DrainRowCount(duckdb_v2_result_handle r) {
	idx_t total = 0;
	while (auto chunk = V2StepChunk(r)) {
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		duckdb_v2_data_chunk_destroy(&chunk);
		total += size;
	}
	return total;
}

// Executes a side-effecting statement (DDL, DML, SET, ...) to completion:
// query + drain + destroy. Streaming execution is lazy, so a statement
// only takes effect once its result is stepped; setup statements must be
// drained, not just destroyed.
inline void V2ExecSQL(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(V2Query(conn, sql, &r) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(r);
	duckdb_v2_result_destroy(&r);
}

// A result's output column count, read through its schema.
inline idx_t V2ColumnCount(duckdb_v2_result_handle r) {
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_schema_destroy(&schema);
	return count;
}

// Asserts result column `index` has the given name and type id, via the result
// schema; fetches, borrows, and destroys so call sites stay one line.
inline void V2RequireColumn(duckdb_v2_result_handle r, idx_t index, const char *name, DUCKDB_V2_LOGICAL_TYPE_ID id) {
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str field_name = {nullptr, 0};
	duckdb_v2_logical_type_handle field_type = nullptr; // borrowed; do not destroy
	REQUIRE(duckdb_v2_schema_get_field(schema, index, &field_name, &field_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(field_name.ptr ? field_name.ptr : "", field_name.len) == name);
	DUCKDB_V2_LOGICAL_TYPE_ID got = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(field_type, &got, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(got == id);
	duckdb_v2_schema_destroy(&schema);
}

// Asserts a result's schema is not yet available: the statement expanded to a
// group whose row-producing fragment is not yet prepared, so result_get_schema
// reports INVALID_INPUT.
inline void V2RequireSchemaDeferred(duckdb_v2_result_handle r) {
	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(schema == nullptr);
}

// Reads the affected-row count of a CHANGED_ROWS result by draining its
// single-row BIGINT Count chunk. Returns -1 if the stream yields no chunk.
inline int64_t V2DrainChangedRows(duckdb_v2_result_handle r) {
	auto chunk = V2StepChunk(r);
	if (!chunk) {
		return -1;
	}
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	int64_t count = view.data ? reinterpret_cast<const int64_t *>(view.data)[0] : -1;
	duckdb_v2_data_chunk_destroy(&chunk);
	// The Count chunk is the stream's only payload; pin end-of-stream.
	auto trailing = V2StepChunk(r);
	if (trailing) {
		duckdb_v2_data_chunk_destroy(&trailing);
		FAIL("CHANGED_ROWS stream yielded more than one chunk");
	}
	return count;
}

// Reads a progress snapshot through the query_progress object: capture,
// read all three accessors, destroy.
struct V2Progress {
	double percentage = 99.0;
	uint64_t rows_processed = 99;
	uint64_t total_rows_to_process = 99;
};
inline V2Progress V2ReadProgress(duckdb_v2_connection_handle conn) {
	duckdb_v2_query_progress_handle progress = nullptr;
	auto capture_rc = duckdb_v2_connection_query_progress(conn, &progress, nullptr);
	REQUIRE(capture_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(progress != nullptr);
	// Read all three accessors, destroy, then assert: a failing REQUIRE
	// between capture and destroy would leak the snapshot.
	V2Progress out;
	auto pct_rc = duckdb_v2_query_progress_get_percentage(progress, &out.percentage, nullptr);
	auto rows_rc = duckdb_v2_query_progress_get_rows_processed(progress, &out.rows_processed, nullptr);
	auto total_rc = duckdb_v2_query_progress_get_total_rows_to_process(progress, &out.total_rows_to_process, nullptr);
	auto destroy_rc = duckdb_v2_query_progress_destroy(&progress);
	REQUIRE(pct_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(total_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroy_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(progress == nullptr);
	return out;
}

// RAII fixture: in-memory environment + database + connection.
struct V2EnvFixture {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	V2EnvFixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~V2EnvFixture() {
		duckdb_v2_disconnect(&conn);
		duckdb_v2_close(&db);
		duckdb_v2_destroy_environment(&env);
	}
};

// RAII result handle: destroyed on scope exit, so a REQUIRE that throws
// mid-test does not leak it. Drop-in for a duckdb_v2_result_handle local.
struct V2Result {
	duckdb_v2_result_handle handle = nullptr;
	V2Result() = default;
	V2Result(const V2Result &) = delete;
	V2Result &operator=(const V2Result &) = delete;
	~V2Result() {
		duckdb_v2_result_destroy(&handle);
	}
	duckdb_v2_result_handle *operator&() {
		return &handle;
	}
	operator duckdb_v2_result_handle() const {
		return handle;
	}
};
