#pragma once

#include "catch.hpp"
#include "duckdb_v2.h"
#include "duckdb.h"

#include <cstring>
#include <string>

inline duckdb_v2_logical_type_handle V1ToV2(duckdb_logical_type t) {
	return reinterpret_cast<duckdb_v2_logical_type_handle>(t);
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

inline DUCKDB_V2_API_CALL_t V2ValueCreateVarchar(const char *data, idx_t len, duckdb_v2_value_handle *out_value,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb_v2_value_create_varchar(duckdb_v2_str {data, len}, out_value, err);
}
inline DUCKDB_V2_API_CALL_t V2VectorAssignString(duckdb_v2_vector_handle vec, idx_t index, const char *data, idx_t len,
                                                 duckdb_v2_error_info_handle *err) {
	return duckdb_v2_vector_assign_string(vec, index, duckdb_v2_str {data, len}, err);
}

inline idx_t SelAt(const duckdb_v2_sel_t *sel, idx_t i) {
	idx_t out = 0;
	REQUIRE(duckdb_v2_sel_at(sel, i, &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	return out;
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
// the string-taking equivalent of connection_query for tests
// (parse_sql + statement_iterator_next + connection_query). FAILs when
// the input contains more than one statement, so a stray semicolon in a
// test never truncates silently; multi-statement semantics have their
// own iterator tests. An input with no statements propagates as
// INVALID_INPUT from connection_query, via the NULL statement handle.
inline duckdb_v2_error_code_t V2Query(duckdb_v2_connection_handle conn, const char *sql,
                                      duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err = nullptr) {
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
		// Guard single-statement intent before running anything.
		duckdb_v2_sql_statement_handle extra = nullptr;
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &extra, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (extra) {
			duckdb_v2_sql_statement_destroy(&extra);
			duckdb_v2_sql_statement_destroy(&stmt);
			duckdb_v2_statement_iterator_destroy(&iter);
			FAIL("V2Query input contains more than one statement: " << sql);
		}
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		rc = duckdb_v2_connection_query(conn, &stmt, out_result, err);
	}
	// The statement is only left alive when connection_query refused
	// without consuming it (busy or null-arg).
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
	REQUIRE(duckdb_v2_connection_query_progress(conn, &progress, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(progress != nullptr);
	V2Progress out;
	REQUIRE(duckdb_v2_query_progress_get_percentage(progress, &out.percentage, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_query_progress_get_rows_processed(progress, &out.rows_processed, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_query_progress_get_total_rows_to_process(progress, &out.total_rows_to_process, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_query_progress_destroy(&progress) == DUCKDB_V2_ERROR_NONE);
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
