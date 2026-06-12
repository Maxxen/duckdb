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
// Convenience for the many test-setup queries: takes the SQL as a plain C
// string / std::string and forwards it through the duckdb_v2_str API.
inline DUCKDB_V2_API_CALL_t V2Query(duckdb_v2_connection_handle conn, const char *sql,
                                    duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_connection_query(conn, V2Str(sql), out_result, err);
}
inline DUCKDB_V2_API_CALL_t V2Query(duckdb_v2_connection_handle conn, const std::string &sql,
                                    duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_connection_query(conn, V2Str(sql), out_result, err);
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
