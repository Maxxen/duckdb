#pragma once

#include "catch.hpp"
#include "duckdb_v2.h"
#include "duckdb.h"

inline duckdb_v2_logical_type_ptr V1ToV2(duckdb_logical_type t) {
	return static_cast<duckdb_v2_logical_type_ptr>(t);
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
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_connection_ptr conn = nullptr;
	V2EnvFixture() {
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);
		duckdb_v2_connect(db, &conn, nullptr);
	}
	~V2EnvFixture() {
		duckdb_v2_disconnect(&conn);
		duckdb_v2_close(&db);
		duckdb_v2_destroy_environment(&env);
	}
};
