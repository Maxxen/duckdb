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
