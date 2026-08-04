#pragma once

//----------------------------------------------------------------------------------------------------------------------
// Common headers for all C-API-V2 tests
//----------------------------------------------------------------------------------------------------------------------

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb_v2.h"

//----------------------------------------------------------------------------------------------------------------------
// Helpers
//----------------------------------------------------------------------------------------------------------------------

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
