//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geometry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Geometry Type
enum class GeometryType : uint8_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

//! The Geometry class is a static class that holds helper function for the GEOMETRY type.
class Geometry {
public:
	static bool FromString(const string &str, string_t &result, Vector &result_vector, bool strict);
	static string_t ToString(Vector &result, const char *buf, idx_t len);
};

} // namespace duckdb
