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

struct Box2D {
	double min_x;
	double min_y;
	double max_x;
	double max_y;

	static Box2D Empty() {
		return {NumericLimits<double>::Maximum(), NumericLimits<double>::Maximum(), NumericLimits<double>::Minimum(),
		        NumericLimits<double>::Minimum()};
	}

	void Extend(const Box2D &other) {
		min_x = MinValue(min_x, other.min_x);
		min_y = MinValue(min_y, other.min_y);
		max_x = MaxValue(max_x, other.max_x);
		max_y = MaxValue(max_y, other.max_y);
	}

	bool Intersects(const Box2D &other) const {
		return !(min_x > other.max_x || max_x < other.min_x || min_y > other.max_y || max_y < other.min_y);
	}
};

//! The Geometry class is a static class that holds helper function for the GEOMETRY type.
class Geometry {
public:
	static bool FromString(const string &str, string_t &result, Vector &result_vector, bool strict);
	static string_t ToString(Vector &result, const char *buf, idx_t len);
	static idx_t GetBounds(const string_t &blob, Box2D &result);
};

} // namespace duckdb
