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
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"

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

// Vertex Type
enum class VertexType : uint8_t {
	XY = 0,  // 2D point
	XYZ = 1, // 3D point
	XYM = 2, // 2D point with measure
	XYZM = 3 // 3D point with measure
};

struct GeometryExtent {
	double min_x;
	double max_x;
	double min_y;
	double max_y;
	double min_z;
	double max_z;
	double min_m;
	double max_m;

	static GeometryExtent Empty() {
		constexpr auto min = NumericLimits<double>::Maximum();
		constexpr auto max = NumericLimits<double>::Minimum();
		return {min, max, min, max, min, max, min, max};
	}

	static GeometryExtent Unknown() {
		constexpr auto min = NumericLimits<double>::Minimum();
		constexpr auto max = NumericLimits<double>::Maximum();
		return {min, max, min, max, min, max, min, max};
	}

	void Extend(const double &x, const double &y) {
		max_x = MaxValue(max_x, x);
		min_x = MinValue(min_x, x);
		max_y = MaxValue(max_y, y);
		min_y = MinValue(min_y, y);
	}

	void Extend(const GeometryExtent &other) {
		min_x = MinValue(min_x, other.min_x);
		max_x = MaxValue(max_x, other.max_x);
		min_y = MinValue(min_y, other.min_y);
		max_y = MaxValue(max_y, other.max_y);
		min_z = MinValue(min_z, other.min_z);
		max_z = MaxValue(max_z, other.max_z);
		min_m = MinValue(min_m, other.min_m);
		max_m = MaxValue(max_m, other.max_m);
	}

	// 2D intersection check
	bool Intersects(const GeometryExtent &other) const {
		return !(min_x > other.max_x || max_x < other.min_x || min_y > other.max_y || max_y < other.min_y);
	}
};

//! The Geometry class is a static class that holds helper function for the GEOMETRY type.
class Geometry {
public:
	static constexpr auto MAX_RECURSION_DEPTH = 16;

	static bool FromWKB(const string_t &wkb_blob, string_t &result, Vector &result_vector, bool strict);
	static bool FromString(const string &str, string_t &result, Vector &result_vector, bool strict);

	static string_t ToString(Vector &result, const char *buf, idx_t len);
	static string_t ToWKB(const string_t &geom, Vector &result);
	static idx_t GetExtent(const string_t &geom, GeometryExtent &result);
	static pair<GeometryType, VertexType> GetGeometryType(const string_t &geom);

	static void Verify(const string_t &blob);
};

} // namespace duckdb
