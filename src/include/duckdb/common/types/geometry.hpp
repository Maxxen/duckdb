//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class GeometryVertexType {
	XY = 0,
	XYZ = 1,
	XYM = 2,
	XYZM = 3,
};

enum class GeometryPartType {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

class GeometryType {
public:
	GeometryType() : part_type(GeometryPartType::INVALID), vert_type(GeometryVertexType::XY) {
	}

	GeometryType(const GeometryPartType part_type_p, const GeometryVertexType vert_type_p)
	    : part_type(part_type_p), vert_type(vert_type_p) {
	}

	void SetPartType(const GeometryPartType part_type_p) {
		part_type = part_type_p;
	}
	void SetVertType(const GeometryVertexType vert_type_p) {
		vert_type = vert_type_p;
	}
	GeometryPartType GetPartType() const {
		return part_type;
	}
	GeometryVertexType GetVertType() const {
		return vert_type;
	}

	static const GeometryType POINT;
	static const GeometryType LINESTRING;
	static const GeometryType POLYGON;
	static const GeometryType MULTIPOINT;
	static const GeometryType MULTILINESTRING;
	static const GeometryType MULTIPOLYGON;
	static const GeometryType GEOMETRYCOLLECTION;

private:
	GeometryPartType part_type;
	GeometryVertexType vert_type;
};

class Geometry {
public:
	static constexpr auto MAX_RECURSION_DEPTH = 16;

	DUCKDB_API static bool FromString(const string_t &wkt_text, string_t &result, Vector &result_vector, bool strict);
	DUCKDB_API static string_t ToString(Vector &result, const string_t &geom);
};

} // namespace duckdb
