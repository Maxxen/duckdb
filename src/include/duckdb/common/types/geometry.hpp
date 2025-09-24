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

enum class GeometryVertexType : uint8_t {
	XY = 0,
	XYZ = 1,
	XYM = 2,
	XYZM = 3,
};

enum class GeometryPartType : uint8_t {
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
	uint32_t GetVertexWidth() const {
		switch (vert_type) {
		case GeometryVertexType::XY:
			return 2 * sizeof(double);
		case GeometryVertexType::XYZ:
		case GeometryVertexType::XYM:
			return 3 * sizeof(double);
		case GeometryVertexType::XYZM:
			return 4 * sizeof(double);
		default:
			throw InternalException("Invalid vertex type");
		}
	}

	bool HasZ() const {
		return vert_type == GeometryVertexType::XYZ || vert_type == GeometryVertexType::XYZM;
	}
	bool HasM() const {
		return vert_type == GeometryVertexType::XYM || vert_type == GeometryVertexType::XYZM;
	}

	static GeometryType FromWKB(uint32_t wkb_type);

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

struct GeometryExtent {

	double xmin;
	double xmax;
	double ymin;
	double ymax;
	double zmin;
	double zmax;
	double mmin;
	double mmax;

	// Encompasses no points
	static GeometryExtent Empty() {
		constexpr auto min = NumericLimits<double>::Maximum();
		constexpr auto max = NumericLimits<double>::Minimum();
		return {min, max, min, max, min, max, min, max};
	}

	// Encompasses all possible points
	static GeometryExtent Unknown() {
		constexpr auto min = NumericLimits<double>::Minimum();
		constexpr auto max = NumericLimits<double>::Maximum();
		return {min, max, min, max, min, max, min, max};
	}

	bool IsSet() const {
		return xmin != NumericLimits<double>::Maximum() && xmax != NumericLimits<double>::Minimum() &&
		       ymin != NumericLimits<double>::Maximum() && ymax != NumericLimits<double>::Minimum();
	}

	bool HasZ() const {
		return zmin != NumericLimits<double>::Maximum() && zmax != NumericLimits<double>::Minimum();
	}

	bool HasM() const {
		return mmin != NumericLimits<double>::Maximum() && mmax != NumericLimits<double>::Minimum();
	}

	void Merge(const GeometryExtent &other) {
		xmin = std::min(xmin, other.xmin);
		xmax = std::max(xmax, other.xmax);
		ymin = std::min(ymin, other.ymin);
		ymax = std::max(ymax, other.ymax);
		zmin = std::min(zmin, other.zmin);
		zmax = std::max(zmax, other.zmax);
		mmin = std::min(mmin, other.mmin);
		mmax = std::max(mmax, other.mmax);
	}

	void Merge(const double &xmin_p, const double &xmax_p, const double &ymin_p, const double &ymax_p) {
		xmin = std::min(xmin, xmin_p);
		xmax = std::max(xmax, xmax_p);
		ymin = std::min(ymin, ymin_p);
		ymax = std::max(ymax, ymax_p);
	}

	void ExtendX(const double &x) {
		xmin = std::min(xmin, x);
		xmax = std::max(xmax, x);
	}
	void ExtendY(const double &y) {
		ymin = std::min(ymin, y);
		ymax = std::max(ymax, y);
	}
	void ExtendZ(const double &z) {
		zmin = std::min(zmin, z);
		zmax = std::max(zmax, z);
	}
	void ExtendM(const double &m) {
		mmin = std::min(mmin, m);
		mmax = std::max(mmax, m);
	}
};

class Geometry {
public:
	static constexpr auto MAX_RECURSION_DEPTH = 16;

	DUCKDB_API static bool FromString(const string_t &wkt_text, string_t &result, Vector &result_vector, bool strict);
	DUCKDB_API static string_t ToString(Vector &result, const string_t &geom);
	DUCKDB_API static GeometryType GetType(const string_t &geom);
	DUCKDB_API static uint32_t GetExtent(const string_t &geom, GeometryExtent &extent);
};

} // namespace duckdb
