#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

//
// class GeometryVisitor {
// 	virtual bool OnEnter(GeometryType curr, GeometryType parent) = 0;
// 	virtual bool OnLeave(GeometryType curr, GeometryType parent) = 0;
// 	virtual bool OnData(GeometryType curr, GeometryType parent, const char* vertex_array, uint32_t vertex_count,
// uint32_t vertex_width) = 0;
// };
//
// class ExtentVisitor : public GeometryVisitor {
// 	Box2D extent;
// 	uint32_t count;
//
// 	bool OnEnter(GeometryType curr, GeometryType parent) override { return true; }
// 	bool OnLeave(GeometryType curr, GeometryType parent) override { return true; }
// 	bool OnData(GeometryType curr, GeometryType parent, const char* vertex_array, uint32_t vertex_count, uint32_t
// vertex_width) override { 		return true;
// 	}
// };

bool Geometry::FromString(const string &str, string_t &result, Vector &result_vector, bool strict) {
	double x;
	double y;

	if (sscanf(str.c_str(), "POINT (%lf %lf)", &x, &y) != 2) {
		if (strict) {
			throw ConversionException("Invalid POINT format: " + str);
		}
		return false;
	}

	result = StringVector::EmptyString(result_vector, sizeof(double) * 2);

	auto data = result.GetDataWriteable();
	memcpy(data, &x, sizeof(double));
	memcpy(data + sizeof(double), &y, sizeof(double));
	result.Finalize();

	return true;
}

string_t Geometry::ToString(Vector &result, const char *buf, idx_t len) {
	double x;
	double y;
	if (len != sizeof(double) * 2) {
		throw ConversionException("Invalid geometry data length: " + to_string(len));
	}
	memcpy(&x, buf, sizeof(double));
	memcpy(&y, buf + sizeof(double), sizeof(double));

	return StringVector::AddString(result, StringUtil::Format("POINT (%g %g)", x, y));
}

idx_t Geometry::GetBounds(const string_t &blob, Box2D &result) {
	auto len = blob.GetSize();
	auto buf = blob.GetData();

	double x;
	double y;
	if (len != sizeof(double) * 2) {
		throw ConversionException("Invalid geometry data length: " + to_string(len));
	}
	memcpy(&x, buf, sizeof(double));
	memcpy(&y, buf + sizeof(double), sizeof(double));

	result.min_x = x;
	result.min_y = y;
	result.max_x = x;
	result.max_y = y;

	return 1;
}

} // namespace duckdb
