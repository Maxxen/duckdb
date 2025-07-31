#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

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

} // namespace duckdb
