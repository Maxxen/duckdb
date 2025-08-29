//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geospatial_crs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

enum class CoordinateReferenceSystemType : uint8_t {
	UNKNOWN = 1,
	PROJJSON = 2,
	WKT2_2019 = 3,
	AUTH_CODE = 4,
	SRID = 5
};

class CoordinateReferenceSystem {
public:
	CoordinateReferenceSystem();
	explicit CoordinateReferenceSystem(const string &crs);

	const string &GetValue() const {
		return text;
	}

	const string &GetName() const {
		return name;
	}

	CoordinateReferenceSystemType GetType() const {
		return type;
	}

	bool operator==(const CoordinateReferenceSystem &other) const {
		return type == other.type && name == other.name && text == other.text;
	}

	bool operator!=(const CoordinateReferenceSystem &other) const {
		return !(*this == other);
	}

	void Serialize(Serializer &serializer) const;

	static CoordinateReferenceSystem Deserialize(Deserializer &deserializer);

	// string TryGetAsPROJJSON();
	// string TryGetAsWKT22019();
	// string TryGetAsAuthCode();

private:
	// The format of the coordinate reference system
	CoordinateReferenceSystemType type;

	// Optional "friendly name" for the CRS, e.g., "WGS 84"
	string name;

	// The text representation of the CRS, e.g., "EPSG:4326" or a PROJJSON or WKT2 string
	// This is the only mandatory property
	string text;
};

} // namespace duckdb
