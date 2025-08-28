//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geospatial_crs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/string.hpp"

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
	CoordinateReferenceSystem() : type(CoordinateReferenceSystemType::UNKNOWN), name(""), text("") {
	}

	// Allow implicit conversion from string
	CoordinateReferenceSystem(string text)
	    : type(CoordinateReferenceSystemType::UNKNOWN), name(""), text(std::move(text)) {
		// TODO: Try to auto-detect the type from the text
	}
	CoordinateReferenceSystem(const char *str) : CoordinateReferenceSystem(string(str)) {
	}

	static CoordinateReferenceSystem FromPROJJSON(const string &projjson) {
		CoordinateReferenceSystem crs;
		crs.type = CoordinateReferenceSystemType::PROJJSON;
		crs.text = projjson;
		// TODO: Try to parse name
		return crs;
	}

	bool operator==(const CoordinateReferenceSystem &other) const {
		return type == other.type && name == other.name && text == other.text;
	}
	bool operator!=(const CoordinateReferenceSystem &other) const {
		return !(*this == other);
	}

	void Serialize(Serializer &serializer) const;
	static CoordinateReferenceSystem Deserialize(Deserializer &deserializer);

	const string &GetValue() const {
		return text;
	}

	const string &GetName() const {
		return name;
	}

	CoordinateReferenceSystemType GetType() const {
		return type;
	}

public:
	// The format of the coordinate reference system
	CoordinateReferenceSystemType type;

	// Optional "friendly name" for the CRS, e.g., "WGS 84"
	string name;

	// The text representation of the CRS, e.g., "EPSG:4326" or a PROJJSON or WKT2 string
	// This is the only mandatory field
	string text;
};

} // namespace duckdb
