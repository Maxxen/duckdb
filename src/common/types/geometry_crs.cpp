#include "duckdb/common/types/geometry_crs.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

void CoordinateReferenceSystem::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<CoordinateReferenceSystemType>(200, "type", type,
	                                                                   CoordinateReferenceSystemType::UNKNOWN);
	serializer.WritePropertyWithDefault<string>(201, "name", name);
	serializer.WritePropertyWithDefault<string>(202, "text", text);
}

CoordinateReferenceSystem CoordinateReferenceSystem::Deserialize(Deserializer &deserializer) {
	CoordinateReferenceSystem crs;
	deserializer.ReadPropertyWithExplicitDefault<CoordinateReferenceSystemType>(200, "type", crs.type,
	                                                                            CoordinateReferenceSystemType::UNKNOWN);
	deserializer.ReadPropertyWithDefault<string>(201, "name", crs.name);
	deserializer.ReadPropertyWithDefault<string>(202, "text", crs.text);
	return crs;
}

} // namespace duckdb
