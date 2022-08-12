#pragma once
#include "duckdb.hpp"

namespace duckdb  {

namespace geo {

extern LogicalType GEOMETRY;
extern LogicalType POINT;
extern LogicalType LINESTRING;
extern LogicalType POLYGON;

void RegisterGeometryTypes(ClientContext &context); 

} // namespace geo

} // namespace duckdb