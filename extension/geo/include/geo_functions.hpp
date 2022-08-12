#pragma once
#include "duckdb.hpp"

namespace duckdb  {

namespace geo {

// Scalar Functions

void RegisterMakeLineFunctions(ClientContext &context);
void RegisterPointFunctions(ClientContext &context);
void RegisterGeohashFunctions(ClientContext &context);



} // namespace geo

} // namespace duckdb