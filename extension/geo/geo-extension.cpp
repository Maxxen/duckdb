#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "geo-extension.hpp"
#include "geo_common.hpp"
#include "geo_types.hpp"
#include "geo_functions.hpp"


namespace duckdb {

void GeoExtension::Load(DuckDB &db){
    Connection con(db);
    con.BeginTransaction();
    
    geo::RegisterMakeLineFunctions(*con.context);
    geo::RegisterPointFunctions(*con.context);
    geo::RegisterGeohashFunctions(*con.context);
    geo::RegisterGeometryTypes(*con.context);

    con.Commit();
}

std::string GeoExtension::Name() { 
    return "geo";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void geo_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::GeoExtension>();
}

DUCKDB_EXTENSION_API const char *geo_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}


#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
