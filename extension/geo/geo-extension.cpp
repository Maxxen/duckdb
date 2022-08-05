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
#include "geo_functions.cpp"

namespace duckdb {


void GeoExtension::Load(DuckDB &db){
    Connection con(db);
    con.BeginTransaction();

    auto &catalog = Catalog::GetCatalog(*con.context);

    ScalarFunction st_point(
        "st_point", 
        {LogicalType::DOUBLE, LogicalType::DOUBLE}, 
        LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}), 
        STPointFunction
    );

    CreateScalarFunctionInfo st_point_info(move(st_point));
    catalog.CreateFunction(*con.context, &st_point_info);

    ScalarFunction st_geohash(
        "st_geohash",
        {LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}), LogicalType::INTEGER}, 
        LogicalType::VARCHAR,
        STGeoHashFunction
    );
    CreateScalarFunctionInfo st_geohash_info(move(st_geohash));
    catalog.CreateFunction(*con.context, &st_geohash_info);

    ScalarFunction st_makeline(
        "st_makeline",
        {LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}))},
        LogicalType::STRUCT({
            { "kind", LogicalType::VARCHAR },
            { "points", LogicalType::LIST(
                LogicalType::STRUCT({
                    {"x", LogicalType::DOUBLE}, 
                    {"y", LogicalType::DOUBLE}}
                ))
            }
        }),
        STMakeLineFunction
    );
    CreateScalarFunctionInfo st_makeline_info(move(st_makeline));
    catalog.CreateFunction(*con.context, &st_makeline_info);

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
