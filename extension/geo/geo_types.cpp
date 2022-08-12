#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

#include "geo_types.hpp"

namespace duckdb {

namespace geo {

LogicalType GEOMETRY = LogicalType::BLOB;

LogicalType POINT = LogicalType::STRUCT({
    {"srid", LogicalType::INTEGER},
    {"x", LogicalType::DOUBLE}, 
    {"y", LogicalType::DOUBLE}
});

LogicalType LINESTRING = LogicalType::STRUCT({
    {"srid", LogicalType::INTEGER},
    {"points", LogicalType::LIST(
        LogicalType::STRUCT({
            {"x", LogicalType::DOUBLE}, 
            {"y", LogicalType::DOUBLE}
        })
    )}
});

LogicalType POLYGON = LogicalType::STRUCT({
    {"srid", LogicalType::INTEGER},
    {"rings", LogicalType::LIST(
        LogicalType::LIST(
            LogicalType::STRUCT({
                {"x", LogicalType::DOUBLE}, 
                {"y", LogicalType::DOUBLE}
            })
        )
    )}
});

void RegisterGeometryTypes(ClientContext &context) 
{

    /* TODO:

    auto &catalog = Catalog::GetCatalog(context);
    // Geometry type
	string alias_name = "Geometry";
	auto alias_info = make_unique<CreateTypeInfo>();
	alias_info->name = alias_name;
	//target_type.SetAlias(alias_name);
	alias_info->type = geo::GEOMETRY;
	auto entry = (TypeCatalogEntry *)catalog.CreateType(context, alias_info.get());
	LogicalType::SetCatalog(geo::GEOMETRY, entry);
    */ 
}

} // namespace geo

} // namespace duckdb