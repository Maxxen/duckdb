#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "geo_functions.hpp"
#include "geo_types.hpp"

namespace duckdb {

namespace geo {

static void STPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {

    if(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR &&
        args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR) {
        // constant arguments
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }

    auto &child_entries = StructVector::GetEntries(result);

    auto x_input = FlatVector::GetData<double>(args.data[0]);
    auto y_input = FlatVector::GetData<double>(args.data[1]);

    auto srid_data = FlatVector::GetData<int32_t>(*child_entries[0]);
    auto x_data = FlatVector::GetData<double>(*child_entries[1]);
    auto y_data = FlatVector::GetData<double>(*child_entries[2]);

    for (idx_t i = 0; i < args.size(); i++) {
        
        srid_data[i] = 0; // TODO: set SRID to 0 for now
        x_data[i] = x_input[i];
        y_data[i] = y_input[i];
    }
}

void RegisterPointFunctions(ClientContext &context) {
    

    ScalarFunction st_point(
        "st_point", 
        {LogicalType::DOUBLE, LogicalType::DOUBLE}, 
        geo::POINT, 
        geo::STPointFunction
    );

    CreateScalarFunctionInfo st_point_info(move(st_point));

    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateFunction(context, &st_point_info);
}


} // namespace geo

} // namespace duckdb
