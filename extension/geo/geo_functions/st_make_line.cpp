#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "geo_functions.hpp"
#include "geo_types.hpp"

namespace duckdb {

namespace geo {

void STMakeLineFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    if(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR) {
        // constant arguments
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }

    auto &input_point_list_vector = ListVector::GetEntry(args.data[0]);
    auto input_point_list_entries = ListVector::GetData(args.data[0]);
    auto &input_points_fields = StructVector::GetEntries(input_point_list_vector);
    
    auto &result_fields = StructVector::GetEntries(result);
    auto &result_kind_vector = result_fields[0];
    result_kind_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::GetData<int32_t>(*result_kind_vector)[0] = 0; // TODO: set srid based on inputs;

    auto &result_points_vector = ListVector::GetEntry(*result_fields[1]);

    auto points_count = ListVector::GetListSize(args.data[0]);
    ListVector::Reserve(*result_fields[1], points_count);
    ListVector::SetListSize(*result_fields[1], points_count);
    
    auto result_points_entries = ListVector::GetData(*result_fields[1]);
    for(idx_t i = 0; i < points_count; i++){
        result_points_entries[i] = input_point_list_entries[i];
    };

    auto &result_points_fields = StructVector::GetEntries(result_points_vector);

    result_points_fields[0]->Reinterpret(*input_points_fields[1]);
    result_points_fields[1]->Reinterpret(*input_points_fields[2]);
}

void RegisterMakeLineFunctions(ClientContext &context) {
    

    ScalarFunction st_makeline(
        "st_makeline",
        {LogicalType::LIST(geo::POINT)},
        geo::LINESTRING,
        geo::STMakeLineFunction
    );
    CreateScalarFunctionInfo st_makeline_info(move(st_makeline));

    auto &catalog = Catalog::GetCatalog(context);

    catalog.CreateFunction(context, &st_makeline_info);   
}
} // namespace geo

} // namespace duckdb
