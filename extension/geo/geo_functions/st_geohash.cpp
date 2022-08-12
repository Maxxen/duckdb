#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "geo_functions.hpp"
#include "geo_types.hpp"

namespace duckdb {

namespace geo {

// TODO: Handle invalid precision inputs or error instead of silently clamping
static void STGeoHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    
    auto &input = StructVector::GetEntries(args.data[0]);
    auto precision_input = FlatVector::GetData<int32_t>(args.data[1]);
    
    auto x_input = FlatVector::GetData<double>(*input[1]);
    auto y_input = FlatVector::GetData<double>(*input[2]);

    for (idx_t i = 0; i < args.size(); i++) {
        auto x = x_input[i];
        auto y = y_input[i];

        // Geohash
        const char* base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
        const auto MAX_PRECISION = 20;
        auto precision = std::min(std::max(precision_input[i], 1), MAX_PRECISION);

        auto lat_min = -90.0;
        auto lat_max = 90.0;
        auto lon_min = -180.0;
        auto lon_max = 180.0;
        
        char hash[MAX_PRECISION];

        idx_t index = 0;
        idx_t bit = 0;
        bool even = true;
        idx_t len = 0;
        while(len < precision) {
            if(even) {
                auto mid = (lon_min + lon_max) / 2;
                if(x >= mid) {
                    index = (index << 1) | 1;
                    lon_min = mid;
                } else {
                    index = (index << 1);
                    lon_max = mid;
                }
            }
            else {
                auto mid = (lat_min + lat_max) / 2;
                if(y >= mid) {
                    index = (index << 1) | 1;
                    lat_min = mid;
                } else {
                    index = (index << 1);
                    lat_max = mid;
                }
            }

            even = !even;
            
            bit++;
            if(bit == 5) {
                hash[len++] = base32[index];
                index = 0;
                bit = 0;
            }
        }

        string_t hash_string = string_t(hash, precision);
        auto data = FlatVector::GetData<string_t>(result);

        data[i] = hash_string.IsInlined() ? hash_string : StringVector::AddString(result, hash_string);
    }
}

void RegisterGeohashFunctions(ClientContext &context) {
    
    ScalarFunction st_geohash(
        "st_geohash",
        {geo::POINT, LogicalType::INTEGER}, 
        LogicalType::VARCHAR,
        geo::STGeoHashFunction
    );
    CreateScalarFunctionInfo st_geohash_info(move(st_geohash));

    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateFunction(context, &st_geohash_info);
}



} // namespace geo

} // namespace duckdb