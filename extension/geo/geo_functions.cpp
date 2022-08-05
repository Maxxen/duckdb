#include "duckdb.hpp"

namespace duckdb {

static void STPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {

    if(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR &&
        args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR) {
        // constant arguments
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }

    auto &child_entries = StructVector::GetEntries(result);

    auto x_input = FlatVector::GetData<double>(args.data[0]);
    auto y_input = FlatVector::GetData<double>(args.data[1]);

    auto x_data = FlatVector::GetData<double>(*child_entries[0]);
    auto y_data = FlatVector::GetData<double>(*child_entries[1]);

    for (idx_t i = 0; i < args.size(); i++) {
        
        x_data[i] = x_input[i];
        y_data[i] = y_input[i];
    }
}

static void STMakeLineFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
    ConstantVector::GetData<string_t>(*result_kind_vector)[0] = "LINESTRING"; // this is always inlined

    auto &result_points_vector = ListVector::GetEntry(*result_fields[1]);

    auto points_count = ListVector::GetListSize(args.data[0]);
    ListVector::Reserve(*result_fields[1], points_count);
    
    auto result_points_entries = ListVector::GetData(*result_fields[1]);
    for(idx_t i = 0; i < points_count; i++){
        result_points_entries[i] = input_point_list_entries[i];
    };

    auto &result_points_fields = StructVector::GetEntries(result_points_vector);

    result_points_fields[0]->Reinterpret(*input_points_fields[0]);
    result_points_fields[1]->Reinterpret(*input_points_fields[1]);

    //result_points_fields[0]->Reference(*input_points_fields[0]);
    //result_points_fields[1]->Reference(*input_points_fields[1]);


    /*
    for(idx_t i = 0; i < args.size(); i++) {
        auto entry = list_entries[i];
        auto sum = 0.0;
        for(idx_t point_idx = 0; point_idx < entry.length; point_idx++) {
            auto x = x_data[entry.offset + point_idx];
            auto y = y_data[entry.offset + point_idx];
            
            sum += x * x + y * y;
        }

        result_x_vector[i] = sum;
    }
    */
} 

// TODO: Handle invalid precision inputs or error instead of silently clamping
static void STGeoHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    
    auto &input = StructVector::GetEntries(args.data[0]);
    auto precision_input = FlatVector::GetData<int32_t>(args.data[1]);
    
    auto x_input = FlatVector::GetData<double>(*input[0]);
    auto y_input = FlatVector::GetData<double>(*input[1]);

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

} // namespace duckdb