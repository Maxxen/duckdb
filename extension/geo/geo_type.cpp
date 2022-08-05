#include "duckdb.hpp"

namespace duckdb {
    static const LogicalType POINT = LogicalType::STRUCT({
        {"kind", LogicalType::VARCHAR},
        {"x", LogicalType::DOUBLE}, 
        {"y", LogicalType::DOUBLE}
    });
    
    static const LogicalType LINESTRING = LogicalType::STRUCT({
        {"kind", LogicalType::VARCHAR},
        {"points", LogicalType::LIST(LogicalType::STRUCT({
            {"x", LogicalType::DOUBLE}, 
            {"y", LogicalType::DOUBLE}
        }))}
    });

    static const LogicalType POLYGON = LogicalType::STRUCT({
        {"kind", LogicalType::VARCHAR},
        {"rings", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT({
            {"x", LogicalType::DOUBLE}, 
            {"y", LogicalType::DOUBLE}
        })))}
    }); 

} // namespace duckdb