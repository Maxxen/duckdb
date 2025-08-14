//===----------------------------------------------------------------------===//
//                         DuckDB
//
// geo_parquet.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "parquet_types.h"

namespace duckdb {
struct ParquetColumnSchema;

enum class WKBGeometryType : uint16_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,

	POINT_Z = 1001,
	LINESTRING_Z = 1002,
	POLYGON_Z = 1003,
	MULTIPOINT_Z = 1004,
	MULTILINESTRING_Z = 1005,
	MULTIPOLYGON_Z = 1006,
	GEOMETRYCOLLECTION_Z = 1007,
};

struct WKBGeometryTypes {
	static const char *ToString(WKBGeometryType type);
};

//------------------------------------------------------------------------------
// GeoParquetMetadata
//------------------------------------------------------------------------------
class ParquetReader;
class ColumnReader;
class ClientContext;
class ExpressionExecutor;

enum class GeoParquetColumnEncoding : uint8_t {
	WKB = 1,
	POINT,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
};

struct GeoParquetColumnMetadata {
	// The encoding of the geometry column
	GeoParquetColumnEncoding geometry_encoding;

	// The geometry types that are present in the column
	set<WKBGeometryType> geometry_types;

	// The bounds of the geometry column
	GeometryExtent bbox = GeometryExtent::Empty();

	// The crs of the geometry column (if any) in PROJJSON format
	string projjson;

	// The logical type of the column
	LogicalType logical_type;
};

enum class GeoParquetVersion : uint8_t {
	NONE = 0,   //! No GeoParquet metadata, write geometries as normal Parquet geometry columns
	V100 = 100, //! GeoParquet v1.0
	V110 = 110, //! GeoParquet v1.1
};

class GeoParquetFileMetadata {
public:
	// Try to read GeoParquet metadata. Returns nullptr if not found, invalid or disabled by setting.
	explicit GeoParquetFileMetadata(GeoParquetVersion version);

	static unique_ptr<GeoParquetFileMetadata> TryRead(const duckdb_parquet::FileMetaData &file_meta_data,
	                                                  const ClientContext &context);
	void Write(duckdb_parquet::FileMetaData &file_meta_data) const;

	const unordered_map<string, GeoParquetColumnMetadata> &GetColumnMeta() const {
		return geometry_columns;
	}

	bool IsGeometryColumn(const string &column_name) const;

	void AddGeoParquetStats(const string &column_name, const LogicalType &type,
	                        const duckdb_parquet::GeospatialStatistics &stats);

	static bool IsGeoParquetConversionEnabled(const ClientContext &context);

private:
	mutex write_lock;
	GeoParquetVersion version;
	string primary_geometry_column;
	unordered_map<string, GeoParquetColumnMetadata> geometry_columns;
};

} // namespace duckdb
