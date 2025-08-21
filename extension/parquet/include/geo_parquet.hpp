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

	// Global geoparquet statistics for this geometry column
	GeometryStatsData stats;

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
