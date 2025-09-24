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
#include "duckdb/common/types/geometry.hpp"

namespace duckdb {

struct ParquetColumnSchema;

struct GeometryKindSet {

	uint8_t bits[4] = {0, 0, 0, 0};

	void Add(uint32_t wkb_type) {
		auto kind = wkb_type % 1000;
		auto dims = wkb_type / 1000;
		if (kind < 1 || kind > 7 || (dims) > 3) {
			return;
		}
		bits[dims] |= (1 << (kind - 1));
	}

	void Combine(const GeometryKindSet &other) {
		for (uint32_t d = 0; d < 4; d++) {
			bits[d] |= other.bits[d];
		}
	}

	bool IsEmpty() const {
		for (uint32_t d = 0; d < 4; d++) {
			if (bits[d] != 0) {
				return false;
			}
		}
		return true;
	}

	template <class T>
	vector<T> ToList() const {
		vector<T> result;
		for (uint32_t d = 0; d < 4; d++) {
			for (uint32_t i = 1; i <= 7; i++) {
				if (bits[d] & (1 << (i - 1))) {
					result.push_back(i + d * 1000);
				}
			}
		}
		return result;
	}

	vector<string> ToString(bool snake_case) const {
		vector<string> result;
		for (uint32_t d = 0; d < 4; d++) {
			for (uint32_t i = 1; i <= 7; i++) {
				if (bits[d] & (1 << (i - 1))) {
					string str;
					switch (i) {
					case 1:
						str = snake_case ? "point" : "Point";
						break;
					case 2:
						str = snake_case ? "linestring" : "LineString";
						break;
					case 3:
						str = snake_case ? "polygon" : "Polygon";
						break;
					case 4:
						str = snake_case ? "multipoint" : "MultiPoint";
						break;
					case 5:
						str = snake_case ? "multilinestring" : "MultiLineString";
						break;
					case 6:
						str = snake_case ? "multipolygon" : "MultiPolygon";
						break;
					case 7:
						str = snake_case ? "geometrycollection" : "GeometryCollection";
						break;
					default:
						str = snake_case ? "unknown" : "Unknown";
						break;
					}
					switch (d) {
					case 1:
						str += snake_case ? "_z" : " Z";
						break;
					case 2:
						str += snake_case ? "_m" : " M";
						break;
					case 3:
						str += snake_case ? "_zm" : " ZM";
						break;
					default:
						break;
					}

					result.push_back(str);
				}
			}
		}
		return result;
	}
};

struct GeometryStats {
	GeometryKindSet types;
	GeometryExtent bbox;

	void Update(const string_t &wkb);
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

	// The statistics of the geometry column
	GeometryStats stats;

	// The crs of the geometry column (if any) in PROJJSON format
	string projjson;

	// Used to track the "primary" geometry column (if any)
	idx_t insertion_index = 0;
};

class GeoParquetFileMetadata {
public:
	void AddGeoParquetStats(const string &column_name, const LogicalType &type, const GeometryStats &stats);
	void Write(duckdb_parquet::FileMetaData &file_meta_data);

	// Try to read GeoParquet metadata. Returns nullptr if not found, invalid or the required spatial extension is not
	// available.
	static unique_ptr<GeoParquetFileMetadata> TryRead(const duckdb_parquet::FileMetaData &file_meta_data,
	                                                  const ClientContext &context);
	const unordered_map<string, GeoParquetColumnMetadata> &GetColumnMeta() const;

	static unique_ptr<ColumnReader> CreateColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema,
	                                                   ClientContext &context);

	bool IsGeometryColumn(const string &column_name) const;

	static bool IsGeoParquetConversionEnabled(const ClientContext &context);
	static LogicalType GeometryType();

private:
	mutex write_lock;
	string version = "1.1.0";
	unordered_map<string, GeoParquetColumnMetadata> geometry_columns;
};

} // namespace duckdb
