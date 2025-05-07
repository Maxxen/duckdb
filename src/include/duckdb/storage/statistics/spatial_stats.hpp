//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/spatial_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

enum class GeometryKind : uint8_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

struct SpatialStatsData {

	// TODO: Optimize
	uint32_t kinds;

	bool none_has_z;
	bool none_has_m;
	bool all_has_z;
	bool all_has_m;

	/*
	bool has_extent; // TODO: Make this a pointer to save mem?
	double min_x;
	double max_x;
	double min_y;
	double max_y;
	double min_z;
	double max_z;
	double min_m;
	double max_m;
	*/

	//bool has_max_vertex_count;
	//bool has_max_byte_length;
	//bool has_extent;

	// TODO: Optimize, or always make LE?
	// bool has_be;
	// bool has_le;

	//uint32_t max_vertex_count;
	//uint32_t max_byte_size;

	//SpatialStatsExtent extent;
};


struct SpatialStats {
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Update(BaseStatistics &stats, const string_t &wkb_blob);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	static bool NoneHasZ(const BaseStatistics &stats);
	static bool NoneHasM(const BaseStatistics &stats);
	static bool AllHasZ(const BaseStatistics &stats);
	static bool AllHasM(const BaseStatistics &stats);
	static bool SomeHasZ(const BaseStatistics &stats);
	static bool SomeHasM(const BaseStatistics &stats);

	static void SetAllHasZ(BaseStatistics &stats);
	static void SetAllHasM(BaseStatistics &stats);
	static void SetNoneHasZ(BaseStatistics &stats);
	static void SetNoneHasM(BaseStatistics &stats);
	static void SetSomeHasZ(BaseStatistics &stats);
	static void SetSomeHasM(BaseStatistics &stats);

	static bool HasSomeOfKind(const BaseStatistics &stats, GeometryKind kind);
	static bool HasOnlyOfKind(const BaseStatistics &stats, GeometryKind kind);
	static bool HasNoneOfKind(const BaseStatistics &stats, GeometryKind kind);

	static void SetHasKind(BaseStatistics &stats, GeometryKind kind, bool set);

	static void SetHasAllKinds(BaseStatistics &stats);
	static void SetHasNoKinds(BaseStatistics &stats);

private:
	static SpatialStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const SpatialStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
