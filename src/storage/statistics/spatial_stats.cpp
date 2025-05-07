#include "duckdb/storage/statistics/spatial_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

BaseStatistics SpatialStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();

	SetHasAllKinds(result);	// Assume all kinds are present
	SetSomeHasM(result);		// Assume some M values are present
	SetSomeHasZ(result);		// Assume some Z values are present

	return result;
}

BaseStatistics SpatialStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();

	SetHasNoKinds(result);	// Assume no kinds are present
	SetNoneHasZ(result);		// Assume no Z values are present
	SetNoneHasM(result);		// Assume no M values are present

	return result;
}

void SpatialStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	throw NotImplementedException("SpatialStats::Serialize not implemented");
}

void SpatialStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	throw NotImplementedException("SpatialStats::Deserialize not implemented");
}

string SpatialStats::ToString(const BaseStatistics &stats) {
	return "TODO";
}

void SpatialStats::Update(BaseStatistics &stats, const string_t &wkb_blob) {
	throw NotImplementedException("SpatialStats::Update not implemented");
}

void SpatialStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	throw NotImplementedException("SpatialStats::Merge not implemented");
}

void SpatialStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	throw NotImplementedException("SpatialStats::Copy not implemented");
}

void SpatialStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	throw NotImplementedException("SpatialStats::Verify not implemented");
}

bool SpatialStats::NoneHasZ(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return data.none_has_z && !data.all_has_z;
}
bool SpatialStats::NoneHasM(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return data.none_has_m && !data.all_has_m;
}
bool SpatialStats::AllHasZ(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return data.all_has_z && !data.none_has_z;
}
bool SpatialStats::AllHasM(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return data.all_has_m && !data.none_has_m;
}
bool SpatialStats::SomeHasZ(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return !data.none_has_z && !data.all_has_z;
}
bool SpatialStats::SomeHasM(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	return !data.none_has_m && !data.all_has_m;
}
void SpatialStats::SetAllHasZ(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.all_has_z = true;
	data.none_has_z = false;
}
void SpatialStats::SetAllHasM(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.all_has_m = true;
	data.none_has_m = false;
}
void SpatialStats::SetNoneHasZ(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.none_has_z = true;
	data.all_has_z = false;
}
void SpatialStats::SetNoneHasM(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.none_has_m = true;
	data.all_has_m = false;
}
void SpatialStats::SetSomeHasZ(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.all_has_z = false;
	data.none_has_z = false;
}
void SpatialStats::SetSomeHasM(BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	data.all_has_m = false;
	data.none_has_m = false;
}
SpatialStatsData &SpatialStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::SPATIAL_STATS);
	return stats.stats_union.spatial_data;
}

static constexpr uint32_t FLAG_NONE = 0x00;
static constexpr uint32_t FLAG_POINT = 0x01;
static constexpr uint32_t FLAG_LINESTRING = 0x02;
static constexpr uint32_t FLAG_POLYGON = 0x04;
static constexpr uint32_t FLAG_MULTIPOINT = 0x08;
static constexpr uint32_t FLAG_MULTILINESTRING = 0x10;
static constexpr uint32_t FLAG_MULTIPOLYGON = 0x20;
static constexpr uint32_t FLAG_GEOMETRYCOLLECTION = 0x40;
static constexpr uint32_t FLAG_ALL = 0xFFFFFFFF;

bool SpatialStats::HasSomeOfKind(const BaseStatistics &stats, const GeometryKind kind) {
	const auto &kinds = GetDataUnsafe(stats).kinds;
	switch (kind) {
	case GeometryKind::POINT:
		return kinds & FLAG_POINT;
	case GeometryKind::LINESTRING:
		return kinds & FLAG_LINESTRING;
	case GeometryKind::POLYGON:
		return kinds & FLAG_POLYGON;
	case GeometryKind::MULTIPOINT:
		return kinds & FLAG_MULTIPOINT;
	case GeometryKind::MULTILINESTRING:
		return kinds & FLAG_MULTILINESTRING;
	case GeometryKind::MULTIPOLYGON:
		return kinds & FLAG_MULTIPOLYGON;
	case GeometryKind::GEOMETRYCOLLECTION:
		return kinds & FLAG_GEOMETRYCOLLECTION;
	default:
		return false;
	}
}

bool SpatialStats::HasOnlyOfKind(const BaseStatistics &stats, const GeometryKind kind) {
	const auto &kinds = GetDataUnsafe(stats).kinds;
	switch (kind) {
	case GeometryKind::POINT:
		return kinds == FLAG_POINT;
	case GeometryKind::LINESTRING:
		return kinds == FLAG_LINESTRING;
	case GeometryKind::POLYGON:
		return kinds == FLAG_POLYGON;
	case GeometryKind::MULTIPOINT:
		return kinds == FLAG_MULTIPOINT;
	case GeometryKind::MULTILINESTRING:
		return kinds == FLAG_MULTILINESTRING;
	case GeometryKind::MULTIPOLYGON:
		return kinds == FLAG_MULTIPOLYGON;
	case GeometryKind::GEOMETRYCOLLECTION:
		return kinds == FLAG_GEOMETRYCOLLECTION;
	default:
		return false;
	}
}

bool SpatialStats::HasNoneOfKind(const BaseStatistics &stats, GeometryKind kind) {
	const auto &kinds = GetDataUnsafe(stats).kinds;
	switch (kind) {
	case GeometryKind::POINT:
		return !(kinds & FLAG_POINT);
	case GeometryKind::LINESTRING:
		return !(kinds & FLAG_LINESTRING);
	case GeometryKind::POLYGON:
		return !(kinds & FLAG_POLYGON);
	case GeometryKind::MULTIPOINT:
		return !(kinds & FLAG_MULTIPOINT);
	case GeometryKind::MULTILINESTRING:
		return !(kinds & FLAG_MULTILINESTRING);
	case GeometryKind::MULTIPOLYGON:
		return !(kinds & FLAG_MULTIPOLYGON);
	case GeometryKind::GEOMETRYCOLLECTION:
		return !(kinds & FLAG_GEOMETRYCOLLECTION);
	default:
		return false;
	}
}

void SpatialStats::SetHasKind(BaseStatistics &stats, const GeometryKind kind, bool set) {
	auto &kinds = GetDataUnsafe(stats).kinds;

	switch (kind) {
	case GeometryKind::POINT:
		kinds = set ? (kinds | FLAG_POINT) : (kinds & ~FLAG_POINT);
		break;
	case GeometryKind::LINESTRING:
		kinds = set ? (kinds | FLAG_LINESTRING) : (kinds & ~FLAG_LINESTRING);
		break;
	case GeometryKind::POLYGON:
		kinds = set ? (kinds | FLAG_POLYGON) : (kinds & ~FLAG_POLYGON);
		break;
	case GeometryKind::MULTIPOINT:
		kinds = set ? (kinds | FLAG_MULTIPOINT) : (kinds & ~FLAG_MULTIPOINT);
		break;
	case GeometryKind::MULTILINESTRING:
		kinds = set ? (kinds | FLAG_MULTILINESTRING) : (kinds & ~FLAG_MULTILINESTRING);
		break;
	case GeometryKind::MULTIPOLYGON:
		kinds = set ? (kinds | FLAG_MULTIPOLYGON) : (kinds & ~FLAG_MULTIPOLYGON);
		break;
	case GeometryKind::GEOMETRYCOLLECTION:
		kinds = set ? (kinds | FLAG_GEOMETRYCOLLECTION) : (kinds & ~FLAG_GEOMETRYCOLLECTION);
		break;
	default:
		throw InternalException("Invalid geometry kind");
	}
}

void SpatialStats::SetHasAllKinds(BaseStatistics &stats) {
	auto &kinds = GetDataUnsafe(stats).kinds;
	kinds |= FLAG_ALL;
}

void SpatialStats::SetHasNoKinds(BaseStatistics &stats) {
	auto &kinds = GetDataUnsafe(stats).kinds;
	kinds = FLAG_NONE;
}

} // namespace duckdb
