#include "duckdb/storage/statistics/geometry_stats.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

BaseStatistics GeometryStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();

	auto &data = GeometryStats::GetDataUnsafe(result);
	data.bounds = GeometryExtent::Empty();

	return result;
}

BaseStatistics GeometryStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();

	auto &data = GeometryStats::GetDataUnsafe(result);
	data.bounds = GeometryExtent::Empty();

	return result;
}

GeometryStatsData &GeometryStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::GEOMETRY_STATS);
	return stats.stats_union.geometry_data;
}

const GeometryStatsData &GeometryStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::GEOMETRY_STATS);
	return stats.stats_union.geometry_data;
}

string GeometryStats::ToString(const BaseStatistics &stats) {
	const auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	return StringUtil::Format("[MinX: %g, MinY: %g, MaxX: %g, MaxY: %g]", geometry_data.bounds.min_x,
	                          geometry_data.bounds.min_y, geometry_data.bounds.max_x, geometry_data.bounds.max_y);
}

void GeometryStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	serializer.WriteProperty(200, "min_x", geometry_data.bounds.min_x);
	serializer.WriteProperty(201, "min_y", geometry_data.bounds.min_y);
	serializer.WriteProperty(202, "max_x", geometry_data.bounds.max_x);
	serializer.WriteProperty(203, "max_y", geometry_data.bounds.max_y);
}

void GeometryStats::Deserialize(Deserializer &deserializer, BaseStatistics &stats) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	geometry_data.bounds.min_x = deserializer.ReadProperty<double>(200, "min_x");
	geometry_data.bounds.min_y = deserializer.ReadProperty<double>(201, "min_y");
	geometry_data.bounds.max_x = deserializer.ReadProperty<double>(202, "max_x");
	geometry_data.bounds.max_y = deserializer.ReadProperty<double>(203, "max_y");
}

void GeometryStats::Update(BaseStatistics &stats, const string_t &value) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);

	GeometryExtent bounds = GeometryExtent::Empty();
	if (Geometry::GetExtent(value, bounds) != 0) {
		geometry_data.bounds.Extend(bounds);
	}
}

void GeometryStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() != LogicalTypeId::GEOMETRY) {
		return;
	}
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	auto &other_geometry_data = GeometryStats::GetDataUnsafe(other);
	geometry_data.bounds.Extend(other_geometry_data.bounds);
}

void GeometryStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
}

FilterPropagateResult GeometryStats::CheckZonemap(const BaseStatistics &stats, const Value &value) {
	if (value.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	auto &geometry_value = StringValue::Get(value);

	GeometryExtent bounds = GeometryExtent::Empty();
	if (!Geometry::GetExtent(geometry_value, bounds)) {
		// If we cannot get bounds from the geometry, we cannot check comparison
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	if (geometry_data.bounds.Intersects(bounds)) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}

	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

} // namespace duckdb
