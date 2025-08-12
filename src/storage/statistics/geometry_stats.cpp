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
	data.bounds = GeometryExtent::Unknown();
	data.types.SetUnknown();

	return result;
}

BaseStatistics GeometryStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();

	auto &data = GeometryStats::GetDataUnsafe(result);
	data.bounds = GeometryExtent::Empty();
	data.types.SetEmpty();

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

GeometryTypeSet &GeometryStats::GetTypes(BaseStatistics &stats) {
	return GeometryStats::GetDataUnsafe(stats).types;
}
const GeometryTypeSet &GeometryStats::GetTypes(const BaseStatistics &stats) {
	return GeometryStats::GetDataUnsafe(stats).types;
}

string GeometryStats::ToString(const BaseStatistics &stats) {
	const auto &geometry_data = GeometryStats::GetDataUnsafe(stats);

	string type_str[4];
	for (idx_t vert_idx = 0; vert_idx < 4; vert_idx++) {
		auto &str = type_str[vert_idx];
		str += "[";
		for (idx_t geom_idx = 0; geom_idx < sizeof(uint32_t) * 8; geom_idx++) {
			const auto bit = (1U << geom_idx);
			if (geometry_data.types.bits[vert_idx] & bit) {
				if (str.back() != '[') {
					str += ", ";
				}
				switch (static_cast<GeometryType>(geom_idx)) {
				case GeometryType::POINT:
					str += "POINT";
					break;
				case GeometryType::LINESTRING:
					str += "LINESTRING";
					break;
				case GeometryType::POLYGON:
					str += "POLYGON";
					break;
				case GeometryType::MULTIPOINT:
					str += "MULTIPOINT";
					break;
				case GeometryType::MULTILINESTRING:
					str += "MULTILINESTRING";
					break;
				case GeometryType::MULTIPOLYGON:
					str += "MULTIPOLYGON";
					break;
				case GeometryType::GEOMETRYCOLLECTION:
					str += "GEOMETRYCOLLECTION";
					break;
				default:
					str += "?";
				}
			}
		}
		str += "]";
	}

	const auto types =
	    StringUtil::Format("[XY: %s, ZZ: %s, MM: %s, ZM: %s]", type_str[0], type_str[1], type_str[2], type_str[3]);
	const auto extent =
	    StringUtil::Format("[MinX: %g, MinY: %g, MaxX: %g, MaxY: %g]", geometry_data.bounds.min_x,
	                       geometry_data.bounds.min_y, geometry_data.bounds.max_x, geometry_data.bounds.max_y);
	return StringUtil::Format("[Types: %s, Extent: %s]", types, extent);
}

void GeometryStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	serializer.WriteProperty(200, "min_x", geometry_data.bounds.min_x);
	serializer.WriteProperty(201, "min_y", geometry_data.bounds.min_y);
	serializer.WriteProperty(202, "max_x", geometry_data.bounds.max_x);
	serializer.WriteProperty(203, "max_y", geometry_data.bounds.max_y);

	serializer.WritePropertyWithDefault<uint32_t>(210, "xy_bitset", geometry_data.types.bits[0]);
	serializer.WritePropertyWithDefault<uint32_t>(211, "xyz_bitset", geometry_data.types.bits[1]);
	serializer.WritePropertyWithDefault<uint32_t>(212, "xym_bitset", geometry_data.types.bits[2]);
	serializer.WritePropertyWithDefault<uint32_t>(213, "xyzm_bitset", geometry_data.types.bits[3]);
}

void GeometryStats::Deserialize(Deserializer &deserializer, BaseStatistics &stats) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	geometry_data.bounds.min_x = deserializer.ReadProperty<double>(200, "min_x");
	geometry_data.bounds.min_y = deserializer.ReadProperty<double>(201, "min_y");
	geometry_data.bounds.max_x = deserializer.ReadProperty<double>(202, "max_x");
	geometry_data.bounds.max_y = deserializer.ReadProperty<double>(203, "max_y");

	geometry_data.types.bits[0] = deserializer.ReadPropertyWithDefault<uint32_t>(210, "xy_bitset");
	geometry_data.types.bits[1] = deserializer.ReadPropertyWithDefault<uint32_t>(211, "xyz_bitset");
	geometry_data.types.bits[2] = deserializer.ReadPropertyWithDefault<uint32_t>(212, "xym_bitset");
	geometry_data.types.bits[3] = deserializer.ReadPropertyWithDefault<uint32_t>(213, "xyzm_bitset");
}

void GeometryStats::Update(BaseStatistics &stats, const string_t &value) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);

	// Add bounds to the statistics
	GeometryExtent bounds = GeometryExtent::Empty();
	if (Geometry::GetExtent(value, bounds) != 0) {
		geometry_data.bounds.Extend(bounds);
	}

	// Add geometry and vertex type to the statistics
	const auto type = Geometry::GetGeometryType(value);
	geometry_data.types.Add(type.first, type.second);
}

void GeometryStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() != LogicalTypeId::GEOMETRY) {
		return;
	}
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	auto &other_geometry_data = GeometryStats::GetDataUnsafe(other);

	geometry_data.bounds.Extend(other_geometry_data.bounds);
	geometry_data.types.Merge(other_geometry_data.types);
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
