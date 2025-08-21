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
	data.bbox = GeometryExtent::Unknown();
	data.types.SetUnknown();

	return result;
}

BaseStatistics GeometryStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();

	auto &data = GeometryStats::GetDataUnsafe(result);
	data.bbox = GeometryExtent::Empty();
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

vector<string> GeometryTypeSet::Format(bool geoparquet_case) const {
	vector<string> result;
	for (idx_t i = 0; i < 4; i++) {
		if (bits[i] == 0) {
			continue;
		}

		Scan([&](GeometryType gtype, VertexType vtype) {
			string type_str;
			switch (gtype) {
			case GeometryType::POINT:
				type_str = geoparquet_case ? "Point" : "POINT";
				break;
			case GeometryType::LINESTRING:
				type_str = geoparquet_case ? "LineString" : "LINESTRING";
				break;
			case GeometryType::POLYGON:
				type_str = geoparquet_case ? "Polygon" : "POLYGON";
				break;
			case GeometryType::MULTIPOINT:
				type_str = geoparquet_case ? "MultiPoint" : "MULTIPOINT";
				break;
			case GeometryType::MULTILINESTRING:
				type_str = geoparquet_case ? "MultiLineString" : "MULTILINESTRING";
				break;
			case GeometryType::MULTIPOLYGON:
				type_str = geoparquet_case ? "MultiPolygon" : "MULTIPOLYGON";
				break;
			case GeometryType::GEOMETRYCOLLECTION:
				type_str = geoparquet_case ? "GeometryCollection" : "GEOMETRYCOLLECTION";
				break;
			default:
				break;
			}
			if (type_str.empty()) {
				return;
			}
			switch (vtype) {
			case VertexType::XY:
				type_str += geoparquet_case ? " XY" : "_XY";
				break;
			case VertexType::XYZ:
				type_str += geoparquet_case ? " XYZ" : "_XYZ";
				break;
			case VertexType::XYM:
				type_str += geoparquet_case ? " XYM" : "_XYM";
				break;
			case VertexType::XYZM:
				type_str += geoparquet_case ? " XYZM" : "_XYZM";
				break;
			default:
				break;
			}
			result.push_back(std::move(type_str));
		});
	}
	return result;
}

string GeometryStats::ToString(const BaseStatistics &stats) {
	const auto &geometry_data = GeometryStats::GetDataUnsafe(stats);

	string type_str = "[";
	auto types = geometry_data.types.Format(false);
	for (idx_t i = 0; i < types.size(); i++) {
		if (i > 0) {
			type_str += ", ";
		}
		type_str += types[i];
	}
	type_str += "]";

	const auto extent =
	    StringUtil::Format("[XMin: %g, XMax: %g, YMin: %g, YMax: %g]", geometry_data.bbox.min_x,
	                       geometry_data.bbox.max_x, geometry_data.bbox.min_y, geometry_data.bbox.max_y);
	return StringUtil::Format("[Extent: %s, Types: %s]", extent, type_str);
}

void GeometryStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	serializer.WriteProperty(200, "xmin", geometry_data.bbox.min_x);
	serializer.WriteProperty(201, "xmax", geometry_data.bbox.max_x);
	serializer.WriteProperty(202, "ymin", geometry_data.bbox.min_y);
	serializer.WriteProperty(203, "ymax", geometry_data.bbox.max_y);

	serializer.WritePropertyWithDefault<uint32_t>(210, "xy_bitset", geometry_data.types.bits[0]);
	serializer.WritePropertyWithDefault<uint32_t>(211, "xyz_bitset", geometry_data.types.bits[1]);
	serializer.WritePropertyWithDefault<uint32_t>(212, "xym_bitset", geometry_data.types.bits[2]);
	serializer.WritePropertyWithDefault<uint32_t>(213, "xyzm_bitset", geometry_data.types.bits[3]);
}

void GeometryStats::Deserialize(Deserializer &deserializer, BaseStatistics &stats) {
	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	geometry_data.bbox.min_x = deserializer.ReadProperty<double>(200, "xmin");
	geometry_data.bbox.max_x = deserializer.ReadProperty<double>(201, "xmax");
	geometry_data.bbox.min_y = deserializer.ReadProperty<double>(202, "ymin");
	geometry_data.bbox.max_y = deserializer.ReadProperty<double>(203, "ymax");

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
		geometry_data.bbox.Extend(bounds);
	}

	// Add geometry and vertex type to the statistics
	const auto type = Geometry::GetGeometryType(value);
	geometry_data.types.Add(type.first, type.second);
}

void GeometryStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (stats.GetType().id() != other.GetType().id()) {
		return;
	}

	auto &geometry_data = GeometryStats::GetDataUnsafe(stats);
	auto &other_geometry_data = GeometryStats::GetDataUnsafe(other);

	geometry_data.bbox.Extend(other_geometry_data.bbox);
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

	// TODO: Make this more robust (check for nulls and whatnot)
	if (!geometry_data.bbox.Intersects(bounds)) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

} // namespace duckdb
