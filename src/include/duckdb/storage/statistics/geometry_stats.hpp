//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/geometry_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/numeric_stats_union.hpp"
#include "duckdb/common/array_ptr.hpp"
#include "duckdb/common/types/geometry.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

// Tracks which geometry types are present in the column
// Can contain false positives, but never false negatives
class GeometryTypeSet {
public:
	GeometryTypeSet() = default;

	// Add the specified geometry type and vertex type to the set
	void Add(GeometryType gtype) {
		const auto vtype = static_cast<uint8_t>(gtype.GetVertType());
		const auto ptype = static_cast<uint8_t>(gtype.GetPartType());
		bits[vtype] |= (uint8_t {1} << ptype);
	}

	// Add the specified geometry type to all vertex types
	void Add(GeometryPartType ptype) {
		for (auto &set : bits) {
			set |= (uint8_t {1} << static_cast<uint8_t>(ptype));
		}
	}

	// Add the specified vertex type to all geometry types
	void Add(GeometryVertexType vtype) {
		bits[static_cast<uint8_t>(vtype)] = bits[0] | bits[1] | bits[2] | bits[3];
	}

	// Check if any geometry of the specified type and vertex type is present
	bool Any(GeometryType gtype) const {
		const auto vtype = static_cast<uint8_t>(gtype.GetVertType());
		const auto ptype = static_cast<uint8_t>(gtype.GetPartType());
		return (bits[vtype] & (uint8_t {1} << ptype)) != 0;
	}

	// Check if any geometry of the specified type is present
	bool Any(GeometryPartType ptype) const {
		for (auto &set : bits) {
			if ((set & (uint8_t {1} << static_cast<uint8_t>(ptype))) != 0) {
				return true;
			}
		}
		return false;
	}

	// Check if any geometry of the specified vertex type is present
	bool Any(GeometryVertexType vtype) const {
		return bits[static_cast<uint8_t>(vtype)] != 0;
	}

	// Check if all geometries are of the specified geometry and vertex type
	bool All(GeometryType gtype) const {
		const auto vtype = static_cast<uint8_t>(gtype.GetVertType());
		const auto ptype = static_cast<uint8_t>(gtype.GetPartType());
		for (idx_t i = 0; i < 4; i++) {
			if (i == vtype) {
				if (bits[i] != (uint8_t {1} << ptype)) {
					return false;
				}
			} else {
				if (bits[i] & (uint8_t {1} << ptype)) {
					return false;
				}
			}
		}
		return true;
	}

	// Check if all geometries are of the specified geometry type
	bool All(GeometryPartType gtype) const {
		const auto ptype = static_cast<uint8_t>(gtype);
		for (auto &set : bits) {
			if (set != (uint8_t {1} << ptype)) {
				return false;
			}
		}
		return true;
	}

	// Check if all geometries are of the specified vertex type
	bool All(GeometryVertexType vtype) const {
		for (idx_t i = 0; i < 4; i++) {
			if (i == static_cast<uint8_t>(vtype)) {
				if (bits[i] == 0) {
					return false;
				}
			} else {
				if (bits[i] != 0) {
					return false;
				}
			}
		}
		return true;
	}

	// Reset to unknown, i.e. all geometry types and vertex types are set
	void SetUnknown() {
		for (auto &set : bits) {
			set = 0xFF;
		}
	}

	// Reset to empty, i.e. no geometry types or vertex types are set
	void SetEmpty() {
		for (auto &set : bits) {
			set = 0x00;
		}
	}

	// Reset to a specific geometry type and vertex type
	void Set(GeometryType gtype) {
		const auto vtype = static_cast<uint8_t>(gtype.GetVertType());
		const auto ptype = static_cast<uint8_t>(gtype.GetPartType());
		for (idx_t i = 0; i < 4; i++) {
			if (i == vtype) {
				bits[i] = static_cast<uint8_t>(uint8_t {1} << ptype);
			} else {
				bits[i] = 0;
			}
		}
	}

	// Reset all vertex types to a specific geometry type
	void Set(GeometryPartType gtype) {
		const auto ptype = static_cast<uint8_t>(gtype);
		for (auto &set : bits) {
			set = static_cast<uint8_t>(uint8_t {1} << ptype);
		}
	}

	// Reset all geometries to a specific vertex type
	void Set(GeometryVertexType vtype) {
		bits[static_cast<uint8_t>(vtype)] = bits[0] | bits[1] | bits[2] | bits[3];
		for (idx_t i = 0; i < 4; i++) {
			if (i != static_cast<uint8_t>(vtype)) {
				bits[i] = 0;
			}
		}
	}

	bool IsEmpty() const {
		return bits[0] == 0x00 && bits[1] == 0x00 && bits[2] == 0x00 && bits[3] == 0x00;
	}

	bool IsUnknown() const {
		return bits[0] == 0xFF && bits[1] == 0xFF && bits[2] == 0xFF && bits[3] == 0xFF;
	}

	void Merge(const GeometryTypeSet &other) {
		for (idx_t i = 0; i < 4; i++) {
			bits[i] |= other.bits[i];
		}
	}

	template <class FUNC>
	void Scan(FUNC &&callback) const {
		for (idx_t i = 0; i < 4; i++) {
			if (bits[i] == 0) {
				continue;
			}
			for (uint32_t j = 0; j < sizeof(uint8_t) * 8; j++) {
				if ((bits[i] & (uint8_t {1} << j)) != 0) {
					const auto vtype = static_cast<GeometryVertexType>(i);
					const auto ptype = static_cast<GeometryPartType>(j);
					callback(GeometryType(ptype, vtype));
				}
			}
		}
	}

	static GeometryTypeSet Unknown() {
		GeometryTypeSet result;
		result.SetUnknown();
		return result;
	}

	static GeometryTypeSet Empty() {
		GeometryTypeSet result;
		result.SetEmpty();
		return result;
	}

	vector<string> Format(bool geoparquet_case) const;

private:
	friend struct GeometryStats;
	uint8_t bits[4]; // One byte per vertex type, each bit represents a geometry type
};

struct GeometryStatsData {
	// TODO: Put this in unique_ptr if it gets too big
	// We got around 40 bytes to work with total before it gets larger than numeric stats
	GeometryExtent bbox;
	GeometryTypeSet types;

	static GeometryStatsData Unknown() {
		GeometryStatsData result;
		result.bbox = GeometryExtent::Unknown();
		result.types = GeometryTypeSet::Unknown();
		return result;
	}
	static GeometryStatsData Empty() {
		GeometryStatsData result;
		result.bbox = GeometryExtent::Empty();
		result.types = GeometryTypeSet::Empty();
		return result;
	}
};

struct GeometryStats {
	//! Unknown statistics
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static GeometryStatsData &GetDataUnsafe(BaseStatistics &stats);
	DUCKDB_API static const GeometryStatsData &GetDataUnsafe(const BaseStatistics &stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &stats);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static GeometryTypeSet &GetTypes(BaseStatistics &stats);
	DUCKDB_API static const GeometryTypeSet &GetTypes(const BaseStatistics &stats);

	DUCKDB_API static void Update(BaseStatistics &stats, const string_t &value);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	DUCKDB_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, const Value &value);
};

} // namespace duckdb
