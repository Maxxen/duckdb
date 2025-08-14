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
	void Add(GeometryType gtype, VertexType vtype) {
		bits[static_cast<uint8_t>(vtype)] |= (1U << static_cast<uint32_t>(gtype));
	}

	// Add the specified geometry type to all vertex types
	void Add(GeometryType gtype) {
		for (auto &set : bits) {
			set |= (1U << static_cast<uint32_t>(gtype));
		}
	}

	// Add the specified vertex type to all geometry types
	void Add(VertexType vtype) {
		bits[static_cast<uint8_t>(vtype)] = bits[0] | bits[1] | bits[2] | bits[3];
	}

	// Check if any geometry of the specified type and vertex type is present
	bool Any(GeometryType gtype, VertexType vtype) const {
		return (bits[static_cast<uint8_t>(vtype)] & (1U << static_cast<uint32_t>(gtype))) != 0;
	}

	// Check if any geometry of the specified type is present
	bool Any(GeometryType gtype) const {
		for (auto &set : bits) {
			if ((set & (1U << static_cast<uint32_t>(gtype))) != 0) {
				return true;
			}
		}
		return false;
	}

	// Check if any geometry of the specified vertex type is present
	bool Any(VertexType vtype) {
		return bits[static_cast<uint8_t>(vtype)] != 0;
	}

	// Check if all geometries are of the specified geometry and vertex type
	bool All(GeometryType gtype, VertexType vtype) const {
		for (idx_t i = 0; i < 4; i++) {
			if (i == static_cast<uint8_t>(vtype)) {
				if (bits[i] != (1U << static_cast<uint32_t>(gtype))) {
					return false;
				}
			} else {
				if (bits[i] & (1U << static_cast<uint32_t>(gtype))) {
					return false;
				}
			}
		}
		return true;
	}

	// Check if all geometries are of the specified geometry type
	bool All(GeometryType gtype) const {
		for (auto &set : bits) {
			if (set != (1U << static_cast<uint32_t>(gtype))) {
				return false;
			}
		}
		return true;
	}

	// Check if all geometries are of the specified vertex type
	bool All(VertexType vtype) const {
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
			set = 0xFFFFFFFF;
		}
	}

	// Reset to empty, i.e. no geometry types or vertex types are set
	void SetEmpty() {
		for (auto &set : bits) {
			set = 0x00000000;
		}
	}

	// Reset to a specific geometry type and vertex type
	void Set(GeometryType gtype, VertexType vtype) {
		for (idx_t i = 0; i < 4; i++) {
			if (i == static_cast<uint8_t>(vtype)) {
				bits[i] = (1U << static_cast<uint32_t>(gtype));
			} else {
				bits[i] = 0;
			}
		}
	}

	// Reset all vertex types to a specific geometry type
	void Set(GeometryType gtype) {
		for (auto &set : bits) {
			set = (1U << static_cast<uint32_t>(gtype));
		}
	}

	// Reset all geometries to a specific vertex type
	void Set(VertexType vtype) {
		bits[static_cast<uint8_t>(vtype)] = bits[0] | bits[1] | bits[2] | bits[3];
		for (idx_t i = 0; i < 4; i++) {
			if (i != static_cast<uint8_t>(vtype)) {
				bits[i] = 0;
			}
		}
	}

	bool IsEmpty() const {
		return bits[0] == 0x00000000 && bits[1] == 0x00000000 && bits[2] == 0x00000000 && bits[3] == 0x00000000;
	}

	bool IsUnknown() const {
		return bits[0] == 0xFFFFFFFF && bits[1] == 0xFFFFFFFF && bits[2] == 0xFFFFFFFF && bits[3] == 0xFFFFFFFF;
	}

	void Merge(const GeometryTypeSet &other) {
		for (idx_t i = 0; i < 4; i++) {
			bits[i] |= other.bits[i];
		}
	}

	template <class CALLBACK>
	void Scan(CALLBACK &&callback) const {
		for (idx_t i = 0; i < 4; i++) {
			if (bits[i] == 0) {
				continue;
			}
			for (uint32_t j = 0; j < sizeof(uint32_t) * 8; j++) {
				if ((bits[i] & (1U << j)) != 0) {
					callback(static_cast<GeometryType>(j), static_cast<VertexType>(i));
				}
			}
		}
	}

private:
	friend struct GeometryStats;
	uint32_t bits[4];
};

struct GeometryStatsData {
	// TODO: Put this in unique_ptr if it gets too big
	// We got around 40 bytes to work with total before it gets larger than numeric stats
	GeometryExtent bounds;
	GeometryTypeSet types;
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
