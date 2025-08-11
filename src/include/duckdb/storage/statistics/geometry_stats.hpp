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

	// Add a type to the set
	void Add(GeometryType type) {
		bitset |= (1U << static_cast<uint32_t>(type));
	}

	// Check if any geometry in the column is of the specified type
	bool Any(GeometryType type) const {
		return (bitset & (1U << static_cast<uint32_t>(type))) != 0;
	}

	// Check if all geometry in the column is of the specified type
	bool All(GeometryType type) const {
		return bitset == (1U << static_cast<uint32_t>(type));
	}

	// Reset to unknown, i.e. all types are set
	void SetUnknown() {
		bitset = 0xFFFFFFFF;
	}

	// Reset to empty, i.e. no types are set
	void SetEmpty() {
		bitset = 0;
	}

	// Reset to a specific type
	void Set(GeometryType type) {
		bitset = (1U << static_cast<uint32_t>(type));
	}

	// Check if the set is unknown, i.e. all types are set
	bool IsUnknown() const {
		return bitset == 0xFFFFFFFF;
	}

	// Check if the set is empty, i.e. no types are set
	bool IsEmpty() const {
		return bitset == 0;
	}

	// Merge with another set
	void Merge(const GeometryTypeSet &other) {
		bitset |= other.bitset;
	}
private:
	friend struct GeometryStats;
	uint32_t bitset; // Bitset to track which geometry types are present
};

// Tracks which geometry flags are present in the column
// Can contain false positives, but never false negatives
class GeometryFlagSet {
public:
	void Add(GeometryFlag flag) {
		bitset |= (1U << static_cast<uint32_t>(flag));
	}
	bool Any(GeometryFlag flag) const {
		return (bitset & (1U << static_cast<uint32_t>(flag))) != 0;
	}
	bool All(GeometryFlag flag) const {
		return bitset == (1U << static_cast<uint32_t>(flag));
	}
	void SetUnknown() {
		bitset = 0xFFFFFFFF;
	}
	void SetEmpty() {
		bitset = 0;
	}
	bool IsUnknown() const {
		return bitset == 0xFFFFFFFF;
	}
	bool IsEmpty() const {
		return bitset == 0;
	}
	void Merge(const GeometryFlagSet &other) {
		bitset |= other.bitset;
	}
private:
	friend struct GeometryStats;
	uint32_t bitset; // to track which geometry flags are present
};

struct GeometryStatsData {
	// TODO: Put this in unique_ptr if it gets too big
	// We got around 40 bytes to work with total before it gets larger than numeric stats
	GeometryExtent bounds;
	GeometryTypeSet types;
	GeometryFlagSet flags;
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
