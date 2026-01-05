//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/list_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct ListStatsData {
	bool has_min_length;
	bool has_max_length;
	uint64_t max_length;
	uint64_t min_length;
};

struct ListStats {
	DUCKDB_API static void Construct(BaseStatistics &stats);
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static const BaseStatistics &GetChildStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetChildStats(BaseStatistics &stats);
	DUCKDB_API static void SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	DUCKDB_API static void UpdateLength(BaseStatistics &stats, const uint64_t length);
	DUCKDB_API static void UpdateLength(BaseStatistics &stats, const uint64_t min_length, const uint64_t max_length);

	DUCKDB_API static uint64_t GetMinLengthUnsafe(const BaseStatistics &stats);
	DUCKDB_API static uint64_t GetMaxLengthUnsafe(const BaseStatistics &stats);
	DUCKDB_API static Value MinLengthOrNull(const BaseStatistics &stats);
	DUCKDB_API static Value MaxLengthOrNull(const BaseStatistics &stats);
	DUCKDB_API static bool HasMinLength(const BaseStatistics &stats);
	DUCKDB_API static bool HasMaxLength(const BaseStatistics &stats);
	DUCKDB_API static void SetMinLength(BaseStatistics &stats, uint64_t length);
	DUCKDB_API static void SetMaxLength(BaseStatistics &stats, uint64_t length);

private:
	static ListStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const ListStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
