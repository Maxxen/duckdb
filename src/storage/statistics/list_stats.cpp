#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void ListStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[1]);
	BaseStatistics::Construct(stats.child_stats[0], ListType::GetChildType(stats.GetType()));
}

BaseStatistics ListStats::CreateUnknown(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(child_type));

	auto &data = GetDataUnsafe(result);
	data.has_min_length = false;
	data.has_max_length = false;
	data.min_length = 0;
	data.max_length = 0;

	return result;
}

BaseStatistics ListStats::CreateEmpty(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(child_type));

	auto &data = GetDataUnsafe(result);
	data.has_min_length = true;
	data.has_max_length = true;
	data.min_length = NumericLimits<uint64_t>::Maximum();
	data.max_length = NumericLimits<uint64_t>::Minimum();

	return result;
}

void ListStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	D_ASSERT(stats.child_stats);
	D_ASSERT(other.child_stats);
	stats.child_stats[0].Copy(other.child_stats[0]);
	auto &data = GetDataUnsafe(stats);
	const auto &other_data = GetDataUnsafe(other);

	data.has_min_length = other_data.has_min_length;
	data.has_max_length = other_data.has_max_length;
	data.min_length = other_data.min_length;
	data.max_length = other_data.max_length;
}

const BaseStatistics &ListStats::GetChildStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}
BaseStatistics &ListStats::GetChildStats(BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}

void ListStats::SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	if (!new_stats) {
		stats.child_stats[0].Copy(BaseStatistics::CreateUnknown(ListType::GetChildType(stats.GetType())));
	} else {
		stats.child_stats[0].Copy(*new_stats);
	}
}

void ListStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	auto &child_stats = ListStats::GetChildStats(stats);
	auto &other_child_stats = ListStats::GetChildStats(other);
	child_stats.Merge(other_child_stats);

	auto &data = GetDataUnsafe(stats);
	auto &other_data = GetDataUnsafe(other);

	if (data.has_min_length && other_data.has_min_length) {
		data.min_length = MinValue(data.min_length, other_data.min_length);
	} else {
		data.has_min_length = false;
		data.min_length = 0;
	}

	if (data.has_max_length && other_data.has_max_length) {
		data.max_length = MaxValue(data.max_length, other_data.max_length);
	} else {
		data.has_max_length = false;
		data.max_length = 0;
	}
}

void ListStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &child_stats = ListStats::GetChildStats(stats);
	serializer.WriteProperty(200, "child_stats", child_stats);
	serializer.WritePropertyWithDefault(201, "has_min_length", GetDataUnsafe(stats).has_min_length, false);
	serializer.WritePropertyWithDefault(202, "has_max_length", GetDataUnsafe(stats).has_max_length, false);
	serializer.WritePropertyWithDefault(203, "min_length", GetDataUnsafe(stats).min_length, 0ULL);
	serializer.WritePropertyWithDefault(204, "max_length", GetDataUnsafe(stats).max_length, 0ULL);
}

void ListStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);

	// Push the logical type of the child type to the deserialization context
	deserializer.Set<const LogicalType &>(child_type);
	base.child_stats[0].Copy(deserializer.ReadProperty<BaseStatistics>(200, "child_stats"));
	deserializer.Unset<LogicalType>();

	auto &data = GetDataUnsafe(base);
	data.has_min_length = deserializer.ReadPropertyWithExplicitDefault<bool>(201, "has_min_length", false);
	data.has_max_length = deserializer.ReadPropertyWithExplicitDefault<bool>(202, "has_max_length", false);
	data.min_length = deserializer.ReadPropertyWithExplicitDefault<uint64_t>(203, "min_length", 0ULL);
	data.max_length = deserializer.ReadPropertyWithExplicitDefault<uint64_t>(204, "max_length", 0ULL);
}

string ListStats::ToString(const BaseStatistics &stats) {
	auto &child_stats = ListStats::GetChildStats(stats);
	return StringUtil::Format("[Min Length: %s, Max Length: %s, Data: [%s]]",
	                          ListStats::MinLengthOrNull(stats).ToString(),
	                          ListStats::MaxLengthOrNull(stats).ToString(), child_stats.ToString());
}

void ListStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_stats = ListStats::GetChildStats(stats);
	auto &child_entry = ListVector::GetEntry(vector);
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
	idx_t total_list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				total_list_count++;
			}
		}
	}
	SelectionVector list_sel(total_list_count);
	idx_t list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				list_sel.set_index(list_count++, list.offset + list_idx);
			}
		}
	}

	child_stats.Verify(child_entry, list_sel, list_count);
}

void ListStats::UpdateLength(BaseStatistics &stats, const uint64_t length) {
	auto &data = GetDataUnsafe(stats);
	if (data.has_min_length) {
		data.min_length = MinValue(data.min_length, length);
	} else {
		data.has_min_length = true;
		data.min_length = length;
	}

	if (data.has_max_length) {
		data.max_length = MaxValue(data.max_length, length);
	} else {
		data.has_max_length = true;
		data.max_length = length;
	}
}

void ListStats::UpdateLength(BaseStatistics &stats, const uint64_t max_length, const uint64_t min_length) {
	auto &data = GetDataUnsafe(stats);
	if (data.has_min_length) {
		data.min_length = MinValue(data.min_length, min_length);
	} else {
		data.has_min_length = true;
		data.min_length = min_length;
	}

	if (data.has_max_length) {
		data.max_length = MaxValue(data.max_length, max_length);
	} else {
		data.has_max_length = true;
		data.max_length = max_length;
	}
}

ListStatsData &ListStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::LIST_STATS);
	return stats.stats_union.list_data;
}

const ListStatsData &ListStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::LIST_STATS);
	return stats.stats_union.list_data;
}

uint64_t ListStats::GetMinLengthUnsafe(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	D_ASSERT(data.has_min_length);
	return data.min_length;
}

uint64_t ListStats::GetMaxLengthUnsafe(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	D_ASSERT(data.has_max_length);
	return data.max_length;
}

Value ListStats::MinLengthOrNull(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	if (!data.has_min_length) {
		return Value(LogicalType::UBIGINT);
	}
	return Value::UBIGINT(data.min_length);
}
Value ListStats::MaxLengthOrNull(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	if (!data.has_max_length) {
		return Value(LogicalType::UBIGINT);
	}
	return Value::UBIGINT(data.max_length);
}

bool ListStats::HasMinLength(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	return data.has_min_length;
}

bool ListStats::HasMaxLength(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	return data.has_max_length;
}

void ListStats::SetMinLength(BaseStatistics &stats, uint64_t length) {
	auto &data = GetDataUnsafe(stats);
	data.has_min_length = true;
	data.min_length = length;
}

void ListStats::SetMaxLength(BaseStatistics &stats, uint64_t length) {
	auto &data = GetDataUnsafe(stats);
	data.has_max_length = true;
	data.max_length = length;
}

template <>
inline void BaseStatistics::UpdateNumericStats<list_entry_t>(list_entry_t new_value) {
	ListStats::UpdateLength(*this, new_value.length);
}

} // namespace duckdb
