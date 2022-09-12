#include "duckdb/storage/statistics/union_statistics.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

UnionStatistics::UnionStatistics(LogicalType type_p) : BaseStatistics(move(type_p), StatisticsType::LOCAL_STATS) {
	D_ASSERT(type.InternalType() == PhysicalType::UNION);
	InitializeBase();

	auto &child_types = UnionType::GetChildTypes(type);
	child_stats.resize(child_types.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		child_stats[i] = BaseStatistics::CreateEmpty(child_types[i].second, StatisticsType::LOCAL_STATS);
	}
}

void UnionStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const UnionStatistics &)other_p;
	D_ASSERT(other.child_stats.size() == child_stats.size());
	for (idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i] && other.child_stats[i]) {
			child_stats[i]->Merge(*other.child_stats[i]);
		} else {
			child_stats[i].reset();
		}
	}
}

// LCOV_EXCL_START
FilterPropagateResult UnionStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	throw InternalException("Union zonemaps are not supported yet");
}
// LCOV_EXCL_STOP

unique_ptr<BaseStatistics> UnionStatistics::Copy() const {
	auto result = make_unique<UnionStatistics>(type);
	result->CopyBase(*this);

	for (idx_t i = 0; i < child_stats.size(); i++) {
		result->child_stats[i] = child_stats[i] ? child_stats[i]->Copy() : nullptr;
	}
	return move(result);
}

void UnionStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint32_t>(child_stats.size());
	auto &serializer = writer.GetSerializer();
	for (idx_t i = 0; i < child_stats.size(); i++) {
		serializer.Write<bool>(child_stats[i] ? true : false);
		if (child_stats[i]) {
			child_stats[i]->Serialize(serializer);
		}
	}
}

unique_ptr<BaseStatistics> UnionStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::UNION);
	auto result = make_unique<UnionStatistics>(move(type));
	auto &child_types = UnionType::GetChildTypes(result->type);

	auto child_type_count = reader.ReadRequired<uint32_t>();
	if (child_types.size() != child_type_count) {
		throw InternalException("Union stats deserialization failure: child count does not match type count!");
	}
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto has_child = source.Read<bool>();
		if (has_child) {
			result->child_stats[i] = BaseStatistics::Deserialize(source, child_types[i].second);
		} else {
			result->child_stats[i].reset();
		}
	}
	return move(result);
}

string UnionStatistics::ToString() const {
	string result;
	result += " {";
	auto &child_types = UnionType::GetChildTypes(type);
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + (child_stats[i] ? child_stats[i]->ToString() : "No Stats");
	}
	result += "}";
	result += BaseStatistics::ToString();
	return result;
}

void UnionStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	auto &child_entries = UnionVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (child_stats[i]) {
			child_stats[i]->Verify(*child_entries[i], sel, count);
		}
	}
}

} // namespace duckdb
