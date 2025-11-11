#include "duckdb/parser/parsed_data/create_cast_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

CreateCastInfo::CreateCastInfo() : CreateInfo(CatalogType::CAST_ENTRY) {
}

unique_ptr<CreateInfo> CreateCastInfo::Copy() const {
	auto result = make_uniq<CreateCastInfo>();
	CopyProperties(*result);
	result->source = source;
	result->target = target;
	result->cast_cost = cast_cost;
	return std::move(result);
}

string CreateCastInfo::ToString() const {
	string result = GetCreatePrefix("CAST");
	result += " FROM " + source.ToString();
	result += " TO " + target.ToString();
	result += " COST " + std::to_string(cast_cost) + ";";
	return result;
}

} // namespace duckdb
