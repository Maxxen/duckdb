#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb {

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunction function)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunctionSet set)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(std::move(set)) {
	name = functions.name;
	catalog = set.catalog;
	schema = set.schema;
	for (auto &func : functions.functions) {
		func.function.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateAggregateFunctionInfo::Copy() const {
	auto result = make_uniq<CreateAggregateFunctionInfo>(functions);
	CopyFunctionProperties(*result);
	return std::move(result);
}

} // namespace duckdb
