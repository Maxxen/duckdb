#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

namespace duckdb {

CreatePragmaFunctionInfo::CreatePragmaFunctionInfo(PragmaFunction function)
    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}
CreatePragmaFunctionInfo::CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions_p)
    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(std::move(functions_p)) {
	this->name = std::move(name);
	if (!functions_p.catalog.empty()) {
		catalog = functions_p.catalog;
	}
	if (!functions_p.schema.empty()) {
		schema = functions_p.schema;
	}
	for (auto &func : functions.functions) {
		func.GetFunctionMutable().name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreatePragmaFunctionInfo::Copy() const {
	auto result = make_uniq<CreatePragmaFunctionInfo>(functions.name, functions);
	CopyFunctionProperties(*result);
	return std::move(result);
}

} // namespace duckdb
