//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

//! A window function in the catalog
class WindowFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::WINDOW_FUNCTION_ENTRY;
	static constexpr const char *Name = "aggregate function";

public:
	WindowFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateWindowFunctionInfo &info)
	    : FunctionEntry(CatalogType::WINDOW_FUNCTION_ENTRY, catalog, schema, info), functions(info.functions) {
		for (auto &function : functions.functions) {
			function.catalog_name = catalog.GetAttached().GetName();
			function.schema_name = schema.name;
		}
	}

	//! The window functions
	WindowFunctionSet functions;
};
} // namespace duckdb
