#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/type_name.hpp"
#include "duckdb/main/client_context.hpp"

#include <map>

using namespace duckdb;

TEST_CASE("Type names compare, hash and print", "[api][type_name]") {
	REQUIRE(TypeName(LogicalType::INTEGER) == TypeName(Identifier("integer")));
	REQUIRE(TypeName(LogicalType::INTEGER).Hash() == TypeName(Identifier("INTEGER")).Hash());
	REQUIRE(TypeName(LogicalType::INTEGER) != TypeName(LogicalType::BIGINT));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::INTEGER)) == TypeName(LogicalType::LIST(LogicalType::INTEGER)));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::INTEGER)) != TypeName(LogicalType::LIST(LogicalType::BIGINT)));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::TEMPLATE("T"))).ToString() == "T[]");
	REQUIRE(TypeName(LogicalType::DECIMAL(18, 3)).ToString() == "DECIMAL(18, 3)");
	REQUIRE(TypeName(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::TEMPLATE("V"))).ToString() ==
	        "MAP(VARCHAR, V)");
	REQUIRE(TypeName(LogicalType::STRUCT({{"a", LogicalType::INTEGER}})).ToString() == "STRUCT(a INTEGER)");
}

TEST_CASE("Every type a registered function declares binds by its name", "[api][type_name]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	vector<string> failures;
	// declared type -> the type its name binds to, where they differ
	std::map<string, string> bound_differently;
	std::map<string, string> printed_differently;
	idx_t parameter_count = 0;
	idx_t unbound_without_context = 0;

	auto check_function = [&](const SimpleFunction &function) {
		auto &signature = function.GetSignature();
		for (auto &param : signature.GetParameters()) {
			parameter_count++;
			auto &declared = param.GetType();
			TypeName name(declared);
			if (name.ToString() != declared.ToString()) {
				printed_differently[declared.ToString()] = name.ToString();
			}
			try {
				auto bound = FunctionBinder::BindTypeName(&context, name, signature);
				if (bound != declared || bound.GetAlias() != declared.GetAlias()) {
					bound_differently[declared.ToString()] = bound.ToString();
				}
			} catch (std::exception &ex) {
				failures.push_back(function.GetName().GetIdentifierName() + ": " + declared.ToString() + " as " +
				                   name.ToString() + ": " + ErrorData(ex).RawMessage());
			}
			try {
				FunctionBinder::BindTypeName(nullptr, name, signature);
			} catch (std::exception &ex) {
				unbound_without_context++;
			}
		}
	};
	auto check_functions = [&](const BaseFunctionSet &functions) {
		for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
			check_function(functions.GetOverload(i));
		}
	};

	for (auto &schema : Catalog::GetAllSchemas(context)) {
		schema.get().Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::SCALAR_FUNCTION_ENTRY) {
				check_functions(entry.Cast<ScalarFunctionCatalogEntry>().functions);
			} else if (entry.type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
				check_functions(entry.Cast<AggregateFunctionCatalogEntry>().functions);
			} else if (entry.type == CatalogType::WINDOW_FUNCTION_ENTRY) {
				check_functions(entry.Cast<WindowFunctionCatalogEntry>().functions);
			}
		});
		schema.get().Scan(context, CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::TABLE_FUNCTION_ENTRY) {
				check_functions(entry.Cast<TableFunctionCatalogEntry>().functions);
			}
		});
		schema.get().Scan(context, CatalogType::PRAGMA_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::PRAGMA_FUNCTION_ENTRY) {
				check_functions(entry.Cast<PragmaFunctionCatalogEntry>().functions);
			}
		});
	}

	Printer::Print(StringUtil::Format("parameters: %llu, not bound without a context: %llu", parameter_count,
	                                  unbound_without_context));
	for (auto &entry : bound_differently) {
		Printer::Print("bound differently: " + entry.first + " -> " + entry.second);
	}
	for (auto &entry : printed_differently) {
		Printer::Print("printed differently: " + entry.first + " -> " + entry.second);
	}
	for (auto &failure : failures) {
		Printer::Print("FAILED: " + failure);
	}
	REQUIRE(parameter_count > 1000);
	REQUIRE(failures.empty());
}
