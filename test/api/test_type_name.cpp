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

using namespace duckdb;

TEST_CASE("Type names compare, hash and print", "[api][type_name]") {
	REQUIRE(TypeName(LogicalType::INTEGER) == TypeName(Identifier("integer")));
	REQUIRE(TypeName(LogicalType::INTEGER) == TypeName(Identifier("INT")));
	REQUIRE(TypeName(LogicalType::INTEGER).Hash() == TypeName(Identifier("INTEGER")).Hash());
	REQUIRE(TypeName(LogicalType::INTEGER) != TypeName(LogicalType::BIGINT));
	REQUIRE(TypeName(LogicalType::INTEGER).IsBuiltinType(LogicalTypeId::INTEGER));
	REQUIRE(!TypeName(Identifier("T")).IsBuiltinType(LogicalTypeId::INTEGER));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::INTEGER)) == TypeName(LogicalType::LIST(LogicalType::INTEGER)));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::INTEGER)) != TypeName(LogicalType::LIST(LogicalType::BIGINT)));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::INTEGER)).Contains("INTEGER"));
	REQUIRE(!TypeName(LogicalType::LIST(LogicalType::INTEGER)).Contains("BIGINT"));
	REQUIRE(TypeName(LogicalType::LIST(LogicalType::TEMPLATE("T"))).ToString() == "T[]");
	REQUIRE(TypeName(LogicalType::DECIMAL(18, 3)).ToString() == "DECIMAL(18,3)");
	REQUIRE(TypeName(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::TEMPLATE("V"))).ToString() ==
	        "MAP(VARCHAR, V)");
	REQUIRE(TypeName(LogicalType::STRUCT({{"a", LogicalType::INTEGER}})).ToString() == "STRUCT(a INTEGER)");
}

TEST_CASE("A type binds by its name to itself", "[api][type_name]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	auto types = LogicalType::AllTypes();
	types.push_back(LogicalType::SQLNULL);
	types.push_back(LogicalType::LAMBDA);
	types.push_back(LogicalType::TABLE);
	types.push_back(LogicalType::ANY);
	types.push_back(LogicalType::DECIMAL(18, 3));
	types.push_back(LogicalType::LIST(LogicalType::INTEGER));
	types.push_back(LogicalType::LIST(LogicalType::ANY));
	types.push_back(LogicalType::ARRAY(LogicalType::FLOAT, 3));
	types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::DOUBLE)));
	types.push_back(LogicalType::STRUCT({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
	types.push_back(LogicalType::UNION({{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}}));
	types.push_back(LogicalType::JSON());
	types.push_back(LogicalType::VARIANT());
	types.push_back(LogicalType::GEOMETRY());
	for (auto &type : types) {
		if (!type.IsComplete() && type.id() != LogicalTypeId::ANY) {
			// a kind of type, e.g. a DECIMAL without width and scale, is not a type a signature declares
			continue;
		}
		INFO(type.ToString());
		TypeName name(type);
		REQUIRE(name.ToString() == type.ToString());
		auto bound = FunctionBinder::BindTypeName(&context, name);
		REQUIRE(bound == type);
		REQUIRE(bound.GetAlias() == type.GetAlias());
		if (type.GetAlias().empty()) {
			// a built-in type also binds without a context
			REQUIRE(FunctionBinder::BindTypeName(nullptr, name) == type);
		}
	}
}

TEST_CASE("Every type a registered function declares binds", "[api][type_name]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;

	vector<string> failures;
	idx_t parameter_count = 0;
	auto check_type = [&](const SimpleFunction &function, const TypeName &name) {
		try {
			FunctionBinder::BindTypePattern(&context, name, function.GetSignature());
		} catch (std::exception &ex) {
			failures.push_back(function.ToString() + ": " + name.ToString() + ": " + ErrorData(ex).RawMessage());
		}
	};
	auto check_functions = [&](const BaseFunctionSet &functions) {
		for (idx_t i = 0; i < functions.GetOverloadCount(); i++) {
			auto &function = functions.GetOverload(i);
			auto &signature = function.GetSignature();
			for (auto &param : signature.GetParameters()) {
				parameter_count++;
				check_type(function, param.GetTypeName());
			}
			if (signature.GetReturnType().IsType()) {
				check_type(function, signature.GetReturnType().GetTypeName());
			}
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
	INFO(StringUtil::Join(failures, "\n"));
	REQUIRE(parameter_count > 1000);
	REQUIRE(failures.empty());
}
