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
	REQUIRE(TypeName::LIST(TypeName::VARIABLE("T")).ToString() == "T[]");
	REQUIRE(TypeName(LogicalType::DECIMAL(18, 3)).ToString() == "DECIMAL(18,3)");
	REQUIRE(TypeName::MAP(LogicalType::VARCHAR, TypeName::VARIABLE("V")).ToString() == "MAP(VARCHAR, V)");
	REQUIRE(TypeName(LogicalType::STRUCT({{"a", LogicalType::INTEGER}})).ToString() == "STRUCT(a INTEGER)");
}

TEST_CASE("The expansion of a pack is a node of its own that wraps a pattern", "[api][type_name]") {
	const auto pack = TypeName::PACK("T");
	const auto named_pack = TypeName::NAMED_PACK("KV");
	const auto variable = TypeName::VARIABLE("T");
	// a name is a type name or an expansion
	REQUIRE(pack.IsExpansion());
	REQUIRE(named_pack.IsExpansion());
	REQUIRE(!variable.IsExpansion());
	REQUIRE(!TypeName(LogicalType::INTEGER).IsExpansion());
	REQUIRE(!TypeName::LIST(pack).IsExpansion());
	REQUIRE(pack.GetExpansionKind() == TypeMultiplicity::POSITIONAL_PACK);
	REQUIRE(named_pack.GetExpansionKind() == TypeMultiplicity::NAMED_PACK);
	// the pack is referenced by its name inside the expansion: "*T" expands the pattern T
	REQUIRE(pack.GetPattern() == variable);
	REQUIRE(named_pack.GetPattern() == TypeName::VARIABLE("KV"));
	REQUIRE(pack == TypeName::EXPAND(variable));
	REQUIRE(named_pack == TypeName::EXPAND_NAMED(TypeName::VARIABLE("KV")));

	// an expansion is not a type name: it has no name and no arguments, and it is not a built-in type
	REQUIRE_THROWS_WITH(pack.GetName(), Catch::Contains("An expansion is not a type name"));
	REQUIRE_THROWS_WITH(pack.GetArguments(), Catch::Contains("An expansion is not a type name"));
	REQUIRE_THROWS_WITH(pack.HasArguments(), Catch::Contains("An expansion is not a type name"));
	REQUIRE(pack.GetBuiltinType() == LogicalTypeId::INVALID);
	REQUIRE(!pack.IsBuiltinType(LogicalTypeId::INTEGER));
	REQUIRE(!TypeName::EXPAND(LogicalType::INTEGER).IsBuiltinType(LogicalTypeId::INTEGER));
	// and a type name has no pattern
	REQUIRE_THROWS_WITH(variable.GetPattern(), Catch::Contains("Type name 'T' is not an expansion"));
	REQUIRE_THROWS_WITH(variable.GetExpansionKind(), Catch::Contains("Type name 'T' is not an expansion"));

	// an expansion is written with its stars
	REQUIRE(pack.ToString() == "*T");
	REQUIRE(named_pack.ToString() == "**KV");
	REQUIRE(variable.ToString() == "T");
	const auto tuple = TypeName::TUPLE({pack});
	const TypeName structure(Identifier("STRUCT"), {named_pack});
	REQUIRE(tuple.ToString() == "TUPLE(*T)");
	REQUIRE(structure.ToString() == "STRUCT(**KV)");
	// at any depth, and next to what prints as before
	REQUIRE(TypeName::LIST(tuple).ToString() == "TUPLE(*T)[]");
	REQUIRE(TypeName::MAP(TypeName::VARIABLE("U"), structure).ToString() == "MAP(U, STRUCT(**KV))");
	REQUIRE(TypeName::TUPLE({variable}).ToString() == "TUPLE(T)");
	// around the pattern, if that is not a name by itself
	const auto lists = TypeName::EXPAND(TypeName::LIST(variable));
	REQUIRE(lists.ToString() == "*(T[])");
	REQUIRE(TypeName::EXPAND_NAMED(TypeName::LIST(TypeName::VARIABLE("KV"))).ToString() == "**(KV[])");
	REQUIRE(TypeName::EXPAND(TypeName::MAP(LogicalType::VARCHAR, variable)).ToString() == "*(MAP(VARCHAR, T))");
	REQUIRE(TypeName::EXPAND(pack).ToString() == "*(*T)");
	// which is another thing than a list of the expansion
	REQUIRE(TypeName::LIST(pack).ToString() == "(*T)[]");
	REQUIRE(TypeName::ARRAY(pack, 3).ToString() == "(*T)[3]");
	REQUIRE(TypeName::TUPLE({lists}).ToString() == "TUPLE(*(T[]))");

	// identity: T, *T and **T are three things
	REQUIRE(pack == TypeName::PACK("T"));
	REQUIRE(pack.Hash() == TypeName::PACK("T").Hash());
	REQUIRE(pack != variable);
	REQUIRE(pack.Hash() != variable.Hash());
	REQUIRE(pack != TypeName::NAMED_PACK("T"));
	REQUIRE(pack.Hash() != TypeName::NAMED_PACK("T").Hash());
	REQUIRE(pack != TypeName::PACK("U"));
	REQUIRE(tuple == TypeName::TUPLE({TypeName::PACK("T")}));
	REQUIRE(tuple != TypeName::TUPLE({variable}));
	REQUIRE(tuple.Hash() != TypeName::TUPLE({variable}).Hash());
	REQUIRE(tuple != TypeName::TUPLE({TypeName::NAMED_PACK("T")}));
	REQUIRE(tuple.Hash() != TypeName::TUPLE({TypeName::NAMED_PACK("T")}).Hash());
	// and so are "*(T[])", "(*T)[]" and "T[]"
	REQUIRE(lists == TypeName::EXPAND(TypeName::LIST(TypeName::VARIABLE("T"))));
	REQUIRE(lists.Hash() == TypeName::EXPAND(TypeName::LIST(TypeName::VARIABLE("T"))).Hash());
	REQUIRE(lists != TypeName::LIST(pack));
	REQUIRE(lists.Hash() != TypeName::LIST(pack).Hash());
	REQUIRE(lists != TypeName::LIST(variable));
	REQUIRE(lists.Hash() != TypeName::LIST(variable).Hash());
	REQUIRE(lists != TypeName::EXPAND_NAMED(TypeName::LIST(variable)));
	REQUIRE(lists != TypeName::EXPAND(TypeName::LIST(TypeName::VARIABLE("U"))));
	// the name of a variable is unchanged by it
	REQUIRE(variable == TypeName(Identifier("T")));
	REQUIRE(variable.Hash() == TypeName(Identifier("T")).Hash());
	// what a name contains is looked for in the pattern of an expansion too
	REQUIRE(tuple.Contains("T"));
	REQUIRE(pack.Contains("T"));
	REQUIRE(lists.Contains("T"));
	REQUIRE(lists.Contains("LIST"));
	REQUIRE(!lists.Contains("U"));
	REQUIRE(TypeName::TUPLE({lists}).Contains("T"));
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
	// a name that uses a variable of the signature is not a type until a call determines the variable
	std::function<bool(const FunctionSignature &, const TypeName &)> uses_variable;
	uses_variable = [&](const FunctionSignature &signature, const TypeName &name) {
		if (name.IsExpansion()) {
			return uses_variable(signature, name.GetPattern());
		}
		if (!name.HasArguments()) {
			return signature.FindVariable(name.GetName()) != nullptr;
		}
		for (auto &argument : name.GetArguments()) {
			if (argument.IsType() && uses_variable(signature, argument.GetType())) {
				return true;
			}
		}
		return false;
	};
	auto check_type = [&](const SimpleFunction &function, const TypeName &name) {
		if (uses_variable(function.GetSignature(), name)) {
			return;
		}
		try {
			FunctionBinder::BindTypeName(&context, name);
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
