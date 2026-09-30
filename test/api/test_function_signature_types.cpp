#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/function/signature_resolver.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

static vector<LogicalType> SignatureTestTypes() {
	child_list_t<LogicalType> struct_children {{"a", LogicalType::INTEGER}, {"b", LogicalType::VARCHAR}};
	child_list_t<LogicalType> union_members {{"n", LogicalType::BIGINT}, {"s", LogicalType::VARCHAR}};
	Vector enum_values(LogicalType::VARCHAR, 2);
	enum_values.SetValue(0, Value("x"));
	enum_values.SetValue(1, Value("y"));
	return {LogicalType::INTEGER,
	        LogicalType::TIMESTAMP,
	        LogicalType::TIMESTAMP_TZ,
	        LogicalType::DECIMAL(18, 3),
	        LogicalType(LogicalTypeId::DECIMAL),
	        LogicalType::LIST(LogicalType::DOUBLE),
	        LogicalType(LogicalTypeId::LIST),
	        LogicalType::LIST(LogicalType::ANY),
	        LogicalType::ARRAY(LogicalType::FLOAT, 3),
	        LogicalType::ARRAY(LogicalType::FLOAT, optional_idx()),
	        LogicalType(LogicalTypeId::ARRAY),
	        LogicalType::STRUCT(struct_children),
	        LogicalType(LogicalTypeId::STRUCT),
	        LogicalType::LIST(LogicalType::STRUCT(struct_children)),
	        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT),
	        LogicalType(LogicalTypeId::MAP),
	        LogicalType::UNION(union_members),
	        LogicalType(LogicalTypeId::UNION),
	        LogicalType::ENUM(enum_values, 2),
	        LogicalType::VARCHAR_COLLATION("nocase"),
	        LogicalType::ANY,
	        LogicalType::ANY_PARAMS(LogicalType::VARCHAR, 10),
	        LogicalType::POINTER,
	        LogicalType::SQLNULL,
	        LogicalType::VARIANT()};
}

TEST_CASE("Signature types resolve to the LogicalTypes they are built from", "[api][function_signature]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (auto &type : SignatureTestTypes()) {
		FunctionSignature signature({type}, type);
		INFO(type.ToString());
		SignatureResolver without_context(nullptr, signature);
		REQUIRE(without_context.ResolveParameter(0) == type);
		REQUIRE(without_context.ResolveReturnType() == type);
		REQUIRE(without_context.TypeToString(signature.GetParameter(0).GetType()) == type.ToString());
		con.context->RunFunctionInTransaction([&]() {
			SignatureResolver with_context(*con.context, signature);
			REQUIRE(with_context.ResolveParameter(0) == type);
		});
	}
}

TEST_CASE("Templates and families become type variables", "[api][function_signature]") {
	// (LIST(T), T) -> T: the template is shared by name, and stays a template when resolved
	FunctionSignature list_signature({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                                 LogicalType::TEMPLATE("T"));
	REQUIRE(list_signature.GetTypeVariables().size() == 1);
	REQUIRE(list_signature.GetTypeVariables()[0].GetName() == Identifier("T"));
	REQUIRE(list_signature.ResolveParameterType(0) == LogicalType::LIST(LogicalType::TEMPLATE("T")));
	REQUIRE(list_signature.ResolveReturnType() == LogicalType::TEMPLATE("T"));

	// every unparameterized DECIMAL gets width and scale variables of its own
	FunctionSignature decimal_signature({LogicalTypeId::DECIMAL, LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL);
	REQUIRE(decimal_signature.GetTypeVariables().size() == 6);
	for (auto &variable : decimal_signature.GetTypeVariables()) {
		REQUIRE(variable.GetKind() == TypeVariableKind::INTEGER);
	}
	REQUIRE(decimal_signature.GetParameter(0).GetType().ToString() == "DECIMAL(W0, S0)");
	REQUIRE(decimal_signature.GetParameter(1).GetType().ToString() == "DECIMAL(W1, S1)");
	REQUIRE(decimal_signature.ResolveParameterType(1) == LogicalType(LogicalTypeId::DECIMAL));
	REQUIRE(decimal_signature.ToString() == "(col0 DECIMAL, col1 DECIMAL, /) -> DECIMAL");

	// a STRUCT of any shape is a pack of fields
	FunctionSignature struct_signature({LogicalTypeId::STRUCT}, LogicalType::BIGINT);
	REQUIRE(struct_signature.GetParameter(0).GetType().ToString() == "STRUCT(T0...)");
	REQUIRE(struct_signature.GetTypeVariables()[0].GetArity() == TypeVariableArity::PACK);

	// a fresh variable does not take the name of a template
	FunctionSignature mixed_signature({LogicalType::TEMPLATE("T0"), LogicalTypeId::LIST}, LogicalType::TEMPLATE("T0"));
	REQUIRE(mixed_signature.GetParameter(1).GetType().ToString() == "LIST(T1)");
	REQUIRE(mixed_signature.ResolveParameterType(1) == LogicalType(LogicalTypeId::LIST));
}

TEST_CASE("Replacing a type drops the type variables no type refers to", "[api][function_signature]") {
	FunctionSignature signature({LogicalTypeId::DECIMAL}, LogicalType::BIGINT);
	REQUIRE(signature.GetTypeVariables().size() == 2);
	signature.SetParameterType(0, LogicalType::INTEGER);
	REQUIRE(signature.GetTypeVariables().empty());
	REQUIRE(signature == FunctionSignature({LogicalType::INTEGER}, LogicalType::BIGINT));
}

TEST_CASE("The options of a typed **kwargs are signature types", "[api][function_signature]") {
	// an option shares the type variables of the signature, and the variables it alone uses are kept
	FunctionSignature signature({LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
	signature.WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("fallback", LogicalType::TEMPLATE("T"));
		options.Add("precision", LogicalTypeId::DECIMAL);
		options.Add("names", LogicalType::LIST(LogicalType::VARCHAR));
	});
	REQUIRE_NOTHROW(signature.Verify());
	REQUIRE(signature.GetTypeVariables().size() == 3);
	auto &options = *signature.GetTypedKwargs();
	REQUIRE(options.Find("fallback")->type.ToString() == "T");
	REQUIRE(options.Find("precision")->type.ToString() == "DECIMAL(W0, S0)");

	SignatureResolver resolver(nullptr, signature);
	REQUIRE(resolver.Resolve(options.Find("fallback")->type) == LogicalType::TEMPLATE("T"));
	REQUIRE(resolver.Resolve(options.Find("precision")->type) == LogicalType(LogicalTypeId::DECIMAL));
	REQUIRE(resolver.Resolve(options.Find("names")->type) == LogicalType::LIST(LogicalType::VARCHAR));
	REQUIRE_NOTHROW(signature.VerifyTypeConversion(nullptr));

	// replacing the parameter keeps the variable an option still refers to
	signature.SetParameterType(0, LogicalType::INTEGER);
	REQUIRE(signature.GetTypeVariable("T"));

	// options added later are converted too, and signatures differing in an option type differ
	auto extended = signature;
	extended.ExtendTypedKwargs([](TypedKwargs &options) { options.Add("limit", LogicalType::BIGINT); });
	REQUIRE(extended.GetTypedKwargs()->Find("limit")->type == TypeName::FromLogicalType(LogicalType::BIGINT));
	REQUIRE(!signature.GetTypedKwargs()->Find("limit"));
	REQUIRE(extended != signature);
}

TEST_CASE("Merging the options of another signature carries over their type variables", "[api][function_signature]") {
	FunctionSignature source({LogicalType::VARCHAR}, LogicalType::BIGINT);
	source.WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("columns", LogicalTypeId::STRUCT);
		options.Add("format", LogicalType::VARCHAR);
	});
	REQUIRE(source.GetTypedKwargs()->Find("columns")->type.ToString() == "STRUCT(T0...)");

	// the target already declares a T0, so the variable of the merged option is renamed
	FunctionSignature target({LogicalTypeId::STRUCT}, LogicalType::BIGINT);
	target.WithTypedKwargs("options", [](TypedKwargs &options) { options.Add("header", LogicalType::BOOLEAN); });
	target.MergeTypedKwargs(source);
	REQUIRE_NOTHROW(target.Verify());
	REQUIRE(target.GetTypedKwargs()->GetOptions().size() == 3);
	REQUIRE(target.GetParameter(0).GetType().ToString() == "STRUCT(T0...)");
	auto &columns = target.GetTypedKwargs()->Find("columns")->type;
	REQUIRE(columns.ToString() == "STRUCT(T0_1...)");
	REQUIRE(target.GetTypeVariable("T0_1")->GetArity() == TypeVariableArity::PACK);
	// each still occurs once, so both lower to the family type
	SignatureResolver resolver(nullptr, target);
	REQUIRE(resolver.ResolveParameter(0) == LogicalType(LogicalTypeId::STRUCT));
	REQUIRE(resolver.Resolve(columns) == LogicalType(LogicalTypeId::STRUCT));
	REQUIRE_NOTHROW(target.VerifyTypeConversion(nullptr));
}

template <class ENTRY>
static void VerifyFunctionSet(ClientContext &context, CatalogEntry &entry, idx_t &count) {
	if (entry.type != ENTRY::Type) {
		return;
	}
	for (auto &function : entry.Cast<ENTRY>().functions.functions) {
		INFO(function->ToString());
		REQUIRE_NOTHROW(function->GetSignature().VerifyTypeConversion(nullptr));
		REQUIRE_NOTHROW(function->GetSignature().VerifyTypeConversion(context));
		count++;
	}
}

TEST_CASE("Every built-in signature resolves to the types it was built from", "[api][function_signature]") {
	DuckDB db(nullptr);
	Connection con(db);
	idx_t count = 0;
	con.context->RunFunctionInTransaction([&]() {
		auto &context = *con.context;
		// resolving a type looks up the catalog, so the entries are collected before they are verified
		vector<reference<CatalogEntry>> entries;
		for (auto &schema : Catalog::GetAllSchemas(context)) {
			for (auto type : {CatalogType::SCALAR_FUNCTION_ENTRY, CatalogType::AGGREGATE_FUNCTION_ENTRY,
			                  CatalogType::WINDOW_FUNCTION_ENTRY, CatalogType::TABLE_FUNCTION_ENTRY,
			                  CatalogType::PRAGMA_FUNCTION_ENTRY, CatalogType::TYPE_ENTRY}) {
				schema.get().Scan(context, type, [&](CatalogEntry &entry) { entries.push_back(entry); });
			}
		}
		for (auto &entry_ref : entries) {
			auto &entry = entry_ref.get();
			VerifyFunctionSet<ScalarFunctionCatalogEntry>(context, entry, count);
			VerifyFunctionSet<AggregateFunctionCatalogEntry>(context, entry, count);
			VerifyFunctionSet<WindowFunctionCatalogEntry>(context, entry, count);
			VerifyFunctionSet<TableFunctionCatalogEntry>(context, entry, count);
			VerifyFunctionSet<PragmaFunctionCatalogEntry>(context, entry, count);
			if (entry.type == CatalogType::TYPE_ENTRY) {
				for (auto &constructor : entry.Cast<TypeCatalogEntry>().constructors.functions) {
					REQUIRE_NOTHROW(constructor->GetSignature().VerifyTypeConversion(context));
					count++;
				}
			}
		}
	});
	REQUIRE(count > 1000);
}
