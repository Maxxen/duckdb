#include "duckdb/function/function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

hash_t FunctionParameter::Hash() const {
	hash_t hash = type.Hash();
	if (!name.empty()) {
		hash = duckdb::CombineHash(hash, duckdb::Hash(name.c_str()));
	}
	return hash;
}

FunctionData::~FunctionData() {
}

bool FunctionData::Equals(const FunctionData *left, const FunctionData *right) {
	if (left == right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

TableFunctionData::~TableFunctionData() {
}

unique_ptr<FunctionData> TableFunctionData::Copy() const {
	throw InternalException("Copy not supported for TableFunctionData");
}

bool TableFunctionData::Equals(const FunctionData &other) const {
	return false;
}

bool FunctionData::SupportStatementCache() const {
	return true;
}

Function::Function(string name_p) : name(std::move(name_p)) {
}
Function::~Function() {
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : Function(std::move(name_p)), arguments(std::move(arguments_p)), varargs(std::move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

bool SimpleNamedParameterFunction::HasNamedParameters() const {
	return !named_parameters.empty();
}

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {
	RegisterTableScanFunctions();
	RegisterSQLiteFunctions();
	RegisterReadFunctions();
	RegisterTableFunctions();
	RegisterArrowFunctions();

	RegisterPragmaFunctions();

	RegisterCopyFunctions();

	// initialize collations
	AddCollation("nocase", LowerFun::GetFunction(), true);
	AddCollation("noaccent", StripAccentsFun::GetFunction(), true);
	AddCollation("nfc", NFCNormalizeFun::GetFunction());

	RegisterExtensionOverloads();
}

hash_t BaseScalarFunction::Hash() const {
	hash_t hash = signature.GetReturnType().Hash();
	for (auto &param : signature.GetParameters()) {
		hash = duckdb::CombineHash(hash, param.Hash());
	}
	return hash;
}

static bool RequiresCatalogAndSchemaNamePrefix(const string &catalog_name, const string &schema_name) {
	return !catalog_name.empty() && catalog_name != SYSTEM_CATALOG && !schema_name.empty() &&
	       schema_name != DEFAULT_SCHEMA;
}

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments, const LogicalType &varargs) {
	string result;
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		result += catalog_name + "." + schema_name + ".";
	}
	result += name + "(";
	vector<string> string_arguments;
	for (auto &arg : arguments) {
		string_arguments.push_back(arg.ToString());
	}
	if (varargs.IsValid()) {
		string_arguments.push_back("[" + varargs.ToString() + "...]");
	}
	result += StringUtil::Join(string_arguments, ", ");
	return result + ")";
}

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments, const LogicalType &varargs,
                              const LogicalType &return_type) {
	string result = CallToString(catalog_name, schema_name, name, arguments, varargs);
	result += " -> " + return_type.ToString();
	return result;
}

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments,
                              const named_parameter_type_map_t &named_parameters) {
	vector<string> input_arguments;
	input_arguments.reserve(arguments.size() + named_parameters.size());
	for (auto &arg : arguments) {
		input_arguments.push_back(arg.ToString());
	}
	for (auto &kv : named_parameters) {
		input_arguments.push_back(StringUtil::Format("%s : %s", kv.first, kv.second.ToString()));
	}
	string prefix = "";
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		prefix = StringUtil::Format("%s.%s.", catalog_name, schema_name);
	}
	return StringUtil::Format("%s%s(%s)", prefix, name, StringUtil::Join(input_arguments, ", "));
}

SimpleFunction::SimpleFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               FunctionStability stability, LogicalType varargs, FunctionNullHandling null_handling,
                               FunctionErrors errors)
    : Function(std::move(name)), stability(stability), null_handling(null_handling), errors(errors) {
	signature = FunctionSignature(arguments, return_type);
	signature.SetVarArgs(varargs);
}

} // namespace duckdb
