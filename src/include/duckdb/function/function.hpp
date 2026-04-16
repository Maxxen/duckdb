//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/enums/function_errors.hpp"

namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class PragmaFunction;
class PragmaFunctionSet;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;
class WindowFunction;
class WindowFunctionSet;

struct PragmaInfo;

class FunctionParameter {
public:
	FunctionParameter(string name, LogicalType type) : name(std::move(name)), type(std::move(type)) {
	}

	const string &GetName() const {
		return name;
	}
	const LogicalType &GetType() const {
		return type;
	}
	void SetName(string new_name) {
		name = std::move(new_name);
	}
	void SetType(LogicalType new_type) {
		type = std::move(new_type);
	}

	string ToString() const {
		if (name.empty()) {
			return type.ToString();
		} else {
			return name + " " + type.ToString();
		}
	}
	hash_t Hash() const;

private:
	string name;
	LogicalType type;
};

class FunctionSignature {
public:
	FunctionSignature() = default;

	FunctionSignature(const vector<LogicalType> &types) {
		for (idx_t i = 0; i < types.size(); i++) {
			parameters.emplace_back("arg" + to_string(i + 1), types[i]);
		}
	}

	idx_t GetParameterCount() const {
		return parameters.size();
	}
	const FunctionParameter &GetParameter(idx_t index) const {
		return parameters[index];
	}
	FunctionParameter &GetParameter(idx_t index) {
		return parameters[index];
	}

	const vector<FunctionParameter> &GetParameters() const {
		return parameters;
	}
	const LogicalType &GetVarArgs() const {
		return varargs;
	}
	const LogicalType &GetReturnType() const {
		return returns;
	}

	void SetReturnType(const LogicalType &type) {
		returns = type;
	}
	void SetVarArgs(const LogicalType &type) {
		varargs = type;
	}
	void AddParemeter(const string &name, const LogicalType &type) {
		parameters.emplace_back(name, type);
	}

	// TODO: Make immutable
	LogicalType &GetVarArgs() {
		return varargs;
	}
	LogicalType &GetReturnType() {
		return returns;
	}

	// Two signatures are considered equal if they have the same parameter types, varargs and return type
	bool operator==(const FunctionSignature &other) const {
		if (parameters.size() != other.parameters.size()) {
			return false;
		}
		for (idx_t i = 0; i < parameters.size(); i++) {
			if (parameters[i].GetType() != other.parameters[i].GetType()) {
				return false;
			}
		}
		return varargs == other.varargs && returns == other.returns;
	}
	bool operator!=(const FunctionSignature &other) const {
		return !(*this == other);
	}

private:
	LogicalType varargs;
	LogicalType returns;
	vector<FunctionParameter> parameters;
};

//! The default null handling is NULL in, NULL out
enum class FunctionNullHandling : uint8_t { DEFAULT_NULL_HANDLING = 0, SPECIAL_HANDLING = 1 };
//! The stability of the function, used by the optimizer
//! CONSISTENT              -> this function always returns the same result when given the same input, no variance
//! CONSISTENT_WITHIN_QUERY -> this function returns the same result WITHIN the same query/transaction
//!                            but the result might change across queries (e.g. NOW(), CURRENT_TIME)
//! VOLATILE                -> the result of this function might change per row (e.g. RANDOM())
enum class FunctionStability : uint8_t { CONSISTENT = 0, VOLATILE = 1, CONSISTENT_WITHIN_QUERY = 2 };

//! How to handle collations
//! PROPAGATE_COLLATIONS        -> this function combines collation from its inputs and emits them again (default)
//! PUSH_COMBINABLE_COLLATIONS  -> combinable collations are executed for the input arguments
//! IGNORE_COLLATIONS           -> collations are completely ignored by the function
enum class FunctionCollationHandling : uint8_t {
	PROPAGATE_COLLATIONS = 0,
	PUSH_COMBINABLE_COLLATIONS = 1,
	IGNORE_COLLATIONS = 2
};

struct FunctionData {
	DUCKDB_API virtual ~FunctionData();

	DUCKDB_API virtual unique_ptr<FunctionData> Copy() const = 0;
	DUCKDB_API virtual bool Equals(const FunctionData &other) const = 0;
	DUCKDB_API static bool Equals(const FunctionData *left, const FunctionData *right);
	DUCKDB_API virtual bool SupportStatementCache() const;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
	// FIXME: this function should be removed in the future
	template <class TARGET>
	TARGET &CastNoConst() const {
		return const_cast<TARGET &>(Cast<TARGET>()); // NOLINT: FIXME
	}
};

struct TableFunctionData : public FunctionData {
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;

	DUCKDB_API ~TableFunctionData() override;

	DUCKDB_API unique_ptr<FunctionData> Copy() const override;
	DUCKDB_API bool Equals(const FunctionData &other) const override;
};

struct FunctionParameters {
	vector<Value> values;
	named_parameter_map_t named_parameters;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	DUCKDB_API explicit Function(string name);
	DUCKDB_API virtual ~Function();

	//! The name of the function
	string name;

	//! Additional Information to specify function from it's name
	string extra_info;

	// Optional catalog name of the function
	string catalog_name;

	// Optional schema name of the function
	string schema_name;

	// The signature of the function
	FunctionSignature signature;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const LogicalType &varargs = LogicalType::INVALID);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments, const LogicalType &varargs,
	                                      const LogicalType &return_type);
	//! Returns the formatted string name(arg1, arg2.., np1=a, np2=b, ...)
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const named_parameter_type_map_t &named_parameters);

	//! Used in the bind to erase an argument from a function
	DUCKDB_API static void EraseArgument(Function &bound_function, vector<unique_ptr<Expression>> &arguments,
	                                     idx_t argument_index);

	// Signature getters/setters
	const FunctionSignature &GetSignature() const {
		return signature;
	}

	FunctionSignature &GetSignature() {
		return signature;
	}

	void SetReturnType(const LogicalType &return_type) {
		signature.SetReturnType(return_type);
	}
	const LogicalType &GetReturnType() const {
		return signature.GetReturnType();
	}
	LogicalType &GetReturnType() {
		return signature.GetReturnType();
	}
	void SetVarArgs(const LogicalType &varargs) {
		signature.SetVarArgs(varargs);
	}
	const LogicalType &GetVarArgs() const {
		return signature.GetVarArgs();
	}
	LogicalType &GetVarArgs() {
		return signature.GetVarArgs();
	}

	bool HasVarArgs() const {
		return signature.GetVarArgs().id() != LogicalTypeId::INVALID;
	}

	// Bound function only
	//! The set of arguments of the function
	// vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	// vector<LogicalType> original_arguments;

	virtual string ToString() const {
		return "TODO";
		// return CallToString(catalog_name, schema_name, name, arguments, signature.GetVarArgs(),
		// signature.GetReturnType());
	}
};

class SimpleNamedParameterFunction : public Function {
public:
	DUCKDB_API SimpleNamedParameterFunction(string name, vector<LogicalType> arguments,
	                                        LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	DUCKDB_API ~SimpleNamedParameterFunction() override;

	//! The named parameters of the function
	named_parameter_type_map_t named_parameters;

public:
	DUCKDB_API bool HasNamedParameters() const;
};

class BaseScalarFunction : public Function {
public:
	DUCKDB_API BaseScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
	                              FunctionStability stability,
	                              LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                              FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                              FunctionErrors errors = FunctionErrors::CANNOT_ERROR);
	DUCKDB_API ~BaseScalarFunction() override;

public:
	FunctionStability GetStability() const {
		return stability;
	}
	void SetStability(FunctionStability stability_p) {
		stability = stability_p;
	}

	FunctionNullHandling GetNullHandling() const {
		return null_handling;
	}
	void SetNullHandling(FunctionNullHandling null_handling_p) {
		null_handling = null_handling_p;
	}

	FunctionErrors GetErrorMode() const {
		return errors;
	}
	void SetErrorMode(FunctionErrors errors_p) {
		errors = errors_p;
	}

	//! Set this functions error-mode as fallible (can throw runtime errors)
	void SetFallible() {
		errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
	}
	//! Set this functions stability as volatile (can not be cached per row)
	void SetVolatile() {
		stability = FunctionStability::VOLATILE;
	}

	void SetCollationHandling(FunctionCollationHandling collation_handling_p) {
		collation_handling = collation_handling_p;
	}
	FunctionCollationHandling GetCollationHandling() const {
		return collation_handling;
	}

public:
	//! The stability of the function (see FunctionStability enum for more info)
	FunctionStability stability;
	//! How this function handles NULL values
	FunctionNullHandling null_handling;
	//! Whether or not this function can throw an error
	FunctionErrors errors;
	//! Collation handling of the function
	FunctionCollationHandling collation_handling;

	static BaseScalarFunction SetReturnsError(BaseScalarFunction &function) {
		function.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
		return function;
	}

public:
	DUCKDB_API hash_t Hash() const;
};

} // namespace duckdb
