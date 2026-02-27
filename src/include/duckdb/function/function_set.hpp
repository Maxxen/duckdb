//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/pragma_function.hpp"

namespace duckdb {

class FunctionParameter {
public:
	LogicalType type;
	string name;

	const string &GetName() const {
		return name;
	}
	const LogicalType &GetType() const {
		return type;
	}
};

template <class T>
class FunctionSignature {
public:

	//! Construct a function signature from an existing function object.
	//! This will automatically generate named parameters for the function based on the argument types,
	//! and extract the return type of the function
	//!
	//! This constructor is here for backwards compatibility, but you should ideally construct a FunctionSignature by
	//! directly setting the properties using the builder interface instead of passing in a function object.
	explicit FunctionSignature(T function);

public:
	string ToString() const {
		return function.ToString();
	}

	bool Equal(const FunctionSignature<T> &other) {
		return function.Equal(other.function);
	}

	T Instantiate() const {
		T result = function;
		// TODO: Pass the properties to the function object and instantiate it properly
		// result.SetReturnType(return_type);
		return result;
	}

	const T &GetFunction() const {
		return function;
	}

	T &GetFunctionMutable() {
		return function;
	}

public:
	const vector<FunctionParameter> &GetParameters() const {
		return parameters;
	}
	void AddParameter(string name, LogicalType type) {
		parameters.push_back({std::move(type), std::move(name)});
	}
	bool HasVarArgs() const {
		return varargs.type.id() != LogicalTypeId::INVALID;
	}
	const FunctionParameter &GetVarArgs() const {
		return varargs;
	}
	const LogicalType &GetReturnType() const {
		return return_type;
	}
	void SetReturnType(LogicalType type) {
		return_type = std::move(type);
	}
private:
	// TODO: Replace this with a "property" object, that we can use to instantiate the actual function object
	T function;

	//! The parameters of the function, with their names and types.
	//! The order of the parameters is important, as it is used for overload resolution.
	vector<FunctionParameter> parameters;
	FunctionParameter varargs;
	LogicalType return_type;
};

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(std::move(name)) {
	}

	//! The name of the function set
	string name;
	//! The schema of the function set
	string schema;
	//! The catalog of the function set
	string catalog;
	//! The set of functions.
	vector<FunctionSignature<T>> functions;

public:
	const string GetFunctionName() const { return name; }
	const string GetSchemaName() const { return schema; }
	const string GetCatalogName() const { return catalog; }

	void AddFunction(T function) {
		functions.emplace_back(std::move(function));
	}
	void AddFunction(FunctionSignature<T> function) {
		functions.push_back(std::move(function));
	}
	idx_t Size() {
		return functions.size();
	}

	const FunctionSignature<T> &GetFunctionByOffset(idx_t offset) const {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}

	FunctionSignature<T> &GetFunctionReferenceByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}

	bool MergeFunctionSet(FunctionSet<T> new_functions, bool override = false) {
		D_ASSERT(!new_functions.functions.empty());
		for (auto &new_func : new_functions.functions) {
			bool overwritten = false;
			for (auto &func : functions) {
				if (new_func.Equal(func)) {
					// function overload already exists
					if (override) {
						// override it
						overwritten = true;
						func = new_func;
					} else {
						// throw an error
						return false;
					}
					break;
				}
			}
			if (!overwritten) {
				functions.push_back(new_func);
			}
		}
		return true;
	}
};

using ScalarFunctionSignature = FunctionSignature<ScalarFunction>;
using AggregateFunctionSignature = FunctionSignature<AggregateFunction>;
using TableFunctionSignature = FunctionSignature<TableFunction>;
using PragmaFunctionSignature = FunctionSignature<PragmaFunction>;

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	DUCKDB_API explicit ScalarFunctionSet();
	DUCKDB_API explicit ScalarFunctionSet(string name);
	DUCKDB_API explicit ScalarFunctionSet(ScalarFunction fun);

	DUCKDB_API ScalarFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	DUCKDB_API explicit AggregateFunctionSet();
	DUCKDB_API explicit AggregateFunctionSet(string name);
	DUCKDB_API explicit AggregateFunctionSet(AggregateFunction fun);

	DUCKDB_API AggregateFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	DUCKDB_API explicit TableFunctionSet(string name);
	DUCKDB_API explicit TableFunctionSet(TableFunction fun);

	TableFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	DUCKDB_API explicit PragmaFunctionSet(string name);
	DUCKDB_API explicit PragmaFunctionSet(PragmaFunction fun);
};

} // namespace duckdb
