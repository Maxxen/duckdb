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
#include "duckdb/function/window_function.hpp"

namespace duckdb {

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

private:
	string name;
	LogicalType type;
};

class FunctionSignature {
public:
	vector<FunctionParameter> parameters;
	LogicalType varargs;
	LogicalType return_type;

	const vector<FunctionParameter> &GetParameters() const {
		return parameters;
	}
	idx_t GetParameterCount() const {
		return parameters.size();
	}

	const FunctionParameter &GetParameter(idx_t idx) const {
		D_ASSERT(idx < parameters.size());
		return parameters[idx];
	}

	bool HasVarArgs() const {
		return varargs.id() != LogicalTypeId::INVALID;
	}

	bool Equal(const FunctionSignature &other) const {
		if (parameters.size() != other.parameters.size()) {
			return false;
		}
		for (idx_t i = 0; i < parameters.size(); i++) {
			// Name does not matter for function signature equality, only type
			if (parameters[i].GetType() != other.parameters[i].GetType()) {
				return false;
			}
		}
		if (varargs != other.varargs) {
			return false;
		}
		if (return_type != other.return_type) {
			return false;
		}

		return true;
	}
};

template <class T>
class FunctionOverload {
public:
	FunctionOverload(T func);

	const FunctionSignature &GetSignature() const {
		return signature;
	}

	T &GetImplementation() {
		return function;
	}

	const T &GetImplementation() const {
		return function;
	}

	string ToString() const {
		return function.ToString();
	}

	bool Equal(const FunctionOverload<T> &other) const {
		return signature.Equal(other.signature);
	}

private:
	FunctionSignature signature; // binding
	T function;                  // exec, catalog
};

// For backwards compatability, we allow constructing implicitly from a fully defined function
template <>
FunctionOverload<ScalarFunction>::FunctionOverload(ScalarFunction func);
template <>
FunctionOverload<AggregateFunction>::FunctionOverload(AggregateFunction func);
template <>
FunctionOverload<WindowFunction>::FunctionOverload(WindowFunction func);
template <>
FunctionOverload<TableFunction>::FunctionOverload(TableFunction func);
template <>
FunctionOverload<PragmaFunction>::FunctionOverload(PragmaFunction func);

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(std::move(name)) { // NOLINT
	}

	//! The name of the function set
	string name;
	//! The set of functions.
	vector<FunctionOverload<T>> functions;

public:
	void AddFunction(T function) {
		functions.push_back(std::move(function));
	}

	void AddFunction(FunctionOverload<T> function) {
		functions.push_back(std::move(function));
	}

	idx_t Size() {
		return functions.size();
	}

	T GetFunctionByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset].GetImplementation();
	}

	T &GetFunctionReferenceByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset].GetImplementation();
	}

	const FunctionOverload<T> &GetEntryByOffset(idx_t offset) {
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

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	DUCKDB_API explicit ScalarFunctionSet();
	DUCKDB_API explicit ScalarFunctionSet(string name);
	DUCKDB_API explicit ScalarFunctionSet(ScalarFunction fun);

	DUCKDB_API ScalarFunction GetFunctionByArguments(ClientContext &context, const string &catalog_name,
	                                                 const string &schema_name, const string &name,
	                                                 const vector<LogicalType> &arguments);
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	DUCKDB_API explicit AggregateFunctionSet();
	DUCKDB_API explicit AggregateFunctionSet(string name);
	DUCKDB_API explicit AggregateFunctionSet(AggregateFunction fun);

	DUCKDB_API AggregateFunction GetFunctionByArguments(ClientContext &context, const string &catalog_name,
	                                                    const string &schema_name, const string &name,
	                                                    const vector<LogicalType> &arguments);
};

class WindowFunctionSet : public FunctionSet<WindowFunction> {
public:
	DUCKDB_API explicit WindowFunctionSet();
	DUCKDB_API explicit WindowFunctionSet(string name);
	DUCKDB_API explicit WindowFunctionSet(WindowFunction fun);

	DUCKDB_API WindowFunction GetFunctionByArguments(ClientContext &context, const string &catalog_name,
	                                                 const string &schema_name, const string &name,
	                                                 const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	DUCKDB_API explicit TableFunctionSet(string name);
	DUCKDB_API explicit TableFunctionSet(TableFunction fun);

	TableFunction GetFunctionByArguments(ClientContext &context, const string &catalog_name, const string &schema_name,
	                                     const string &name, const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	DUCKDB_API explicit PragmaFunctionSet(string name);
	DUCKDB_API explicit PragmaFunctionSet(PragmaFunction fun);
};

} // namespace duckdb
