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
};

template <class T>
class FunctionSignature {
public:
	T function;

public:
	vector<FunctionParameter> parameters;

	FunctionSignature(T function) : function(std::move(function)) {
	}

public:
	string ToString() const {
		return function.ToString();
	}

	bool Equal(const FunctionSignature<T> &other) {
		return function.Equal(other.function);
	}
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
	void AddFunction(T function) {
		functions.emplace_back(std::move(function));
	}
	void AddFunction(FunctionSignature<T> function) {
		functions.push_back(std::move(function));
	}
	idx_t Size() {
		return functions.size();
	}
	T GetFunctionByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset].function;
	}
	T &GetFunctionReferenceByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset].function;
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
