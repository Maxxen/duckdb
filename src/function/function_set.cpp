#include "duckdb/function/function_set.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// FunctionOverload
//----------------------------------------------------------------------------------------------------------------------

template <>
FunctionOverload<ScalarFunction>::FunctionOverload(ScalarFunction func) : function(std::move(func)) {
	// Copy over parameters
	for (auto &arg : func.arguments) {
		parameters.push_back({arg});
	}
	varags = func.varargs;
	return_type = func.return_type;
}

template <>
FunctionOverload<AggregateFunction>::FunctionOverload(AggregateFunction func) : function(std::move(func)) {
	// Copy over parameters
	for (auto &arg : func.arguments) {
		parameters.push_back({arg});
	}
	varags = func.varargs;
	return_type = func.return_type;
}

template <>
FunctionOverload<WindowFunction>::FunctionOverload(WindowFunction func) : function(std::move(func)) {
	// Copy over parameters
	for (auto &arg : func.arguments) {
		parameters.push_back({arg});
	}
	varags = func.varargs;
	return_type = func.return_type;
}

template <>
FunctionOverload<TableFunction>::FunctionOverload(TableFunction func) : function(std::move(func)) {
	// Copy over parameters
	for (auto &arg : func.arguments) {
		parameters.push_back({arg});
	}
	varags = func.varargs;
	// no return type yet

	// TODO: Named params
}

template <>
FunctionOverload<PragmaFunction>::FunctionOverload(PragmaFunction func) : function(std::move(func)) {
	// Copy over parameters
	for (auto &arg : func.arguments) {
		parameters.push_back({arg});
	}
	varags = func.varargs;
	// no return type yet

	// TODO: Named params
}

//----------------------------------------------------------------------------------------------------------------------
// FunctionSet
//----------------------------------------------------------------------------------------------------------------------

ScalarFunctionSet::ScalarFunctionSet() : FunctionSet("") {
}

ScalarFunctionSet::ScalarFunctionSet(string name) : FunctionSet(std::move(name)) {
}

ScalarFunctionSet::ScalarFunctionSet(ScalarFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

ScalarFunction ScalarFunctionSet::GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error.Message());
	}
	return GetFunctionByOffset(index.GetIndex());
}

AggregateFunctionSet::AggregateFunctionSet() : FunctionSet("") {
}

AggregateFunctionSet::AggregateFunctionSet(string name) : FunctionSet(std::move(name)) {
}

AggregateFunctionSet::AggregateFunctionSet(AggregateFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

AggregateFunction AggregateFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                               const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		// check if the arguments are a prefix of any of the arguments
		// this is used for functions such as quantile or string_agg that delete part of their arguments during bind
		// FIXME: we should come up with a better solution here
		for (auto &func : functions) {
			if (arguments.size() >= func.parameters.size()) {
				continue;
			}
			bool is_prefix = true;
			for (idx_t k = 0; k < arguments.size(); k++) {
				if (arguments[k].id() != func.parameters[k].type.id()) {
					is_prefix = false;
					break;
				}
			}
			if (is_prefix) {
				return func.function;
			}
		}
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error.Message());
	}
	return GetFunctionByOffset(index.GetIndex());
}

WindowFunctionSet::WindowFunctionSet() : FunctionSet("") {
}

WindowFunctionSet::WindowFunctionSet(string name) : FunctionSet(std::move(name)) {
}

WindowFunctionSet::WindowFunctionSet(WindowFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

WindowFunction WindowFunctionSet::GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error.Message());
	}
	return GetFunctionByOffset(index.GetIndex());
}

TableFunctionSet::TableFunctionSet(string name) : FunctionSet(std::move(name)) {
}

TableFunctionSet::TableFunctionSet(TableFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

TableFunction TableFunctionSet::GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error.Message());
	}
	return GetFunctionByOffset(index.GetIndex());
}

PragmaFunctionSet::PragmaFunctionSet(string name) : FunctionSet(std::move(name)) {
}

PragmaFunctionSet::PragmaFunctionSet(PragmaFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

} // namespace duckdb
