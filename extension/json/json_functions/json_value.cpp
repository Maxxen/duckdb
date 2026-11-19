#include "json_executors.hpp"

namespace duckdb {

static void ValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, JSONCommon::JSONValue);
}

static void ValueManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, JSONCommon::JSONValue);
}

static LogicalType ValueReturnType(ResolveReturnTypeInput &input) {
	return JSONReadFunctionData::ResolveReturnType(input, LogicalType::VARCHAR);
}

static void GetValueFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	ScalarFunction path_fun({}, ReturnType::ANY(ValueReturnType), ValueFunction, JSONReadFunctionData::Bind, nullptr,
	                        JSONFunctionLocalState::Init);
	JSONReadFunctionData::AddPathFunctions(set, path_fun, input_type);
	ScalarFunction many_fun({}, LogicalType::LIST(LogicalType::VARCHAR), ValueManyFunction,
	                        JSONReadManyFunctionData::Bind, nullptr, JSONFunctionLocalState::Init);
	many_fun.GetSignature()
	    .AddParameter("json", input_type)
	    .AddParameter("path", LogicalType::LIST(LogicalType::VARCHAR));
	set.AddFunction(many_fun);
}

ScalarFunctionSet JSONFunctions::GetValueFunction() {
	// The value function is just like the extract function but returns NULL if the JSON is not a scalar value
	ScalarFunctionSet set("json_value");
	GetValueFunctionsInternal(set, LogicalType::VARCHAR);
	GetValueFunctionsInternal(set, LogicalType::JSON());
	set.ApplyToFunctions([](ScalarFunction &func) {
		const auto &sig = func.GetSignature();
		if (sig.GetParameter(0).GetTypeName().GetName() == LogicalType::JSON_TYPE_NAME &&
		    LogicalType(sig.GetParameter(1).GetTypeName().GetBuiltinType()).IsNumeric()) {
			return;
		}
		func.SetFallible();
	});
	return set;
}

} // namespace duckdb
