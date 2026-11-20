#include "json_executors.hpp"

namespace duckdb {

static inline optional<uint64_t> GetArrayLength(yyjson_val *val, yyjson_alc *, Vector &) {
	return yyjson_arr_size(val);
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void ManyArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<uint64_t>(args, state, result, GetArrayLength);
}

static LogicalType ArrayLengthReturnType(const ResolveReturnTypeInput &input) {
	return JSONReadFunctionData::ResolveReturnType(input, LogicalType::UBIGINT);
}

static void GetArrayLengthFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	ScalarFunction unary_fun({}, LogicalType::UBIGINT, UnaryArrayLengthFunction, nullptr, nullptr,
	                         JSONFunctionLocalState::Init);
	unary_fun.GetSignature().AddParameter("json", input_type);
	set.AddFunction(unary_fun);
	ScalarFunction path_fun({}, ReturnType::ANY(ArrayLengthReturnType), BinaryArrayLengthFunction,
	                        JSONReadFunctionData::Bind, nullptr, JSONFunctionLocalState::Init);
	JSONReadFunctionData::AddPathFunctions(set, path_fun, input_type);
	ScalarFunction many_fun({}, LogicalType::LIST(LogicalType::UBIGINT), ManyArrayLengthFunction,
	                        JSONReadManyFunctionData::Bind, nullptr, JSONFunctionLocalState::Init);
	many_fun.GetSignature()
	    .AddParameter("json", input_type)
	    .AddParameter("path", LogicalType::LIST(LogicalType::VARCHAR));
	set.AddFunction(many_fun);
}

ScalarFunctionSet JSONFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("json_array_length");
	GetArrayLengthFunctionsInternal(set, LogicalType::VARCHAR);
	GetArrayLengthFunctionsInternal(set, LogicalType::JSON());
	set.ApplyToFunctions([](ScalarFunction &func) {
		const auto &sig = func.GetSignature();
		if (sig.GetParameterCount() == 1 &&
		    sig.GetParameter(0).GetTypeName().GetName() == LogicalType::JSON_TYPE_NAME) {
			return;
		}
		func.SetFallible();
	});
	return set;
}

} // namespace duckdb
