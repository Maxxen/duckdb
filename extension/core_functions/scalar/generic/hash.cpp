#include "core_functions/scalar/generic_functions.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
}

ScalarFunction HashFun::GetFunction() {
	auto hash_fun = ScalarFunction(FunctionSignature()
	                                   .AddVariable("T")
	                                   .AddParameter("value", TypeName::VARIABLE("T"))
	                                   .AddArgs("args", LogicalType::ANY)
	                                   .SetReturnType(LogicalType::HASH),
	                               HashFunction);
	hash_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// hash the arguments after their combinable collations
	hash_fun.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	return hash_fun;
}

} // namespace duckdb
