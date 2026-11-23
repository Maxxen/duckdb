#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

void CastToTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("CastToType function cannot be executed directly");
}

unique_ptr<Expression> BindCastToTypeFunction(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[1]->GetReturnType();
	if (return_type.id() == LogicalTypeId::UNKNOWN) {
		// parameter - unknown return type
		throw ParameterNotResolvedException();
	}
	if (return_type.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException("cast_to_type cannot be used to cast to NULL");
	}
	auto result = BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), return_type);
	return Expression::PreserveReturnType(return_type, std::move(result));
}

} // namespace
ScalarFunction CastToTypeFun::GetFunction() {
	auto fun = ScalarFunction(FunctionSignature()
	                              .AddVariable("T")
	                              .AddVariable("U")
	                              .AddParameter("param", TypeName::VARIABLE("T"))
	                              .AddParameter("type", TypeName::VARIABLE("U"))
	                              .SetReturnType(TypeName::VARIABLE("U")),
	                          CastToTypeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// the result has the type of the second argument as it is, the collations of the arguments are not combined
	fun.SetCollationHandling(FunctionCollationHandling::IGNORE_COLLATIONS);
	fun.SetBindExpressionCallback(BindCastToTypeFunction);
	return fun;
}

} // namespace duckdb
