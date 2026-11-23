#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

static void ReplaceTypeFunction(DataChunk &, ExpressionState &, Vector &) {
	throw InternalException("ReplaceTypeFunction function cannot be executed directly");
}

//! The type of the first argument, with the second type replaced by the third wherever it occurs
static LogicalType ReplaceTypeReturnType(const ResolveReturnTypeInput &input) {
	const auto &from = input.GetArgumentType(1);
	const auto &to = input.GetArgumentType(2);
	return TypeVisitor::VisitReplace(input.GetArgumentType(0),
	                                 [&from, &to](const LogicalType &type) { return type == from ? to : type; });
}

static unique_ptr<Expression> BindReplaceTypeFunction(FunctionBindExpressionInput &input) {
	const auto &from = input.children[1]->GetReturnType();
	const auto &to = input.children[2]->GetReturnType();
	if (from.id() == LogicalTypeId::UNKNOWN || to.id() == LogicalTypeId::UNKNOWN) {
		// parameters - unknown return type
		throw ParameterNotResolvedException();
	}
	if (to.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException("replace_type cannot be used to replace type with NULL");
	}
	const auto return_type = TypeVisitor::VisitReplace(
	    input.children[0]->GetReturnType(), [&from, &to](const LogicalType &type) { return type == from ? to : type; });
	return BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), return_type);
}

ScalarFunction ReplaceTypeFun::GetFunction() {
	auto fun = ScalarFunction(FunctionSignature()
	                              .AddVariable("T")
	                              .AddVariable("U")
	                              .AddVariable("V")
	                              .AddParameter("param", TypeName::VARIABLE("T"))
	                              .AddParameter("type1", TypeName::VARIABLE("U"))
	                              .AddParameter("type2", TypeName::VARIABLE("V"))
	                              .SetReturnType(ReturnType::ANY(ReplaceTypeReturnType)),
	                          ReplaceTypeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// the result has the type the arguments determine as it is, the collations of the arguments are not combined
	fun.SetCollationHandling(FunctionCollationHandling::IGNORE_COLLATIONS);
	fun.SetBindExpressionCallback(BindReplaceTypeFunction);
	return fun;
}

} // namespace duckdb
