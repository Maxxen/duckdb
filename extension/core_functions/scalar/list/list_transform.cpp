#include "core_functions/scalar/list_functions.hpp"

#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the bound lambda expression
	if (arguments[1]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	// bound_function.return_type = LogicalType::LIST(bound_lambda_expr.lambda_expr->return_type);
	auto has_index = arguments[1]->Cast<BoundLambdaExpression>().parameter_count == 2;
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, has_index);
}

static LogicalType ListTransformBindLambda(ClientContext &context, const vector<LogicalType> &function_child_types,
                                           const idx_t parameter_idx) {
	return LambdaFunctions::BindBinaryChildren(function_child_types, parameter_idx);
}

ScalarFunction ListTransformFun::GetFunction() {
	auto in_type = LogicalType::TEMPLATE("T");
	auto out_type = LogicalType::TEMPLATE("U");
	auto in_list_type = LogicalType::LIST(in_type);
	auto out_list_type = LogicalType::LIST(out_type);
	auto func_type = LogicalType::LAMBDA_TYPE({{"x", in_type}, {"i", LogicalType::BIGINT}}, out_type);

	ScalarFunction fun({in_list_type, func_type}, out_list_type, LambdaFunctions::ListTransformFunction,
	                   ListTransformBind, nullptr, nullptr);

	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	fun.bind_lambda = ListTransformBindLambda;

	return fun;
}

} // namespace duckdb
