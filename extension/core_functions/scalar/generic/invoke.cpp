#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/function/lambda_functions.hpp"

namespace duckdb {

static unique_ptr<FunctionData> InvokeBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {

	if (arguments[0]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	auto &bound_lambda_expr = arguments[0]->Cast<BoundLambdaExpression>();
	bound_function.return_type = bound_lambda_expr.lambda_expr->return_type;

	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(bound_lambda_expr.lambda_expr));
}

static LogicalType InvokeLambdaBind(const idx_t parameter_idx, const LogicalType &param_type) {
	return param_type;
}

static void InvokeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// TODO:
	const auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &bind_info = func_expr.bind_info->Cast<ListLambdaBindData>();

	ExpressionExecutor executor(state.GetContext(), *bind_info.lambda_expr);
	executor.ExecuteExpression(args, result);
}

ScalarFunction InvokeFun::GetFunction() {

	ScalarFunction invoke_fun({LogicalType::ANY}, LogicalType::ANY, InvokeFunction, InvokeBind);

	invoke_fun.varargs = LogicalType::ANY;
	invoke_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	invoke_fun.serialize = ListLambdaBindData::Serialize;
	invoke_fun.deserialize = ListLambdaBindData::Deserialize;
	invoke_fun.bind_lambda = InvokeLambdaBind;

	return invoke_fun;
}

} // namespace duckdb
