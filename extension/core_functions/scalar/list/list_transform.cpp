#include "core_functions/scalar/list_functions.hpp"

#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListTransformBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	bound_function.SetReturnType(LogicalType::LIST(bound_lambda_expr.LambdaExpr()->GetReturnType()));
	auto has_index = bound_lambda_expr.ParameterCount() == 2;
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, has_index);
}

ScalarFunctionSet ListTransformFun::GetFunctions() {
	ScalarFunctionSet set;
	for (const bool has_index : {false, true}) {
		vector<LogicalType> lambda_parameters {LogicalType::TEMPLATE("T")};
		if (has_index) {
			lambda_parameters.push_back(LogicalType::BIGINT);
		}
		FunctionSignature signature;
		signature.AddParameter("list", LogicalType::LIST(LogicalType::TEMPLATE("T")))
		    .AddParameter("lambda", LogicalType::LAMBDA(std::move(lambda_parameters), LogicalType::TEMPLATE("U")));
		signature.SetReturnType(LogicalType::LIST(LogicalType::TEMPLATE("U")));

		ScalarFunction fun(Identifier(), std::move(signature), LambdaFunctions::ListTransformFunction);
		fun.SetBindCallback(ListTransformBind);
		fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		fun.SetSerializeCallback(ListLambdaBindData::Serialize);
		fun.SetDeserializeCallback(ListLambdaBindData::Deserialize);
		fun.SetFallible();
		set.AddFunction(fun);
	}
	return set;
}

} // namespace duckdb
