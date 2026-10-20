#include "core_functions/scalar/list_functions.hpp"

#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListFilterBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	// the list column and the bound lambda expression
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();

	// the binder casts the body to the BOOLEAN the lambda signature declares
	D_ASSERT(bound_lambda_expr.LambdaExpr()->GetReturnType() == LogicalType::BOOLEAN);

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	bound_function.SetReturnType(arguments[0]->GetReturnType());
	auto has_index = bound_lambda_expr.ParameterCount() == 2;
	return LambdaFunctions::ListLambdaBind(context, bound_function, arguments, has_index);
}

ScalarFunctionSet ListFilterFun::GetFunctions() {
	ScalarFunctionSet set;
	for (const bool has_index : {false, true}) {
		vector<LogicalType> lambda_parameters {LogicalType::TEMPLATE("T")};
		if (has_index) {
			lambda_parameters.push_back(LogicalType::BIGINT);
		}
		FunctionSignature signature;
		signature.AddParameter("list", LogicalType::LIST(LogicalType::TEMPLATE("T")))
		    .AddParameter("lambda", LogicalType::LAMBDA(std::move(lambda_parameters), LogicalType::BOOLEAN));
		signature.SetReturnType(LogicalType::LIST(LogicalType::TEMPLATE("T")));

		ScalarFunction fun(Identifier(), std::move(signature), LambdaFunctions::ListFilterFunction);
		fun.SetBindCallback(ListFilterBind);
		fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		fun.SetSerializeCallback(ListLambdaBindData::Serialize);
		fun.SetDeserializeCallback(ListLambdaBindData::Deserialize);
		fun.SetFallible();
		set.AddFunction(fun);
	}
	return set;
}

} // namespace duckdb
