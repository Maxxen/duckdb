#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"

namespace duckdb {

namespace {

static void PropagateLambdaProperties(BoundScalarFunction &function, const Expression &lambda_expr) {
	if (lambda_expr.IsVolatile()) {
		function.SetVolatile();
	}
	if (lambda_expr.CanThrow()) {
		function.SetFallible();
	}
}

struct LambdaInvokeData final : public LambdaFunctionData {
	unique_ptr<Expression> lambda_expr;

	explicit LambdaInvokeData(unique_ptr<Expression> lambda_expr_p) : lambda_expr(std::move(lambda_expr_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto lambda_expr_copy = lambda_expr ? lambda_expr->Copy() : nullptr;
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr_copy));
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LambdaInvokeData>();
		return Expression::Equals(lambda_expr, other.lambda_expr);
	}

	//! Serializes a lambda function's bind data
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const BoundScalarFunction &function) {
		auto &bind_data = bind_data_p->Cast<LambdaInvokeData>();
		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr, unique_ptr<Expression>());
	}

	//! Deserializes a lambda function's bind data
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, BoundScalarFunction &function) {
		auto lambda_expr = deserializer.ReadPropertyWithExplicitDefault<unique_ptr<Expression>>(
		    101, "lambda_expr", unique_ptr<Expression>());
		if (lambda_expr) {
			auto &bound_lambda_expr = lambda_expr->Cast<BoundLambdaExpression>();
			PropagateLambdaProperties(function, *bound_lambda_expr.LambdaExpr());
		}
		return make_uniq<LambdaInvokeData>(std::move(lambda_expr));
	}

	unique_ptr<Expression> RecoverLambdaChild() const override {
		return lambda_expr ? lambda_expr->Copy() : nullptr;
	}

	optional_ptr<const Expression> GetLambdaExpression() const override {
		if (!lambda_expr) {
			return nullptr;
		}
		auto &bound_lambda_expr = lambda_expr->Cast<BoundLambdaExpression>();
		return bound_lambda_expr.LambdaExpr().get();
	}
};

struct LambdaInvokeState final : public FunctionLocalState {
	unique_ptr<ExpressionExecutor> executor;
	DataChunk input_chunk;
	idx_t parameter_count;

	LambdaInvokeState(unique_ptr<ExpressionExecutor> executor_p, const vector<LogicalType> &input_types,
	                  const idx_t parameter_count_p)
	    : executor(std::move(executor_p)), parameter_count(parameter_count_p) {
		input_chunk.InitializeEmpty(input_types);
	}

	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data) {
		auto &bdata = bind_data->Cast<LambdaInvokeData>();
		if (!bdata.lambda_expr) {
			throw InternalException("Invoke function is missing its bound lambda expression");
		}
		auto &bound_lambda_expr = bdata.lambda_expr->Cast<BoundLambdaExpression>();
		const auto parameter_count = bound_lambda_expr.ParameterCount();
		D_ASSERT(parameter_count < expr.GetChildren().size());
		D_ASSERT(expr.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA);

		// children[0] is the lambda placeholder itself, the parameters follow it. The lambda body refers
		// to its parameters in reverse order, so the input chunk lists them reversed
		vector<LogicalType> input_types;
		input_types.reserve(expr.GetChildren().size() - 1);
		for (idx_t i = 0; i < parameter_count; i++) {
			input_types.push_back(expr.GetChildren()[parameter_count - i]->GetReturnType());
		}
		// the captures of the lambda follow the parameters - they are passed after the arguments
		for (auto &capture : expr.GetChildren()[0]->Cast<BoundLambdaExpression>().Captures()) {
			input_types.push_back(capture->GetReturnType());
		}

		auto executor = make_uniq<ExpressionExecutor>(state.GetContext(), *bound_lambda_expr.LambdaExpr());
		return make_uniq<LambdaInvokeState>(std::move(executor), input_types, parameter_count);
	}
};

void LambdaInvokeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<LambdaInvokeState>();
	for (idx_t i = 0; i < lstate.parameter_count; i++) {
		lstate.input_chunk.data[i].Reference(args.data[lstate.parameter_count - i]);
	}
	// the lambda evaluates to its captures, which follow the parameters
	auto &captures = StructVector::GetEntries(args.data[0]);
	for (idx_t i = 0; i < captures.size(); i++) {
		lstate.input_chunk.data[lstate.parameter_count + i].Reference(captures[i]);
	}
	lstate.input_chunk.SetChildCardinality(args.size());
	lstate.executor->ExecuteExpression(lstate.input_chunk, result);
}

unique_ptr<FunctionData> LambdaInvokeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments[0]->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression passed to 'invoke' function.");
	}
	auto &bound_lambda_expr = arguments[0]->Cast<BoundLambdaExpression>();
	bound_function.SetReturnType(bound_lambda_expr.LambdaExpr()->GetReturnType());
	PropagateLambdaProperties(bound_function, *bound_lambda_expr.LambdaExpr());
	return make_uniq<LambdaInvokeData>(bound_lambda_expr.Copy());
}

} // namespace

ScalarFunction InvokeFun::GetFunction() {
	// the lambda takes one parameter for every argument
	FunctionSignature signature;
	signature.AddParameter("lambda", LogicalType::LAMBDA({}, LogicalType::ANY, LogicalType::ANY))
	    .AddArgs("args", LogicalType::ANY);
	signature.SetReturnType(LogicalType::ANY);

	ScalarFunction fun("invoke", std::move(signature), LambdaInvokeFunction);
	fun.SetBindCallback(LambdaInvokeBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetInitStateCallback(LambdaInvokeState::Init);
	fun.SetSerializeCallback(LambdaInvokeData::Serialize);
	fun.SetDeserializeCallback(LambdaInvokeData::Deserialize);
	fun.SetFallible();
	return fun;
}

} // namespace duckdb
