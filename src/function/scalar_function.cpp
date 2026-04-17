#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunctionInfo::~ScalarFunctionInfo() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bind_scalar_function_t bind,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionStability side_effects, FunctionNullHandling null_handling,
                               bind_lambda_function_t bind_lambda)
    : BaseScalarFunction(std::move(name), std::move(arguments), std::move(return_type), side_effects,
                         std::move(varargs), null_handling) {
	this->function = std::move(function);
	this->bind = bind;
	this->statistics = statistics;
	this->init_local_state = init_local_state;
	this->bind_lambda = bind_lambda;
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bind_scalar_function_t bind, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs, FunctionStability side_effects,
                               FunctionNullHandling null_handling, bind_lambda_function_t bind_lambda)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), bind, statistics,
                     init_local_state, std::move(varargs), side_effects, null_handling, bind_lambda) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return name == rhs.name && signature == rhs.signature && bind == rhs.bind && statistics == rhs.statistics &&
	       bind_lambda == rhs.bind_lambda;
}

bool ScalarFunction::operator!=(const ScalarFunction &rhs) const {
	return !(*this == rhs);
}

bool ScalarFunction::Equal(const ScalarFunction &rhs) const {
	// signature
	if (this->signature != rhs.signature) {
		return false;
	}

	return true; // they are equal
}

void ScalarFunction::NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() >= 1);
	result.Reference(input.data[0]);
}

unique_ptr<BoundFunctionExpression>
ScalarFunction::Bind(ClientContext &context, vector<unique_ptr<Expression>> arguments, optional_ptr<Binder> binder) {
	FunctionBinder func_binder(context);
	auto expr = func_binder.BindScalarFunction(*this, std::move(arguments), binder);

	if (expr->GetExpressionType() != ExpressionType::BOUND_FUNCTION) {
		throw InvalidInputException("BindScalarFunction did not return a BoundFunctionExpression");
	}

	return unique_ptr_cast<Expression, BoundFunctionExpression>(std::move(expr));
}

} // namespace duckdb
