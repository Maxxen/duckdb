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
                         std::move(varargs), null_handling),
      function(std::move(function)), bind(bind), init_local_state(init_local_state), statistics(statistics),
      bind_lambda(bind_lambda), bind_expression(nullptr), get_modified_databases(nullptr), serialize(nullptr),
      deserialize(nullptr) {
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

pair<unique_ptr<BoundScalarFunction>, unique_ptr<FunctionData>>
ScalarFunction::Bind(ClientContext &context, vector<unique_ptr<Expression>> &arguments, optional_ptr<Binder> binder) {
	// Make a BoundFunction out of the func
	BoundScalarFunction bound_function(*this);

	unique_ptr<FunctionData> bind_info;
	if (bound_function.HasBindCallback()) {
		BindScalarFunctionInput input(context, bound_function, arguments, binder);
		bind_info = bound_function.GetBindCallback()(input);
	}

	// After the "bind" callback, we verify that all template types are bound to concrete types.
	// CheckTemplateTypesResolved(bound_function);

	if (bound_function.HasModifiedDatabasesCallback() && binder) {
		auto &properties = binder->GetStatementProperties();
		FunctionModifiedDatabasesInput input(bind_info, properties);
		bound_function.GetModifiedDatabasesCallback()(context, input);
	}

	HandleCollations(context, bound_function, arguments);

	return make_pair(make_uniq<BoundScalarFunction>(bound_function), std::move(bind_info));
}

} // namespace duckdb
