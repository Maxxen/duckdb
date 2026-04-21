#include "duckdb/function/window_function.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

unique_ptr<BoundWindowExpression> WindowFunction::Bind(ClientContext &context,
                                                       vector<unique_ptr<Expression>> arguments) {
	FunctionBinder binder(context);
	vector<OrderByNode> orders;
	vector<OrderByNode> arg_orders;
	AggregateType aggr_type = AggregateType::NON_DISTINCT;

	return binder.BindWindowFunction(*this, std::move(arguments), orders, arg_orders, aggr_type);
}

void Function::EraseArgument(BoundWindowFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                             idx_t argument_index) {
	if (bound_function.original_arguments.empty()) {
		bound_function.original_arguments = bound_function.arguments;
	}
	D_ASSERT(arguments.size() == bound_function.arguments.size());
	D_ASSERT(argument_index < arguments.size());
	arguments.erase_at(argument_index);
	bound_function.arguments.erase_at(argument_index);
}

} // namespace duckdb
