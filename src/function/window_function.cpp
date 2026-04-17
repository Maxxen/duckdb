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

} // namespace duckdb
