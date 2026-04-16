#include "duckdb/function/window_function.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

pair<unique_ptr<BoundWindowFunction>, unique_ptr<FunctionData>> WindowFunction::Bind(ClientContext &context) {
	vector<unique_ptr<Expression>> arguments;
	return Bind(context, arguments);
}

pair<unique_ptr<BoundWindowFunction>, unique_ptr<FunctionData>>
WindowFunction::Bind(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
	FunctionBinder binder(context);

	// TODO: pass these things properly
	// vector<OrderByNode> orders;
	// vector<OrderByNode> arg_orders;
	// binder.BindWindowFunction(*this, arguments, orders, arg_orders);

	throw NotImplementedException("WindowFunction::Bind is not implemented yet!");
}

} // namespace duckdb
