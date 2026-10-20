#include "duckdb/planner/expression/bound_subquery_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundSubqueryExpression::BoundSubqueryExpression(LogicalType return_type, shared_ptr<Binder> binder_p,
                                                 BoundStatement subquery_p, SubqueryType subquery_type_p,
                                                 ExpressionType comparison_type_p,
                                                 vector<unique_ptr<Expression>> children_p,
                                                 vector<LogicalType> child_types_p, vector<LogicalType> child_targets_p)
    : Expression(ExpressionType::SUBQUERY, ExpressionClass::BOUND_SUBQUERY, std::move(return_type)),
      binder(std::move(binder_p)), subquery(std::move(subquery_p)), subquery_type(subquery_type_p),
      children(std::move(children_p)), comparison_type(comparison_type_p), child_types(std::move(child_types_p)),
      child_targets(std::move(child_targets_p)) {
}

string BoundSubqueryExpression::ToString() const {
	return "SUBQUERY";
}

bool BoundSubqueryExpression::Equals(const BaseExpression &other_p) const {
	// equality between bound subqueries not implemented currently
	return false;
}

unique_ptr<Expression> BoundSubqueryExpression::Copy() const {
	throw SerializationException("Cannot copy BoundSubqueryExpression");
}

bool BoundSubqueryExpression::PropagatesNullValues() const {
	// TODO this can be optimized further by checking the actual subquery node
	return false;
}

bool BoundSubqueryExpression::IsVolatile() const {
	if (Expression::IsVolatile()) {
		return true;
	}
	return subquery.plan && subquery.plan->HasVolatileExpressions();
}

} // namespace duckdb
