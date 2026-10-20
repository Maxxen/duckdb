#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type,
                                                 vector<unique_ptr<Expression>> children_p)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, std::move(return_type)), children(std::move(children_p)) {
}

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type,
                                                 unique_ptr<Expression> child)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, std::move(return_type)) {
	children.push_back(std::move(child));
}

string BoundOperatorExpression::ToString() const {
	return OperatorExpression::ToString<BoundOperatorExpression, Expression>(*this);
}

bool BoundOperatorExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundOperatorExpression>();
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundOperatorExpression::Copy() const {
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto copy = make_uniq<BoundOperatorExpression>(type, return_type, std::move(new_children));
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
