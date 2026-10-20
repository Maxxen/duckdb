#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, vector<unique_ptr<Expression>> children_p)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, LogicalType::BOOLEAN), children(std::move(children_p)) {
}

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, LogicalType::BOOLEAN) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

unique_ptr<Expression> BoundConjunctionExpression::Create(ExpressionType type,
                                                          vector<unique_ptr<Expression>> children) {
	D_ASSERT(!children.empty());
	if (children.size() == 1) {
		return std::move(children[0]);
	}
	return make_uniq<BoundConjunctionExpression>(type, std::move(children));
}

string BoundConjunctionExpression::ToString() const {
	return ConjunctionExpression::ToString<BoundConjunctionExpression, Expression>(*this);
}

bool BoundConjunctionExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundConjunctionExpression>();
	return ExpressionUtil::SetEquals(children, other.children);
}

bool BoundConjunctionExpression::PropagatesNullValues() const {
	return false;
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() const {
	vector<unique_ptr<Expression>> new_children;
	for (auto &expr : children) {
		new_children.push_back(expr->Copy());
	}
	auto copy = make_uniq<BoundConjunctionExpression>(type, std::move(new_children));
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
