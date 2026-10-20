#include "duckdb/planner/expression/bound_unnest_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BoundUnnestExpression::BoundUnnestExpression(LogicalType return_type, unique_ptr<Expression> child_p)
    : Expression(ExpressionType::BOUND_UNNEST, ExpressionClass::BOUND_UNNEST, std::move(return_type)),
      child(std::move(child_p)) {
}

bool BoundUnnestExpression::IsFoldable() const {
	return false;
}

string BoundUnnestExpression::ToString() const {
	return "UNNEST(" + child->ToString() + ")";
}

hash_t BoundUnnestExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, duckdb::Hash("unnest"));
}

bool BoundUnnestExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundUnnestExpression>();
	if (!Expression::Equals(*child, *other.child)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundUnnestExpression::Copy() const {
	return make_uniq<BoundUnnestExpression>(return_type, child->Copy());
}

} // namespace duckdb
