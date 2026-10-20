#include "duckdb/planner/filter/conjunction_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

LegacyConjunctionOrFilter::LegacyConjunctionOrFilter()
    : LegacyConjunctionFilter(TableFilterType::LEGACY_CONJUNCTION_OR) {
}

unique_ptr<Expression> LegacyConjunctionOrFilter::ToExpression(const Expression &column) const {
	vector<unique_ptr<Expression>> children;
	for (auto &filter : child_filters) {
		children.push_back(filter->ToExpression(column));
	}
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
}

LegacyConjunctionAndFilter::LegacyConjunctionAndFilter()
    : LegacyConjunctionFilter(TableFilterType::LEGACY_CONJUNCTION_AND) {
}

unique_ptr<Expression> LegacyConjunctionAndFilter::ToExpression(const Expression &column) const {
	vector<unique_ptr<Expression>> children;
	for (auto &filter : child_filters) {
		children.push_back(filter->ToExpression(column));
	}
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
}

} // namespace duckdb
