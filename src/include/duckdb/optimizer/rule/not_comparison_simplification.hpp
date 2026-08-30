//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/not_comparison_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Rewrites NOT(comparison) into the negated comparison: NOT(a > b) -> a <= b
class NotComparisonSimplificationRule : public Rule {
public:
	explicit NotComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
