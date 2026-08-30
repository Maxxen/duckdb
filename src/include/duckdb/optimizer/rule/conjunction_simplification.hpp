//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/conjunction_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Conjunction Simplification rule rewrites conjunctions with a constant
class ConjunctionSimplificationRule : public Rule {
public:
	explicit ConjunctionSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;

	unique_ptr<Expression> RemoveExpression(BoundConjunctionExpression &conj, const Expression &expr);
};

} // namespace duckdb
