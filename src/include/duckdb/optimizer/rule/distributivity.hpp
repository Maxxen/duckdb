//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/distributivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

// (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
class DistributivityRule : public Rule {
public:
	explicit DistributivityRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;

private:
	void AddExpressionSet(Expression &expr, expression_set_t &set);
	unique_ptr<Expression> ExtractExpression(BoundConjunctionExpression &conj, idx_t idx, const Expression &expr);
};

} // namespace duckdb
