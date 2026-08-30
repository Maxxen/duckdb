//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/not_conjunction_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Applies De Morgan's law: NOT(a AND b) -> (NOT a) OR (NOT b), NOT(a OR b) -> (NOT a) AND (NOT b)
class NotConjunctionSimplificationRule : public Rule {
public:
	explicit NotConjunctionSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                             vector<reference<Expression>> &bindings, bool is_root) override;
};

} // namespace duckdb
