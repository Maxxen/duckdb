//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/logical_operator_matcher.hpp"

namespace duckdb {
class ExpressionRewriter;

class Rule {
public:
	explicit Rule(ExpressionRewriter &rewriter) : rewriter(rewriter) {
	}
	virtual ~Rule() {
	}

	//! The expression rewriter this rule belongs to
	ExpressionRewriter &rewriter;
	//! The expression matcher of the rule
	unique_ptr<ExpressionMatcher> root;

	ClientContext &GetContext() const;
	//! Apply the rule to the matched expression. `expr_ptr` owns the expression the bindings point into.
	//! Return the replacement expression - which may be built by taking apart `expr_ptr`, or be
	//! `std::move(expr_ptr)` itself after modifying it - or return nullptr to leave the expression untouched.
	virtual unique_ptr<Expression> Apply(LogicalOperator &op, unique_ptr<Expression> &expr_ptr,
	                                     vector<reference<Expression>> &bindings, bool is_root) = 0;
};

} // namespace duckdb
