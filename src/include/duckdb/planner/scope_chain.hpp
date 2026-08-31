//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/scope_chain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Binder;
class ExpressionBinder;

//! An ordered view of the query scopes an expression can be resolved against, innermost first.
//! Index 0 is the scope that is currently binding, index d is d query levels outward - so the
//! index of a scope is the depth that a reference resolved against it is bound at.
class ScopeChain {
public:
	//! The chain of the given binder: the binder itself, followed by its enclosing scopes
	static ScopeChain FromBinder(ExpressionBinder &binder);

	idx_t Size() const {
		return scopes.size();
	}
	ExpressionBinder &At(idx_t depth) const {
		return scopes[depth];
	}

private:
	vector<reference<ExpressionBinder>> scopes;
};

} // namespace duckdb
