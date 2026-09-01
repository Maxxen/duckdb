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
//! This is a view over the binder's enclosing scopes, not a copy of them, so it must not outlive
//! them and the scopes must not change while it is in use.
class ScopeChain {
public:
	//! The chain of the given binder: the binder itself, followed by its enclosing scopes
	static ScopeChain FromBinder(ExpressionBinder &binder);

	idx_t Size() const;
	ExpressionBinder &At(idx_t depth) const;

private:
	ScopeChain(ExpressionBinder &current, Binder &owner);

private:
	reference<ExpressionBinder> current;
	reference<Binder> owner;
#ifdef DEBUG
	//! The scope count when the chain was built - a scope pushed or popped underneath a live chain
	//! would shift every index, and the index of a scope is a depth
	idx_t initial_size;
#endif
};

} // namespace duckdb
