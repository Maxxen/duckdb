#include "duckdb/planner/scope_chain.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

ScopeChain::ScopeChain(ExpressionBinder &current, Binder &owner) : current(current), owner(owner) {
#ifdef DEBUG
	initial_size = owner.GetEnclosingScopes().size() + 1;
#endif
}

ScopeChain ScopeChain::FromBinder(ExpressionBinder &binder) {
	return ScopeChain(binder, binder.GetBinder());
}

idx_t ScopeChain::Size() const {
	auto size = owner.get().GetEnclosingScopes().size() + 1;
	D_ASSERT(size == initial_size);
	return size;
}

ExpressionBinder &ScopeChain::At(idx_t depth) const {
	D_ASSERT(depth < Size());
	if (depth == 0) {
		return current;
	}
	// the enclosing scopes are stored outermost first, so the index is counted from the back
	auto &enclosing = owner.get().GetEnclosingScopes();
	return enclosing[enclosing.size() - depth];
}

} // namespace duckdb
