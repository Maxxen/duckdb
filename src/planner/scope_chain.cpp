#include "duckdb/planner/scope_chain.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

ScopeChain ScopeChain::FromBinder(ExpressionBinder &binder) {
	ScopeChain result;
	// the enclosing scopes are stored outermost first - reverse them so that the index is the depth
	auto &enclosing = binder.GetBinder().GetEnclosingScopes();
	result.scopes.reserve(enclosing.size() + 1);
	result.scopes.push_back(binder);
	for (auto entry = enclosing.rbegin(); entry != enclosing.rend(); entry++) {
		result.scopes.push_back(*entry);
	}
	return result;
}

} // namespace duckdb
