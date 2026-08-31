//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/scope_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/scope_chain.hpp"

namespace duckdb {
class ColumnRefExpression;
class FunctionExpression;
class ParsedExpression;

//! The outcome of resolving a name against a chain of query scopes
struct ColumnResolution {
	//! Whether some scope in the chain resolves the name
	bool found = false;
	//! The index of the resolving scope, i.e. the depth a reference to it binds at
	idx_t depth = 0;
	//! The qualified replacement produced by the resolving scope
	unique_ptr<ParsedExpression> qualified;
	//! The errors of every probed scope, combined - only set when the name is not found
	ErrorData error;
};

//! Determines which query scope owns a name or an expression, without binding anything.
//! Resolution consults exactly what a real bind against that scope would consult, by reusing
//! the qualifier each scope builds for itself.
class ScopeResolver {
public:
	//! Resolve a column reference against the chain, starting at the given depth
	static ColumnResolution ResolveColumn(const ScopeChain &chain, ColumnRefExpression &colref, idx_t start);

	//! The scope that owns an aggregate: the innermost one in which any of its arguments resolves a
	//! column of its own scope. Returns `start` when no argument resolves a column anywhere, which
	//! pins a constant-only aggregate to the scope it appears in.
	static idx_t ResolveAggregateOwner(const ScopeChain &chain, FunctionExpression &aggregate, idx_t start);

	//! The innermost scope whose groups all of the expressions match, or an invalid index if there is none
	static optional_idx ResolveOuterGroup(const ScopeChain &chain, vector<reference<ParsedExpression>> &expressions,
	                                      idx_t start);

	//! Merge the error of a newly probed scope into the error accumulated over the previous ones,
	//! preferring a missing column over any other error and merging the candidate bindings
	static void CombineErrors(ErrorData &current, ErrorData new_error);
};

} // namespace duckdb
