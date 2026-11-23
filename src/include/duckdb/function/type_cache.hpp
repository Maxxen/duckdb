//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/type_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/type_constructor.hpp"

namespace duckdb {
class ClientContext;

//! Constructs the types that type names stand for, and remembers them: a type that is named more than once is
//! constructed once. A cache must not outlive the transaction it is used in
class TypeCache {
public:
	explicit TypeCache(optional_ptr<ClientContext> context_p) : context(context_p) {
	}

	optional_ptr<ClientContext> GetContext() const {
		return context;
	}
	//! The type of the given name, constructed from the arguments. Without a ClientContext only the built-in types
	//! are found. Returns INVALID if there is no type of that name
	LogicalType Construct(const Identifier &name, vector<TypeArgument> arguments);

private:
	struct Entry {
		Identifier name;
		vector<TypeArgument> arguments;
		LogicalType type;
	};

private:
	optional_ptr<ClientContext> context;
	//! The constructed types, by the hash of the name and the arguments of their constructor
	unordered_map<hash_t, vector<Entry>> entries;
};

} // namespace duckdb
