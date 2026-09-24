//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/signature_resolver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
class TypeArgument;

//! The types that names resolved to while the overloads of one function are matched, so that a name recurring across
//! them is only resolved once. The overloads share a catalog and schema, so the name alone identifies the type. Only
//! names that refer to no type variables are remembered. The names are referenced from the signatures of the
//! overloads, which outlive the matching
using SignatureTypeCache = unordered_map<reference<const TypeName>, LogicalType, TypeNameHash, TypeNameEquality>;

//! Resolves the TypeNames of a function signature to LogicalTypes. A name is resolved against the signature's type
//! variables first, and otherwise looked up as a type: in the catalog and schema of the function that owns the
//! signature, and then in the system catalog. Without a client context only the default types are available.
//! Type variables are lowered to the placeholder types the function binder works with: a type variable becomes a
//! TEMPLATE, and a type whose parameters are all variables that occur nowhere else becomes its unparameterized form.
class SignatureResolver {
public:
	SignatureResolver(optional_ptr<ClientContext> context, const FunctionSignature &signature,
	                  Identifier catalog = Identifier(), Identifier schema = Identifier(),
	                  optional_ptr<SignatureTypeCache> cache = nullptr);
	SignatureResolver(optional_ptr<ClientContext> context, const SimpleFunction &function,
	                  optional_ptr<SignatureTypeCache> cache = nullptr);

	LogicalType Resolve(const TypeName &type) const;
	//! Like Resolve, but returns INVALID instead of throwing when a name is not a default type and there is no context
	LogicalType TryResolve(const TypeName &type) const;

	LogicalType ResolveParameter(idx_t index) const;
	vector<LogicalType> ResolveParameters() const;
	LogicalType ResolveReturnType() const;
	//! The type of the "*args" parameter, or INVALID if there is none
	LogicalType ResolveVarArgs() const;

	//! Resolves a type name that refers to no type variables
	static LogicalType ResolveType(optional_ptr<ClientContext> context, const TypeName &type);
	//! Renders a type of the signature the way the LogicalType it resolves to renders. A type that cannot be resolved
	//! without a client context is rendered by its name
	string TypeToString(const TypeName &type) const;

private:
	LogicalType ResolveInternal(const TypeName &type) const;
	bool IsVariable(const TypeParam &param) const;
	bool ReferencesVariables(const TypeName &type) const;
	bool IsOwnedBySystemCatalog() const;
	bool IsFamilyOfUnusedVariables(const TypeName &type) const;
	LogicalType ResolveFamily(const TypeName &type, LogicalTypeId id) const;
	LogicalType LookupType(const TypeName &type, const vector<TypeArgument> &arguments) const;

private:
	optional_ptr<ClientContext> context;
	const FunctionSignature &signature;
	Identifier catalog;
	Identifier schema;
	mutable optional_ptr<SignatureTypeCache> cache;
	//! How often each type variable occurs in the signature
	identifier_map_t<idx_t> occurrences;
	//! Whether an unresolvable name yields INVALID rather than an error
	mutable bool try_resolve = false;
};

} // namespace duckdb
