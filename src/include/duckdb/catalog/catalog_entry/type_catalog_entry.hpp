//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/type_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

//! A type catalog entry
class TypeCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TYPE_ENTRY;
	static constexpr const char *Name = "type";

public:
	//! Create a TypeCatalogEntry and initialize storage for it
	TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info);

	//! The definition of a type created through SQL, as a normalized type expression: constant-folded, with any
	//! referenced user types inlined, so it names only built-in types and can be re-bound on its own.
	//! Null for built-in and extension-provided types, which are primitives described by `user_type` instead -
	//! and which must not get one, since their definition would name the very entry being resolved.
	//!
	//! TODO: this should become a TypeDescriptor (see TYPE_DESCRIPTOR_PLAN.md). A TypeExpression is a parse tree
	//! and carries more than the catalog needs - unbound ParsedExpression children, query locations, aliases -
	//! whereas what is actually stored here is always already folded. A descriptor is that, as a first-class
	//! serializable form, and would remove the need to re-bind on every lookup.
	unique_ptr<TypeExpression> type_expression;

	//! The type this entry names, when it is not described by `type_expression`. For a parameterised built-in
	//! this is only the base its `bind_function` is applied to (e.g. DECIMAL with no width), not a usable type.
	LogicalType user_type;

	bind_logical_type_function_t bind_function;

public:
	//! The type this entry names, resolving `type_expression` if it has one
	DUCKDB_API LogicalType GetType(ClientContext &context) const;
	//! Whether this entry names a type at all
	bool IsValid() const {
		return type_expression != nullptr || user_type.id() != LogicalTypeId::INVALID;
	}

	unique_ptr<CreateInfo> GetInfo() const override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	string ToSQL() const override;
};
} // namespace duckdb
