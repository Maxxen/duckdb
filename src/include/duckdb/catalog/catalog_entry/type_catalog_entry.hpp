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
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

//! A type catalog entry
class TypeCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TYPE_ENTRY;
	static constexpr const char *Name = "type";

public:
	//! Create a TypeCatalogEntry and initialize storage for it
	TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info);

	//! Whether this type keeps its own identity - see CreateTypeInfo::nominal
	bool nominal;

	bind_logical_type_function_t bind_function;

public:
	//! The definition of this type, as written. A user-created type constructs a type much as a macro
	//! constructs an expression, so the definition is kept unfolded - a parameterised type would reference
	//! its parameters, which a folded form cannot express. The entry never holds a resolved LogicalType,
	//! which it could not anyway: a checkpoint is read with no ClientContext to resolve one against.
	//!
	//! Read-only: a definition says what the type is made of, never what it is called, and one naming its
	//! own entry resolves to nothing. It is set once, by the constructor, which rejects that.
	const TypeExpression &Definition() const {
		D_ASSERT(type_expression);
		return *type_expression;
	}
	bool HasDefinition() const {
		return type_expression != nullptr;
	}

	//! The type this entry names, resolving its definition
	DUCKDB_API LogicalType GetType(ClientContext &context) const;
	//! The id of the type this entry names, without resolving it
	DUCKDB_API LogicalTypeId GetTypeId() const;

	//! Whether this entry names a type at all
	bool IsValid() const {
		return HasDefinition() || bind_function != nullptr;
	}
	//! Whether this entry is a template rather than a type: a parameterised built-in like ARRAY or MAP only
	//! becomes a type once modifiers are supplied, so there is nothing here to resolve.
	//!
	//! TODO: this is inferred from having a bind_function and no parameters. Once a type entry carries a
	//! FunctionSignature the distinction is explicit - a constructor like ARRAY(TYPE, N) has parameters, a
	//! leaf like INTEGER does not.
	bool IsTemplate() const {
		return bind_function && HasDefinition() && Definition().GetChildren().empty();
	}

	unique_ptr<CreateInfo> GetInfo() const override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	string ToSQL() const override;

private:
	unique_ptr<TypeExpression> type_expression;
};
} // namespace duckdb
