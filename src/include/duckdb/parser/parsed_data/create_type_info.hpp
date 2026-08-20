//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

class TypeArgument {
public:
	TypeArgument(string name_p, Value value_p) : name(std::move(name_p)), value(std::move(value_p)) {
	}
	const string &GetName() const {
		return name;
	}
	const Value &GetValue() const {
		return value;
	}
	bool HasName() const {
		return !name.empty();
	}
	bool IsNamed(const char *name_to_check) const {
		return StringUtil::CIEquals(name, name_to_check);
	}
	bool IsNotNull() const {
		return !value.IsNull();
	}
	const LogicalType &GetType() const {
		return value.type();
	}

private:
	string name;
	Value value;
};

struct BindLogicalTypeInput {
	optional_ptr<ClientContext> context;
	const vector<TypeArgument> &modifiers;
};

//! The type to bind type modifiers to a type
typedef LogicalType (*bind_logical_type_function_t)(BindLogicalTypeInput &input);

struct CreateTypeInfo : public CreateInfo {
	CreateTypeInfo();
	CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p = nullptr);

	//! Name of the Type
	const Identifier &GetTypeName() const {
		return qualified_name.Name();
	}
	void SetTypeName(Identifier name) {
		qualified_name = qualified_name.WithName(std::move(name));
	}
	//! The definition of the type, as written. A user-created type constructs a type much as a macro
	//! constructs an expression, so the definition stays an expression rather than being folded - a
	//! parameterised type would reference its parameters, which a folded form cannot express.
	//!
	//! This says what the type is made of, never what it is called: a definition naming its own entry
	//! resolves to nothing. To register a type from a LogicalType, use the constructor below - it strips
	//! the alias and sets `nominal`, so the name is re-applied on resolution instead.
	unique_ptr<TypeExpression> type_expression;
	//! Whether this type keeps its own identity. An extension-registered type is nominal: the description
	//! says what it is made of, and the entry's name is applied on top. A type created with CREATE TYPE is
	//! not - it is an alias, and resolves to exactly what it describes.
	bool nominal = false;
	//! Used by create enum from query
	unique_ptr<SQLStatement> query;
	//! Bind type modifiers to the type
	bind_logical_type_function_t bind_function;

public:
	unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace duckdb
