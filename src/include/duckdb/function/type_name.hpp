//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/type_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/literal.hpp"

#include <variant>

namespace duckdb {

class TypeNameArgument;
struct TypeNameNode;

//! The name of a type as a function signature declares it: an identifier, optionally with arguments - INTEGER, T,
//! DECIMAL(18, 3), MAP(K, V[]). A name is not a type: it is bound to one when a call is bound to the function, as a
//! type variable of the signature or as a type of the catalog.
//! Names are immutable and cheap to copy, hash and compare
class TypeName {
public:
	DUCKDB_API explicit TypeName(Identifier name);
	DUCKDB_API TypeName(Identifier name, vector<TypeNameArgument> arguments);
	//! The name a type is declared by - a template is named by its type variable
	DUCKDB_API TypeName(const LogicalType &type); // NOLINT: allow implicit conversion

public:
	DUCKDB_API const Identifier &GetName() const;
	DUCKDB_API const vector<TypeNameArgument> &GetArguments() const;
	DUCKDB_API bool HasArguments() const;

	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;
	DUCKDB_API bool operator==(const TypeName &other) const;
	DUCKDB_API bool operator!=(const TypeName &other) const;

private:
	shared_ptr<const TypeNameNode> node;
};

//! An argument of a type name: a type name or a literal, optionally labelled - "a INTEGER" in STRUCT(a INTEGER), 18
//! in DECIMAL(18, 3)
class TypeNameArgument {
public:
	DUCKDB_API TypeNameArgument(TypeName type, Identifier label = Identifier());   // NOLINT: allow implicit conversion
	DUCKDB_API TypeNameArgument(Literal literal, Identifier label = Identifier()); // NOLINT: allow implicit conversion

public:
	bool IsType() const {
		return std::holds_alternative<TypeName>(value);
	}
	const TypeName &GetType() const {
		return std::get<TypeName>(value);
	}
	const Literal &GetLiteral() const {
		return std::get<Literal>(value);
	}
	const Identifier &GetLabel() const {
		return label;
	}

	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;
	DUCKDB_API bool operator==(const TypeNameArgument &other) const;
	DUCKDB_API bool operator!=(const TypeNameArgument &other) const;

private:
	std::variant<TypeName, Literal> value;
	Identifier label;
};

struct TypeNameNode {
	TypeNameNode(Identifier name, vector<TypeNameArgument> arguments);

	const Identifier name;
	const vector<TypeNameArgument> arguments;
	const hash_t hash;
};

} // namespace duckdb
