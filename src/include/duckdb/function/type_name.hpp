//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/type_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/literal.hpp"

#include <variant>

namespace duckdb {

class TypeNameArgument;
struct TypeNameNode;

//! How many types a variable of a signature stands for (TypeVariable), and how the expansion of a pack gives them
//! where a type name uses it
//! SINGLE          -> one
//! POSITIONAL_PACK -> "*T", any number in order
//! NAMED_PACK      -> "**KV", any number, each with a label
enum class TypeMultiplicity : uint8_t { SINGLE = 0, POSITIONAL_PACK, NAMED_PACK };

//! The name of a type as a function signature declares it: an identifier, optionally with arguments - INTEGER, T,
//! DECIMAL(18, 3), MAP(K, V[]). A name is not a type: it is bound to one when a call is bound to the function, as a
//! type variable of the signature or as a type of the catalog.
//! Where a pack is used it is expanded, which is a node of its own: "*T" in TUPLE(*T) is the expansion of the
//! pattern T, "**KV" in STRUCT(**KV) the named expansion of KV. An expansion stands for its pattern once for every
//! element of the pack the pattern uses. It is not a type name: it has no name and no arguments, only its pattern.
//! Names are immutable and cheap to copy, hash and compare
class TypeName {
public:
	DUCKDB_API explicit TypeName(Identifier name);
	DUCKDB_API TypeName(Identifier name, vector<TypeNameArgument> arguments);
	//! The name a type is declared by
	DUCKDB_API TypeName(const LogicalType &type); // NOLINT: allow implicit conversion
	//! For the type constants, e.g. LogicalType::INTEGER
	DUCKDB_API TypeName(LogicalTypeId type_id); // NOLINT: allow implicit conversion

public:
	//! The name of a variable of a signature, which the signature declares: FunctionSignature::AddVariable
	DUCKDB_API static TypeName VARIABLE(const string &name);
	//! The use of a pack of a signature, "*name": the types it stands for in order, FunctionSignature::AddPack. It is
	//! the expansion of the pack by itself, EXPAND(VARIABLE(name)). In a return type the pack can be a named one:
	//! its types without their labels
	DUCKDB_API static TypeName PACK(const string &name);
	//! The use of a named pack of a signature, "**name": its labelled types, FunctionSignature::AddNamedPack
	DUCKDB_API static TypeName NAMED_PACK(const string &name);
	//! The expansion of a pattern that uses a pack, "*(pattern)": the pattern once for every element of the pack
	DUCKDB_API static TypeName EXPAND(TypeName pattern);
	//! The same with the label of every element of a named pack, "**(pattern)"
	DUCKDB_API static TypeName EXPAND_NAMED(TypeName pattern);
	//! The names of nested types, as LogicalType::LIST etc. build the types
	DUCKDB_API static TypeName LIST(TypeName child);
	DUCKDB_API static TypeName ARRAY(TypeName child, TypeName size);
	DUCKDB_API static TypeName ARRAY(TypeName child, idx_t size);
	DUCKDB_API static TypeName MAP(TypeName key, TypeName value);
	DUCKDB_API static TypeName DECIMAL(TypeNameArgument width, TypeNameArgument scale);
	DUCKDB_API static TypeName STRUCT(vector<pair<string, TypeName>> children);
	DUCKDB_API static TypeName TUPLE(vector<TypeName> children);

public:
	//! Whether this is the expansion of a pattern rather than a type name
	inline bool IsExpansion() const;
	//! How an expansion gives the elements: POSITIONAL_PACK for "*", NAMED_PACK for "**". Only for an expansion
	inline TypeMultiplicity GetExpansionKind() const;
	//! The pattern an expansion stands for once for every element. Only for an expansion
	inline const TypeName &GetPattern() const;

	//! The name and the arguments of a type name. Not for an expansion, which has neither: its pattern has them
	inline const Identifier &GetName() const;
	inline const vector<TypeNameArgument> &GetArguments() const;
	inline bool HasArguments() const;
	//! The built-in type a name without arguments stands for, INVALID if it is not the name of one - as an expansion
	//! is not
	inline LogicalTypeId GetBuiltinType() const;
	//! Whether this is the name of the given built-in type
	bool IsBuiltinType(LogicalTypeId id) const {
		return id != LogicalTypeId::INVALID && GetBuiltinType() == id;
	}

	//! Whether this name, a type name it takes as an argument, or the pattern of an expansion is the given name
	DUCKDB_API bool Contains(const Identifier &name) const;

	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;
	DUCKDB_API bool operator==(const TypeName &other) const;
	DUCKDB_API bool operator!=(const TypeName &other) const;

private:
	explicit TypeName(shared_ptr<const TypeNameNode> node);

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

//! What a TypeName shares: a type name or the expansion of a pattern, and its hash
struct TypeNameNode {
	//! A type name: an identifier with its arguments
	struct Name {
		Identifier name;
		vector<TypeNameArgument> arguments;
		//! The built-in type the name stands for, INVALID if it is not the name of one
		LogicalTypeId builtin_type;
	};
	//! The expansion of a pattern: the pattern once for every element of the pack it uses
	struct Expansion {
		//! POSITIONAL_PACK or NAMED_PACK
		TypeMultiplicity kind;
		TypeName pattern;
	};

	TypeNameNode(Identifier name, vector<TypeNameArgument> arguments,
	             LogicalTypeId builtin_type = LogicalTypeId::INVALID);
	TypeNameNode(TypeMultiplicity kind, TypeName pattern);

	//! The type name the node is. An expansion is not one: asking it for a name or for arguments would treat it as
	//! the pattern it expands
	const Name &GetName() const {
		auto name = std::get_if<Name>(&value);
		if (!name) {
			ThrowNotAName();
		}
		return *name;
	}
	const Expansion &GetExpansion() const {
		auto expansion = std::get_if<Expansion>(&value);
		if (!expansion) {
			ThrowNotAnExpansion();
		}
		return *expansion;
	}
	//! Whether the node is an expansion rather than a type name
	bool IsExpansion() const {
		return std::holds_alternative<Expansion>(value);
	}
	//! The type name the node is, nothing for an expansion
	optional_ptr<const Name> TryGetName() const {
		return std::get_if<Name>(&value);
	}

	const hash_t hash;

private:
	[[noreturn]] DUCKDB_API void ThrowNotAName() const;
	[[noreturn]] DUCKDB_API void ThrowNotAnExpansion() const;

private:
	const std::variant<Name, Expansion> value;
};

bool TypeName::IsExpansion() const {
	return node->IsExpansion();
}

TypeMultiplicity TypeName::GetExpansionKind() const {
	return node->GetExpansion().kind;
}

const TypeName &TypeName::GetPattern() const {
	return node->GetExpansion().pattern;
}

const Identifier &TypeName::GetName() const {
	return node->GetName().name;
}

const vector<TypeNameArgument> &TypeName::GetArguments() const {
	return node->GetName().arguments;
}

bool TypeName::HasArguments() const {
	return !node->GetName().arguments.empty();
}

LogicalTypeId TypeName::GetBuiltinType() const {
	// an expansion is not the name of a built-in type
	auto name = node->TryGetName();
	return name ? name->builtin_type : LogicalTypeId::INVALID;
}

} // namespace duckdb
