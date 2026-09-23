//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/type_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class TypeName;

//! What a type variable stands for: a type, or an integer / string type parameter such as a DECIMAL's width
enum class TypeVariableKind : uint8_t { TYPE, INTEGER, STRING };

//! Whether a type variable stands for a single value, or a pack of zero or more values (e.g. a STRUCT's fields)
enum class TypeVariableArity : uint8_t { SINGLE, PACK };

class TypeVariable {
public:
	TypeVariable(Identifier name, TypeVariableKind kind, TypeVariableArity arity = TypeVariableArity::SINGLE);

	const Identifier &GetName() const {
		return name;
	}
	TypeVariableKind GetKind() const {
		return kind;
	}
	TypeVariableArity GetArity() const {
		return arity;
	}

	string ToString() const;
	bool operator==(const TypeVariable &other) const;
	bool operator!=(const TypeVariable &other) const;

private:
	Identifier name;
	TypeVariableKind kind;
	TypeVariableArity arity;
};

enum class TypeParamKind : uint8_t { TYPE, INTEGER, STRING };

//! A single, optionally named, parameter of a TypeName: a type name, an integer or a string
class TypeParam {
public:
	static TypeParam Type(TypeName type, Identifier name = Identifier(), bool expand_pack = false);
	static TypeParam Integer(int64_t value, Identifier name = Identifier());
	static TypeParam String(string value, Identifier name = Identifier());

	TypeParamKind GetKind() const {
		return kind;
	}
	//! The name of the parameter, e.g. a STRUCT field name. Empty if the parameter is unnamed
	const Identifier &GetName() const {
		return name;
	}
	//! Whether this parameter expands a pack variable, i.e. is spelled "T..."
	bool ExpandsPack() const {
		return expand_pack;
	}
	const TypeName &GetType() const;
	int64_t GetInteger() const;
	const string &GetString() const;

	string ToString() const;
	bool operator==(const TypeParam &other) const;
	bool operator!=(const TypeParam &other) const;
	hash_t Hash() const;

private:
	TypeParam(TypeParamKind kind, Identifier name);

private:
	TypeParamKind kind;
	Identifier name;
	bool expand_pack = false;
	shared_ptr<const TypeName> type;
	int64_t integer = 0;
	string str;
};

//! An unbound, optionally qualified, type name with parameters, e.g. "DECIMAL(18, 3)", "LIST(T)" or "s.my_type".
//! A name is resolved against the type variables in scope first, and against the catalog otherwise
class TypeName {
public:
	TypeName() = default;
	explicit TypeName(QualifiedName name, vector<TypeParam> params = vector<TypeParam>());

	const QualifiedName &GetQualifiedName() const {
		return name;
	}
	const Identifier &GetName() const {
		return name.Name();
	}
	const vector<TypeParam> &GetParams() const {
		return params;
	}
	bool IsQualified() const {
		return name.Path().size() > 1;
	}
	bool IsValid() const {
		return !name.Path().empty();
	}
	//! Whether this can name a type variable: an unqualified name without parameters
	bool IsPlainName() const {
		return !IsQualified() && params.empty();
	}

	//! The qualifier of the placeholder types (ANY, INVALID, ...) that only exist in function signatures
	static const Identifier &PseudoTypeQualifier();
	bool IsPseudoType() const;
	//! Whether this is the given placeholder type
	bool IsPseudoType(LogicalTypeId id) const;

	//! Converts a bound type to the equivalent type name, declaring the type variables it introduces
	static TypeName FromLogicalType(const LogicalType &type, vector<TypeVariable> &variables);
	//! Converts a type that introduces no type variables, e.g. to compare it with the type of a signature
	static TypeName FromLogicalType(const LogicalType &type);

	//! Whether this is, or has as a (nested) parameter, an invalid type name
	bool ContainsInvalid() const;
	//! Whether this is, or has as a (nested) parameter, the given placeholder type
	bool ContainsPseudoType(LogicalTypeId id) const;

	//! The canonical name of a built-in type, or nullptr if the type id has none
	static const char *BuiltinName(LogicalTypeId id);

	string ToString() const;
	bool operator==(const TypeName &other) const;
	bool operator!=(const TypeName &other) const;
	hash_t Hash() const;

private:
	QualifiedName name;
	vector<TypeParam> params;
};

} // namespace duckdb
