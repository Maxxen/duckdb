//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/type_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class TypeExpression;
class Serializer;
class Deserializer;

class TypeDescriptor;

//! A single parameter of a TypeDescriptor. Following C++ template terminology, a parameter is either a
//! *non-type* parameter - a plain value, e.g. the width and scale of DECIMAL(18, 3) - or a *type*
//! parameter - a nested descriptor, e.g. the child of INTEGER[].
//!
//! A parameter may carry a label. STRUCT fields and UNION members are labelled type parameters; the
//! modifiers of DECIMAL are unlabelled non-type parameters.
class TypeParameter {
public:
	DUCKDB_API static TypeParameter NonType(Value value);
	DUCKDB_API static TypeParameter NonType(Identifier label, Value value);
	DUCKDB_API static TypeParameter Type(TypeDescriptor type);
	DUCKDB_API static TypeParameter Type(Identifier label, TypeDescriptor type);

	DUCKDB_API TypeParameter(const TypeParameter &other);
	DUCKDB_API TypeParameter &operator=(const TypeParameter &other);
	TypeParameter(TypeParameter &&other) noexcept = default;
	TypeParameter &operator=(TypeParameter &&other) noexcept = default;
	DUCKDB_API ~TypeParameter();

public:
	//! Whether this is a type parameter (as opposed to a non-type parameter)
	bool IsType() const {
		return type != nullptr;
	}
	//! The label, or an empty identifier when the parameter is unlabelled
	const Identifier &Label() const {
		return label;
	}
	bool HasLabel() const {
		return !label.empty();
	}
	//! The value of a non-type parameter. Only valid when !IsType()
	DUCKDB_API const Value &GetValue() const;
	//! The nested descriptor of a type parameter. Only valid when IsType()
	DUCKDB_API const TypeDescriptor &GetType() const;
	//! The nested descriptor of a type parameter, for rewriting it in place. Only valid when IsType()
	DUCKDB_API TypeDescriptor &GetTypeMutable();

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static TypeParameter Deserialize(Deserializer &deserializer);

	DUCKDB_API bool operator==(const TypeParameter &other) const;
	bool operator!=(const TypeParameter &other) const {
		return !(*this == other);
	}

private:
	TypeParameter() = default;

private:
	Identifier label;
	Value value;
	unique_ptr<TypeDescriptor> type;
};

//! An unbound description of a type: a qualified name plus folded parameters.
//!
//! A descriptor is what the catalog stores; a LogicalType is what you get from binding one. Unlike
//! TypeExpression - which is the parser's form, and whose children are unbound ParsedExpressions - a
//! descriptor's parameters are already folded to values, so binding one never evaluates an expression.
class TypeDescriptor {
public:
	DUCKDB_API explicit TypeDescriptor(QualifiedName name);
	DUCKDB_API TypeDescriptor(QualifiedName name, vector<TypeParameter> parameters);

public:
	const QualifiedName &Name() const {
		return name;
	}
	void SetName(QualifiedName name_p) {
		name = std::move(name_p);
	}
	const vector<TypeParameter> &Parameters() const {
		return parameters;
	}
	vector<TypeParameter> &Parameters() {
		return parameters;
	}

	//! Describe an already-bound type. Context-free: it never consults the catalog, so it is safe to
	//! call while loading a database.
	//! @throws NotImplementedException if the type cannot be described without a catalog - currently
	//! any type carrying an alias or extension info, which is migrated separately.
	DUCKDB_API static TypeDescriptor FromLogicalType(const LogicalType &type);
	//! As FromLogicalType, but returns nullptr instead of throwing when the type cannot be described.
	DUCKDB_API static unique_ptr<TypeDescriptor> TryFromLogicalType(const LogicalType &type);

	//! Resolve this descriptor using only the compiled-in built-in types, without a ClientContext.
	//! @throws BinderException if the name is not a built-in type, or the parameters do not fit it.
	DUCKDB_API LogicalType DefaultBind() const;

	//! This description as the equivalent parsed type expression
	DUCKDB_API unique_ptr<TypeExpression> ToTypeExpression() const;

	DUCKDB_API string ToString() const;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static unique_ptr<TypeDescriptor> Deserialize(Deserializer &deserializer);

	DUCKDB_API bool operator==(const TypeDescriptor &other) const;
	bool operator!=(const TypeDescriptor &other) const {
		return !(*this == other);
	}

private:
	QualifiedName name;
	vector<TypeParameter> parameters;
};

} // namespace duckdb
