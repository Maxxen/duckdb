//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! CastExpression represents a type cast from one SQL type to another SQL type
class CastExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::CAST;

public:
	//! Cast to a type that still has to be bound (a TypeExpression)
	DUCKDB_API CastExpression(unique_ptr<ParsedExpression> target, unique_ptr<ParsedExpression> child,
	                          bool try_cast = false);
	//! Cast to a type that is already known. An UNBOUND type contributes its type expression; any other type
	//! is folded into a constant, so the target is always a parsed expression.
	DUCKDB_API CastExpression(const LogicalType &target, unique_ptr<ParsedExpression> child, bool try_cast = false);

public:
	//! Turns a type into the parsed expression a CastExpression stores it as
	DUCKDB_API static unique_ptr<ParsedExpression> TypeExpressionFrom(const LogicalType &type);

	const ParsedExpression &TargetType() const {
		return *cast_type;
	}
	const unique_ptr<ParsedExpression> &GetTargetType() const {
		return cast_type;
	}
	ParsedExpression &TargetTypeMutable() {
		return *cast_type;
	}
	void SetTargetType(unique_ptr<ParsedExpression> target);
	//! The target as a LogicalType: the resolved type if it has been folded to a constant, an UNBOUND type
	//! wrapping the type expression otherwise. Used at the serialization seam.
	DUCKDB_API LogicalType GetTargetLogicalType() const;

	const ParsedExpression &Child() const {
		return *child;
	}
	unique_ptr<ParsedExpression> &ChildMutable() {
		return child;
	}
	bool IsTryCast() const {
		return try_cast;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return (entry.IsTryCast() ? "TRY_CAST(" : "CAST(") + entry.Child().ToString() + " AS " +
		       entry.TargetType().ToString() + ")";
	}

private:
	//! The child of the cast expression
	unique_ptr<ParsedExpression> child;
	//! The type to cast to, as an unbound type expression
	unique_ptr<ParsedExpression> cast_type;
	//! Whether or not this is a try_cast expression
	bool try_cast;

private:
	CastExpression();
};
} // namespace duckdb
