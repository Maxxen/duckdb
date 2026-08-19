//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"

namespace duckdb {

//! A column of a table, as produced by the parser.
//!
//! The declared type is held as an unbound ParsedExpression (a TypeExpression) rather than a
//! LogicalType, and there is no oid or storage_oid - those are assigned during binding. The binder
//! resolves a ParsedColumnDefinition into a bound ColumnDefinition.
class ParsedColumnDefinition {
public:
	//! type_expression may be null when the type is to be inferred (a generated column without an
	//! explicit type, or a CTAS target column list)
	DUCKDB_API ParsedColumnDefinition(Identifier name, unique_ptr<TypeExpression> type_expression);
	DUCKDB_API ParsedColumnDefinition(Identifier name, unique_ptr<TypeExpression> type_expression,
	                                  unique_ptr<ParsedExpression> expression, TableColumnType category);

public:
	//! name
	DUCKDB_API const Identifier &Name() const;
	void SetName(const Identifier &name);

	//! type expression
	bool HasType() const;
	//! The type expression. Throws if there is none.
	DUCKDB_API const TypeExpression &Type() const;
	TypeExpression &TypeMutable();
	//! Raw (possibly null) handle to the type expression, for the binder to resolve
	const unique_ptr<TypeExpression> &GetTypeExpression() const;
	void SetTypeExpression(unique_ptr<TypeExpression> type_expression);
	unique_ptr<TypeExpression> CopyTypeExpression() const;

	//! default_value
	const ParsedExpression &DefaultValue() const;
	bool HasDefaultValue() const;
	void SetDefaultValue(unique_ptr<ParsedExpression> default_value);

	//! generated column expression
	const ParsedExpression &GeneratedExpression() const;
	void GetListOfDependencies(vector<string> &dependencies) const;

	//! category
	const TableColumnType &Category() const;
	//! Whether this column is a Generated Column
	bool Generated() const;

	//! comment
	const Value &Comment() const;
	void SetComment(const Value &comment);

	//! tags
	const InsertionOrderPreservingMap<string> &Tags() const;
	void SetTags(InsertionOrderPreservingMap<string> new_tags);

	//! compression_type
	const duckdb::CompressionType &CompressionType() const;
	void SetCompressionType(duckdb::CompressionType compression_type);

	DUCKDB_API ParsedColumnDefinition Copy() const;
	string ToSQLString() const;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static ParsedColumnDefinition Deserialize(Deserializer &deserializer);

private:
	//! Deserialization
	explicit ParsedColumnDefinition(Identifier name);

private:
	//! The name of the column
	Identifier name;
	//! The declared type of the column, as an unbound TypeExpression (null = infer)
	unique_ptr<TypeExpression> type_expression;
	//! The default value of the column (for non-generated columns)
	//! The generated column expression (for generated columns)
	unique_ptr<ParsedExpression> expression;
	//! The category of the column
	TableColumnType category = TableColumnType::STANDARD;
	//! Compression Type used for this column
	duckdb::CompressionType compression_type = duckdb::CompressionType::COMPRESSION_AUTO;
	//! Comment on this column
	Value comment;
	//! Tags on this column
	InsertionOrderPreservingMap<string> tags;
};

} // namespace duckdb
