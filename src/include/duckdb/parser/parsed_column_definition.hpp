//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"

namespace duckdb {

//! A column of a table, as produced by the parser.
//!
//! Unlike ColumnDefinition (the bound representation used by the catalog and storage), the
//! declared type of a ParsedColumnDefinition is represented as an (unbound) ParsedExpression
//! (a TypeExpression) rather than a LogicalType. The binder resolves this into a concrete
//! LogicalType and produces a bound ColumnDefinition. A ParsedColumnDefinition never carries
//! a LogicalType, and never holds storage information (oid / storage_oid), which are assigned
//! during binding.
class ParsedColumnDefinition {
public:
	//! Standard column: type_expression may be null when the type is to be inferred (e.g. a
	//! generated column without an explicit type).
	DUCKDB_API ParsedColumnDefinition(string name, unique_ptr<ParsedExpression> type_expression);
	DUCKDB_API ParsedColumnDefinition(string name, unique_ptr<ParsedExpression> type_expression,
	                                  unique_ptr<ParsedExpression> expression, TableColumnType category);

public:
	//! name
	DUCKDB_API const string &Name() const;

	//! type expression (TypeExpression). May be null when the type is to be inferred.
	bool HasType() const;
	//! The type expression. Asserts HasType().
	const ParsedExpression &Type() const;
	//! Raw (possibly null) handle to the type expression, for the binder to resolve.
	const unique_ptr<ParsedExpression> &GetTypeExpression() const;

	//! default value (for non-generated columns)
	const ParsedExpression &DefaultValue() const;
	bool HasDefaultValue() const;
	void SetDefaultValue(unique_ptr<ParsedExpression> default_value);

	//! generated column expression (for generated columns)
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
	//! The name of the column
	string name;
	//! The declared type of the column, as an unbound TypeExpression (null = infer)
	unique_ptr<ParsedExpression> type_expression;
	//! The default value (non-generated) or the generated column expression (generated)
	unique_ptr<ParsedExpression> expression;
	//! The category of the column
	TableColumnType category = TableColumnType::STANDARD;
	//! Compression Type requested for this column
	duckdb::CompressionType compression_type = duckdb::CompressionType::COMPRESSION_AUTO;
	//! Comment on this column
	Value comment;
	//! Tags on this column
	InsertionOrderPreservingMap<string> tags;
};

} // namespace duckdb
