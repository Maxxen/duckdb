#include "duckdb/parser/parsed_column_definition.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

ParsedColumnDefinition::ParsedColumnDefinition(Identifier name_p, unique_ptr<ParsedExpression> type_expression_p)
    : name(std::move(name_p)), type_expression(std::move(type_expression_p)) {
}

ParsedColumnDefinition::ParsedColumnDefinition(Identifier name_p, unique_ptr<ParsedExpression> type_expression_p,
                                               unique_ptr<ParsedExpression> expression_p, TableColumnType category)
    : name(std::move(name_p)), type_expression(std::move(type_expression_p)), expression(std::move(expression_p)),
      category(category) {
}

unique_ptr<ParsedExpression> ParsedColumnDefinition::ResolvedTypeExpression(const LogicalType &type) {
	return make_uniq_base<ParsedExpression, ConstantExpression>(Value::TYPE(type));
}

ColumnDefinition ParsedColumnDefinition::ToResolvedColumn() const {
	LogicalType resolved = LogicalType::ANY;
	if (type_expression) {
		if (type_expression->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw SerializationException("Column \"%s\" still has an unresolved type expression", name);
		}
		auto &constant = type_expression->Cast<ConstantExpression>();
		if (constant.GetValue().type().id() != LogicalTypeId::TYPE) {
			throw SerializationException("Column \"%s\" still has an unresolved type expression", name);
		}
		resolved = TypeValue::GetType(constant.GetValue());
	}
	ColumnDefinition result(name, resolved, expression ? expression->Copy() : nullptr, category);
	result.SetCompressionType(compression_type);
	result.SetComment(comment);
	result.SetTags(tags);
	return result;
}

ParsedColumnDefinition ParsedColumnDefinition::FromColumn(const ColumnDefinition &column) {
	unique_ptr<ParsedExpression> type_expr;
	if (column.Type().id() != LogicalTypeId::ANY) {
		type_expr = ResolvedTypeExpression(column.Type());
	}
	auto expr = column.Generated()             ? column.GeneratedExpression().Copy()
	            : column.HasDefaultValue()     ? column.DefaultValue().Copy()
	                                           : nullptr;
	ParsedColumnDefinition result(column.Name(), std::move(type_expr), std::move(expr), column.Category());
	result.SetCompressionType(column.CompressionType());
	result.SetComment(column.Comment());
	result.SetTags(column.Tags());
	return result;
}

ParsedColumnDefinition ParsedColumnDefinition::Copy() const {
	ParsedColumnDefinition copy(name, type_expression ? type_expression->Copy() : nullptr);
	copy.expression = expression ? expression->Copy() : nullptr;
	copy.category = category;
	copy.compression_type = compression_type;
	copy.comment = comment;
	copy.tags = tags;
	return copy;
}

const Identifier &ParsedColumnDefinition::Name() const {
	return name;
}

void ParsedColumnDefinition::SetName(const Identifier &name_p) {
	this->name = name_p;
}

bool ParsedColumnDefinition::HasType() const {
	return type_expression != nullptr;
}

const ParsedExpression &ParsedColumnDefinition::Type() const {
	if (!type_expression) {
		throw InternalException("Type() called on a column without a declared type");
	}
	return *type_expression;
}

const unique_ptr<ParsedExpression> &ParsedColumnDefinition::GetTypeExpression() const {
	return type_expression;
}

void ParsedColumnDefinition::SetTypeExpression(unique_ptr<ParsedExpression> type_expression_p) {
	this->type_expression = std::move(type_expression_p);
}

const ParsedExpression &ParsedColumnDefinition::DefaultValue() const {
	if (!HasDefaultValue()) {
		if (Generated()) {
			throw InternalException("Calling DefaultValue() on a generated column");
		}
		throw InternalException("DefaultValue() called on a column without a default value");
	}
	return *expression;
}

bool ParsedColumnDefinition::HasDefaultValue() const {
	if (Generated()) {
		return false;
	}
	return expression != nullptr;
}

void ParsedColumnDefinition::SetDefaultValue(unique_ptr<ParsedExpression> default_value) {
	if (Generated()) {
		throw InternalException("Calling SetDefaultValue() on a generated column");
	}
	this->expression = std::move(default_value);
}

const ParsedExpression &ParsedColumnDefinition::GeneratedExpression() const {
	D_ASSERT(Generated());
	return *expression;
}

const TableColumnType &ParsedColumnDefinition::Category() const {
	return category;
}

bool ParsedColumnDefinition::Generated() const {
	return category == TableColumnType::GENERATED;
}

const Value &ParsedColumnDefinition::Comment() const {
	return comment;
}

void ParsedColumnDefinition::SetComment(const Value &comment_p) {
	this->comment = comment_p;
}

const InsertionOrderPreservingMap<string> &ParsedColumnDefinition::Tags() const {
	return tags;
}

void ParsedColumnDefinition::SetTags(InsertionOrderPreservingMap<string> new_tags) {
	this->tags = std::move(new_tags);
}

const duckdb::CompressionType &ParsedColumnDefinition::CompressionType() const {
	return compression_type;
}

void ParsedColumnDefinition::SetCompressionType(duckdb::CompressionType compression_type_p) {
	this->compression_type = compression_type_p;
}

string ParsedColumnDefinition::ToSQLString() const {
	string result = SQLIdentifier(Name()) + "";
	if (type_expression) {
		result += " " + type_expression->ToString();
	}
	if (Generated()) {
		result += " GENERATED ALWAYS AS(" + GeneratedExpression().ToString() + ")";
	} else if (HasDefaultValue()) {
		result += " DEFAULT(" + DefaultValue().ToString() + ")";
	}
	if (CompressionType() != CompressionType::COMPRESSION_AUTO) {
		result += " USING COMPRESSION " + CompressionTypeToString(CompressionType());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Generated Columns (VIRTUAL)
//===--------------------------------------------------------------------===//

static void InnerGetListOfDependencies(const ParsedExpression &expr, vector<string> &dependencies) {
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &columnref = expr.Cast<ColumnRefExpression>();
		dependencies.emplace_back(columnref.GetColumnName());
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		if (expr.GetExpressionType() == ExpressionType::LAMBDA) {
			throw NotImplementedException("Lambda functions are currently not supported in generated columns.");
		}
		InnerGetListOfDependencies(child, dependencies);
	});
}

void ParsedColumnDefinition::GetListOfDependencies(vector<string> &dependencies) const {
	D_ASSERT(Generated());
	InnerGetListOfDependencies(*expression, dependencies);
}

} // namespace duckdb
