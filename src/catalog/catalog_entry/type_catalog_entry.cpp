#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

constexpr const char *TypeCatalogEntry::Name;

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.GetTypeName()), user_type(info.type),
      bind_function(info.bind_function) {
	if (info.type_expression) {
		type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(info.type_expression->Copy());
	}
	this->temporary = info.temporary;
	this->internal = info.internal;
	this->extension_name = info.extension_name;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
}

unique_ptr<CatalogEntry> TypeCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateTypeInfo>();
	auto result = make_uniq<TypeCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

LogicalType TypeCatalogEntry::GetType(ClientContext &context) const {
	if (!type_expression) {
		return user_type;
	}
	auto binder = Binder::CreateBinder(context);
	return binder->BindLogicalType(*type_expression);
}

unique_ptr<CreateInfo> TypeCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTypeInfo>();
	result->SetQualifiedName(schema.GetQualifiedName(name));
	result->type = user_type;
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(type_expression->Copy());
	}
	result->extension_name = extension_name;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->bind_function = bind_function;
	return std::move(result);
}

string TypeCatalogEntry::ToSQL() const {
	duckdb::stringstream ss;
	ss << "CREATE TYPE ";
	ss << SQLIdentifier(name);
	ss << " AS ";

	if (type_expression) {
		ss << type_expression->ToString();
		ss << ";";
		return ss.str();
	}

	// Strip off the potential alias so ToString doesn't just output the alias
	auto user_type_copy = user_type.WithAlias("");
	D_ASSERT(user_type_copy.GetAlias().empty());

	ss << user_type_copy.ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
