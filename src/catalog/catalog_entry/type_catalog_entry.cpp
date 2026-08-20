#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

constexpr const char *TypeCatalogEntry::Name;

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.GetTypeName()), nominal(info.nominal),
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
		return LogicalType::INVALID;
	}
	LogicalType resolved;
	if (type_expression->GetTypeName() == name) {
		// the definition names this very entry - a built-in, whose real definition is the compiled-in
		// table. Resolving it through the catalog would only come back here.
		resolved = UnboundType::TryDefaultBind(*type_expression);
	} else {
		auto binder = Binder::CreateBinder(context);
		resolved = binder->BindLogicalType(*type_expression);
	}
	if (nominal) {
		// the description says what the type is made of; its identity is this entry's name
		return resolved.WithAlias(name.GetIdentifierName());
	}
	return resolved;
}

LogicalTypeId TypeCatalogEntry::GetTypeId() const {
	if (!type_expression) {
		return LogicalTypeId::INVALID;
	}
	return DefaultTypeGenerator::GetDefaultType(type_expression->GetTypeName());
}

unique_ptr<CreateInfo> TypeCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTypeInfo>();
	result->SetQualifiedName(schema.GetQualifiedName(name));
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(type_expression->Copy());
	}
	result->extension_name = extension_name;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->bind_function = bind_function;
	result->nominal = nominal;
	return std::move(result);
}

string TypeCatalogEntry::ToSQL() const {
	duckdb::stringstream ss;
	ss << "CREATE TYPE ";
	ss << SQLIdentifier(name);
	ss << " AS ";

	D_ASSERT(type_expression);
	ss << type_expression->ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
