#include "duckdb/parser/parsed_data/create_type_info.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// The type definition
//===--------------------------------------------------------------------===//
//
// A user-created type constructs a type much as a macro constructs an expression, so its definition is
// kept as written - a TypeExpression - rather than folded. A parameterised type would reference its
// parameters, which a folded form cannot express.
//
// Nothing here carries a resolved LogicalType. It could not anyway: a checkpoint is read with no
// ClientContext, so the definition is resolved lazily, when the type is used.
//
//   type_expression  the definition. Set by the parser, by the LogicalType constructor (which describes
//                    what it is handed), by PhysicalCreateType for an ENUM built from a query, or by
//                    deserialization.
//   nominal          whether the type keeps its own identity.
//
// A nominal type is described *structurally* - by what it is made of, never by its own name, which
// would only lead back to the entry being described. json is stored as VARCHAR, and TypeCatalogEntry
// re-applies "json" when it resolves. An alias type (CREATE TYPE) is not nominal: it resolves to
// exactly what it describes, which is why CREATE TYPE b AS INTEGER gives a plain INTEGER column.
//
// The constructor that takes a LogicalType infers `nominal` from whether that type carries an alias,
// so callers registering an extension type get the right behaviour without knowing the flag exists.

CreateTypeInfo::CreateTypeInfo() : CreateInfo(CatalogType::TYPE_ENTRY), bind_function(nullptr) {
}
CreateTypeInfo::CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p)
    : CreateInfo(CatalogType::TYPE_ENTRY),
      // a type handed in carrying an alias is a nominal one. The definition says what it is made of, with
      // the alias stripped - naming itself would only lead back to this entry - and the entry re-applies
      // the name when it resolves.
      type_expression(TypeExpression::FromLogicalType(type_p.WithAlias(""))), nominal(!type_p.GetAlias().empty()),
      bind_function(bind_function_p) {
	SetTypeName(Identifier(std::move(name_p)));
}

unique_ptr<CreateInfo> CreateTypeInfo::Copy() const {
	auto result = make_uniq<CreateTypeInfo>();
	CopyProperties(*result);
	result->SetTypeName(GetTypeName());
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(type_expression->Copy());
	}
	if (query) {
		result->query = query->Copy();
	}
	result->bind_function = bind_function;
	result->nominal = nominal;
	return std::move(result);
}

string CreateTypeInfo::ToString() const {
	string result = GetCreatePrefix("TYPE");
	result += QualifiedNameToString();
	if (type_expression) {
		result += " AS ";
		result += type_expression->ToString();
		result += ";";
		return result;
	}
	// CREATE TYPE mood AS ENUM (SELECT 'happy') - nothing is known until the query runs
	D_ASSERT(query);
	result += " AS ENUM (" + query->ToString() + ")";
	result += ";";
	return result;
}

void CreateTypeInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<Identifier>(200, "name", qualified_name.Name());
	// from v2.0.0 the definition is stored as a descriptor, before that as a resolved LogicalType. An entry
	// that has no descriptor, a built-in, or one read back from a pre-v2.0.0 database, keeps being written
	// as a LogicalType, otherwise its definition would be dropped on the way out.
	const bool write_expression = serializer.ShouldSerialize(StorageVersion::V2_0_0);
	if (!write_expression) {
		// older versions store the definition as a resolved type - fold it to write it
		serializer.WriteProperty<LogicalType>(201, "logical_type",
		                                      type_expression ? UnboundType::TryDefaultBind(*type_expression)
		                                                      : LogicalType(LogicalTypeId::INVALID));
	}
	if (write_expression) {
		serializer.WritePropertyWithDefault<unique_ptr<TypeExpression>>(202, "type_expression", type_expression);
	}
}

unique_ptr<CreateInfo> CreateTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateTypeInfo>(new CreateTypeInfo());
	auto name = deserializer.ReadPropertyWithDefault<Identifier>(200, "name");
	LogicalType logical_type(LogicalTypeId::INVALID);
	deserializer.ReadPropertyWithExplicitDefault<LogicalType>(201, "logical_type", logical_type,
	                                                          LogicalType(LogicalTypeId::INVALID));
	auto type_expression =
	    deserializer.ReadPropertyWithExplicitDefault<unique_ptr<ParsedExpression>>(202, "type_expression", nullptr);
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(std::move(type_expression));
	} else if (logical_type.id() != LogicalTypeId::INVALID) {
		// written by a version that stored the definition as a resolved type
		result->type_expression = TypeExpression::FromLogicalType(logical_type);
	}
	result->SetName(std::move(name));
	return std::move(result);
}

} // namespace duckdb
