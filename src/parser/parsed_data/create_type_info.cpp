#include "duckdb/parser/parsed_data/create_type_info.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

CreateTypeInfo::CreateTypeInfo() : CreateInfo(CatalogType::TYPE_ENTRY), bind_function(nullptr) {
}
CreateTypeInfo::CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p)
    : CreateInfo(CatalogType::TYPE_ENTRY), type(std::move(type_p)), bind_function(bind_function_p) {
	SetTypeName(Identifier(std::move(name_p)));
}

unique_ptr<CreateInfo> CreateTypeInfo::Copy() const {
	auto result = make_uniq<CreateTypeInfo>();
	CopyProperties(*result);
	result->SetTypeName(GetTypeName());
	result->type = type;
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(type_expression->Copy());
	}
	if (query) {
		result->query = query->Copy();
	}
	result->bind_function = bind_function;
	return std::move(result);
}

string CreateTypeInfo::ToString() const {
	string result = GetCreatePrefix("TYPE");
	result += QualifiedNameToString();
	if (type_expression) {
		// not yet bound
		result += " AS ";
		result += type_expression->ToString();
		result += ";";
		return result;
	}
	if (type.id() == LogicalTypeId::ENUM) {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(type);
		idx_t size = EnumType::GetSize(type);

		result += " AS ENUM ( ";
		for (idx_t i = 0; i < size; i++) {
			result += SQLString(values_insert_order.GetValue(i).ToString());
			if (i != size - 1) {
				result += ", ";
			}
		}
		result += " )";
	} else if (type.id() == LogicalTypeId::INVALID) {
		// CREATE TYPE mood AS ENUM (SELECT 'happy')
		D_ASSERT(query);
		result += " AS ENUM (" + query->ToString() + ")";
	} else {
		result += " AS ";
		result += type.ToString();
	}
	result += ";";
	return result;
}

void CreateTypeInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<Identifier>(200, "name", qualified_name.Name());
	// from v2.0.0 the definition is stored as a type expression, before that as a resolved LogicalType
	const bool write_expression = serializer.ShouldSerialize(StorageVersion::V2_0_0);
	if (!write_expression) {
		serializer.WriteProperty<LogicalType>(201, "logical_type", type);
	}
	if (write_expression) {
		serializer.WritePropertyWithDefault<unique_ptr<TypeExpression>>(202, "type_expression", type_expression);
	}
}

unique_ptr<CreateInfo> CreateTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateTypeInfo>(new CreateTypeInfo());
	auto name = deserializer.ReadPropertyWithDefault<Identifier>(200, "name");
	deserializer.ReadPropertyWithExplicitDefault<LogicalType>(201, "logical_type", result->type, LogicalType::INVALID);
	auto type_expression =
	    deserializer.ReadPropertyWithExplicitDefault<unique_ptr<ParsedExpression>>(202, "type_expression", nullptr);
	if (type_expression) {
		result->type_expression = unique_ptr_cast<ParsedExpression, TypeExpression>(std::move(type_expression));
	}
	result->SetName(std::move(name));
	return std::move(result);
}

} // namespace duckdb
