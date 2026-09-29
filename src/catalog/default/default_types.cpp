#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/catalog/default/default_types.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/type_constructor.hpp"

namespace duckdb {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// FLOAT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDefaultFloatType(BindLogicalTypeInput &input) {
	return LogicalType::FLOAT;
}

void RegisterFloatConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("precision", LogicalType::BIGINT);
	set.AddFunction(TypeConstructor(std::move(signature), [](BindLogicalTypeInput &input) -> LogicalType {
		auto precision = input.modifiers[0].GetValue().GetValue<int64_t>();
		if (precision < 1) {
			throw BinderException(input.GetLocation(0), "precision for type float must be at least 1 bit");
		} else if (precision <= 24) {
			return LogicalType::FLOAT;
		} else if (precision <= 53) {
			return LogicalType::DOUBLE;
		} else {
			throw BinderException(input.GetLocation(0), "precision for type float must be less than 54 bits");
		}
	}));
}

//----------------------------------------------------------------------------------------------------------------------
// DECIMAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDefaultDecimalType(BindLogicalTypeInput &input) {
	return LogicalType::DECIMAL(18, 3);
}

LogicalType BindDecimalType(BindLogicalTypeInput &input) {
	auto width = input.modifiers[0].GetValue().GetValue<uint8_t>();
	auto scale = input.modifiers[1].GetValue().GetValue<uint8_t>();

	if (width < 1 || width > Decimal::MAX_WIDTH_DECIMAL) {
		throw BinderException(input.GetLocation(0), "DECIMAL type width must be between 1 and %d",
		                      Decimal::MAX_WIDTH_DECIMAL);
	}
	if (scale > width) {
		throw BinderException(input.GetLocation(1), "DECIMAL type scale cannot be greater than width");
	}
	return LogicalType::DECIMAL(width, scale);
}

void RegisterDecimalConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("width", LogicalType::UTINYINT);
	signature.AddParameter("scale", LogicalType::UTINYINT, Value::UTINYINT(0));
	set.AddFunction(TypeConstructor(std::move(signature), BindDecimalType));
}

//----------------------------------------------------------------------------------------------------------------------
// TIMESTAMP Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDefaultTimestampType(BindLogicalTypeInput &input) {
	return LogicalType::TIMESTAMP;
}

LogicalType BindTimestampType(BindLogicalTypeInput &input) {
	auto precision = input.modifiers[0].GetValue().GetValue<uint8_t>();

	if (precision > 9) {
		throw BinderException(input.GetLocation(0), "TIMESTAMP only supports until nano-second precision (9)");
	}
	if (precision == 0) {
		return LogicalType::TIMESTAMP_S;
	}
	if (precision <= 3) {
		return LogicalType::TIMESTAMP_MS;
	}
	if (precision <= 6) {
		return LogicalType::TIMESTAMP;
	}
	return LogicalType::TIMESTAMP_NS;
}

void RegisterTimestampConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("precision", LogicalType::UTINYINT);
	set.AddFunction(TypeConstructor(std::move(signature), BindTimestampType));
}

//----------------------------------------------------------------------------------------------------------------------
// VARCHAR Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindVarcharType(BindLogicalTypeInput &input) {
	return LogicalType::VARCHAR;
}

LogicalType BindCollatedVarcharType(BindLogicalTypeInput &input) {
	auto &collation = StringValue::Get(input.modifiers[0].GetValue());

	// The collation can only be checked against the catalog when there is a connection. Without one this is a
	// type that was bound before - re-binding it, for instance to serialize it, must not lose the collation.
	if (input.context) {
		// Ensure this is a valid collation
		ExpressionBinder::TestCollation(*input.context, collation);
	}

	return LogicalType::VARCHAR_COLLATION(collation);
}

void RegisterVarcharConstructors(TypeConstructorSet &set) {
	// The length is accepted for compatibility, but VARCHAR is not length-limited so it is ignored
	auto length_signature = TypeConstructor::Signature();
	length_signature.AddParameter("length", LogicalType::BIGINT);
	set.AddFunction(TypeConstructor(std::move(length_signature), BindVarcharType));

	auto collation_signature = TypeConstructor::Signature();
	collation_signature.AddParameter("collation", LogicalType::VARCHAR);
	set.AddFunction(TypeConstructor(std::move(collation_signature), BindCollatedVarcharType));
}

//----------------------------------------------------------------------------------------------------------------------
// BIT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindBitType(BindLogicalTypeInput &input) {
	return LogicalType::BIT;
}

void RegisterBitConstructors(TypeConstructorSet &set) {
	// The length is accepted for compatibility, but BIT is not length-limited so it is ignored
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("length", LogicalType::BIGINT);
	set.AddFunction(TypeConstructor(std::move(signature), BindBitType));
}

//----------------------------------------------------------------------------------------------------------------------
// INTERVAL Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindIntervalType(BindLogicalTypeInput &input) {
	return LogicalType::INTERVAL;
}

void RegisterIntervalConstructors(TypeConstructorSet &set) {
	// The leading field is accepted for compatibility, but is ignored
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("precision", LogicalType::UTINYINT);
	set.AddFunction(TypeConstructor(std::move(signature), BindIntervalType));
}

//----------------------------------------------------------------------------------------------------------------------
// ENUM Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindEnumType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		throw BinderException(input.query_location, "ENUM type requires at least one argument");
	}

	Vector enum_vector(LogicalType::VARCHAR, NumericCast<idx_t>(arguments.size()));
	auto string_data = FlatVector::Writer<string_t>(enum_vector, arguments.size());

	for (auto &arg : arguments) {
		string_data.WriteValue(string_t(StringValue::Get(arg.GetValue())));
	}

	return LogicalType::ENUM(enum_vector, NumericCast<idx_t>(arguments.size()));
}

void RegisterEnumConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddArgs("args", LogicalType::VARCHAR);
	set.AddFunction(TypeConstructor(std::move(signature), BindEnumType));
}

//----------------------------------------------------------------------------------------------------------------------
// LIST Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindListType(BindLogicalTypeInput &input) {
	return LogicalType::LIST(TypeValue::GetType(input.modifiers[0].GetValue()));
}

void RegisterListConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("child", LogicalType::TYPE());
	set.AddFunction(TypeConstructor(std::move(signature), BindListType));
}

//----------------------------------------------------------------------------------------------------------------------
// ARRAY Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindArrayType(BindLogicalTypeInput &input) {
	auto child_type = TypeValue::GetType(input.modifiers[0].GetValue());
	auto array_size = input.modifiers[1].GetValue().GetValue<int64_t>();

	if (array_size < 1) {
		throw BinderException(input.GetLocation(1), "ARRAY type size must be at least 1");
	}
	if (array_size > static_cast<int64_t>(ArrayType::MAX_ARRAY_SIZE)) {
		throw BinderException(input.GetLocation(1), "ARRAY type size must be at most %d", ArrayType::MAX_ARRAY_SIZE);
	}

	return LogicalType::ARRAY(child_type, UnsafeNumericCast<idx_t>(array_size));
}

void RegisterArrayConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("child", LogicalType::TYPE());
	signature.AddParameter("size", LogicalType::BIGINT);
	set.AddFunction(TypeConstructor(std::move(signature), BindArrayType));
}

//----------------------------------------------------------------------------------------------------------------------
// STRUCT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindStructType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	child_list_t<LogicalType> children;
	children.reserve(arguments.size());

	for (auto &arg : arguments) {
		children.emplace_back(Identifier(arg.GetName()), TypeValue::GetType(arg.GetValue()));
	}

	return LogicalType::STRUCT(std::move(children));
}

void RegisterStructConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddKwargs("kwargs", LogicalType::TYPE());
	set.AddFunction(TypeConstructor(std::move(signature), BindStructType));
}

//----------------------------------------------------------------------------------------------------------------------
// TUPLE Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindTupleType(BindLogicalTypeInput &input) {
	vector<LogicalType> children;
	children.reserve(input.modifiers.size());
	for (auto &arg : input.modifiers) {
		children.push_back(TypeValue::GetType(arg.GetValue()));
	}
	return LogicalType::TUPLE(std::move(children));
}

void RegisterTupleConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddArgs("args", LogicalType::TYPE());
	set.AddFunction(TypeConstructor(std::move(signature), BindTupleType));
}

//----------------------------------------------------------------------------------------------------------------------
// MAP Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindMapType(BindLogicalTypeInput &input) {
	auto key_type = TypeValue::GetType(input.modifiers[0].GetValue());
	auto val_type = TypeValue::GetType(input.modifiers[1].GetValue());
	return LogicalType::MAP(std::move(key_type), std::move(val_type));
}

void RegisterMapConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("key", LogicalType::TYPE());
	signature.AddParameter("value", LogicalType::TYPE());
	set.AddFunction(TypeConstructor(std::move(signature), BindMapType));
}

//----------------------------------------------------------------------------------------------------------------------
// UNION Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindUnionType(BindLogicalTypeInput &input) {
	auto &arguments = input.modifiers;

	if (arguments.empty()) {
		throw BinderException(input.query_location, "UNION type requires at least one type modifier");
	}
	if (arguments.size() > UnionType::MAX_UNION_MEMBERS) {
		throw BinderException(input.query_location, "UNION type supports at most %d type modifiers",
		                      UnionType::MAX_UNION_MEMBERS);
	}

	child_list_t<LogicalType> children;
	for (auto &arg : arguments) {
		children.emplace_back(arg.GetName(), TypeValue::GetType(arg.GetValue()));
	}

	return LogicalType::UNION(std::move(children));
}

void RegisterUnionConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddKwargs("kwargs", LogicalType::TYPE());
	set.AddFunction(TypeConstructor(std::move(signature), BindUnionType));
}

//----------------------------------------------------------------------------------------------------------------------
// VARIANT Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindVariantType(BindLogicalTypeInput &input) {
	// We need this function to make sure we always create a VARIANT type with ExtraTypeInfo
	return LogicalType::VARIANT();
}

//----------------------------------------------------------------------------------------------------------------------
// GEOMETRY Type
//----------------------------------------------------------------------------------------------------------------------
LogicalType BindDefaultGeometryType(BindLogicalTypeInput &input) {
	return LogicalType::GEOMETRY();
}

LogicalType BindGeometryType(BindLogicalTypeInput &input) {
	// FIXME: Use extension/ClientContext to expand incomplete/shorthand CRS definitions
	auto &crs = StringValue::Get(input.modifiers[0].GetValue());

	if (!input.context) {
		throw BinderException(input.query_location,
		                      "Cannot create GEOMETRY type with coordinate system without a connection");
	}

	const auto crs_result = CoordinateReferenceSystem::TryIdentify(*input.context, crs);
	if (!crs_result) {
		if (Settings::Get<IgnoreUnknownCrsSetting>(*input.context)) {
			// Ignored by user configuration - return generic GEOMETRY type
			return LogicalType::GEOMETRY();
		}

		throw BinderException(
		    input.GetLocation(0),
		    "Encountered unrecognized coordinate system '%s' when trying to create GEOMETRY type\n"
		    "The coordinate system definition may be incomplete or invalid. Your options are as follows:\n"
		    "* Load an extension that can identify this coordinate system\n"
		    "* Provide a full coordinate system definition in e.g. \"PROJJSON\" or \"WKT2\" format\n"
		    "* Set the 'ignore_unknown_crs' configuration option to drop the coordinate system from the resulting "
		    "geometry type and make this error go away",
		    crs);
	}

	return LogicalType::GEOMETRY(crs_result->GetDefinition());
}

void RegisterGeometryConstructors(TypeConstructorSet &set) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("crs", LogicalType::VARCHAR);
	set.AddFunction(TypeConstructor(std::move(signature), BindGeometryType));
}

//----------------------------------------------------------------------------------------------------------------------
// All Types
//----------------------------------------------------------------------------------------------------------------------

using constructor_registration_t = void (*)(TypeConstructorSet &set);

struct DefaultType {
	const char *name;
	LogicalTypeId type;
	//! Registers the constructors accepting this type's modifiers, or null if it takes no modifiers
	constructor_registration_t register_constructors;
	//! Binds the type without modifiers, or null if it requires them (or takes none and binds to itself)
	bind_logical_type_function_t bind_without_modifiers;
};

using builtin_type_array = std::array<DefaultType, 83>;

const builtin_type_array BUILTIN_TYPES = {
    {{"decimal", LogicalTypeId::DECIMAL, RegisterDecimalConstructors, BindDefaultDecimalType},
     {"dec", LogicalTypeId::DECIMAL, RegisterDecimalConstructors, BindDefaultDecimalType},
     {"numeric", LogicalTypeId::DECIMAL, RegisterDecimalConstructors, BindDefaultDecimalType},
     {"time", LogicalTypeId::TIME, nullptr, nullptr},
     {"time_ns", LogicalTypeId::TIME_NS, nullptr, nullptr},
     {"date", LogicalTypeId::DATE, nullptr, nullptr},
     {"timestamp", LogicalTypeId::TIMESTAMP, RegisterTimestampConstructors, BindDefaultTimestampType},
     {"datetime", LogicalTypeId::TIMESTAMP, RegisterTimestampConstructors, BindDefaultTimestampType},
     {"timestamp_us", LogicalTypeId::TIMESTAMP, nullptr, nullptr},
     {"timestamp_ms", LogicalTypeId::TIMESTAMP_MS, nullptr, nullptr},
     {"timestamp_ns", LogicalTypeId::TIMESTAMP_NS, nullptr, nullptr},
     {"timestamp_s", LogicalTypeId::TIMESTAMP_SEC, nullptr, nullptr},
     {"timestamptz", LogicalTypeId::TIMESTAMP_TZ, nullptr, nullptr},
     {"timestamp with time zone", LogicalTypeId::TIMESTAMP_TZ, nullptr, nullptr},
     {"timestamptz_ns", LogicalTypeId::TIMESTAMP_TZ_NS, nullptr, nullptr},
     {"timetz", LogicalTypeId::TIME_TZ, nullptr, nullptr},
     {"time with time zone", LogicalTypeId::TIME_TZ, nullptr, nullptr},
     {"interval", LogicalTypeId::INTERVAL, RegisterIntervalConstructors, BindIntervalType},
     {"varchar", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"bpchar", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"string", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"char", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"nvarchar", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"text", LogicalTypeId::VARCHAR, RegisterVarcharConstructors, BindVarcharType},
     {"blob", LogicalTypeId::BLOB, nullptr, nullptr},
     {"bytea", LogicalTypeId::BLOB, nullptr, nullptr},
     {"varbinary", LogicalTypeId::BLOB, nullptr, nullptr},
     {"binary", LogicalTypeId::BLOB, nullptr, nullptr},
     {"hugeint", LogicalTypeId::HUGEINT, nullptr, nullptr},
     {"int128", LogicalTypeId::HUGEINT, nullptr, nullptr},
     {"uhugeint", LogicalTypeId::UHUGEINT, nullptr, nullptr},
     {"uint128", LogicalTypeId::UHUGEINT, nullptr, nullptr},
     {"bigint", LogicalTypeId::BIGINT, nullptr, nullptr},
     {"oid", LogicalTypeId::BIGINT, nullptr, nullptr},
     {"long", LogicalTypeId::BIGINT, nullptr, nullptr},
     {"int8", LogicalTypeId::BIGINT, nullptr, nullptr},
     {"int64", LogicalTypeId::BIGINT, nullptr, nullptr},
     {"ubigint", LogicalTypeId::UBIGINT, nullptr, nullptr},
     {"uint64", LogicalTypeId::UBIGINT, nullptr, nullptr},
     {"integer", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"int", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"int4", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"signed", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"integral", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"int32", LogicalTypeId::INTEGER, nullptr, nullptr},
     {"uinteger", LogicalTypeId::UINTEGER, nullptr, nullptr},
     {"uint32", LogicalTypeId::UINTEGER, nullptr, nullptr},
     {"smallint", LogicalTypeId::SMALLINT, nullptr, nullptr},
     {"int2", LogicalTypeId::SMALLINT, nullptr, nullptr},
     {"short", LogicalTypeId::SMALLINT, nullptr, nullptr},
     {"int16", LogicalTypeId::SMALLINT, nullptr, nullptr},
     {"usmallint", LogicalTypeId::USMALLINT, nullptr, nullptr},
     {"uint16", LogicalTypeId::USMALLINT, nullptr, nullptr},
     {"tinyint", LogicalTypeId::TINYINT, nullptr, nullptr},
     {"int1", LogicalTypeId::TINYINT, nullptr, nullptr},
     {"utinyint", LogicalTypeId::UTINYINT, nullptr, nullptr},
     {"uint8", LogicalTypeId::UTINYINT, nullptr, nullptr},
     {"struct", LogicalTypeId::STRUCT, RegisterStructConstructors, nullptr},
     {"row", LogicalTypeId::STRUCT, RegisterStructConstructors, nullptr},
     {"tuple", LogicalTypeId::TUPLE, RegisterTupleConstructors, nullptr},
     {"list", LogicalTypeId::LIST, RegisterListConstructors, nullptr},
     {"array", LogicalTypeId::ARRAY, RegisterArrayConstructors, nullptr},
     {"map", LogicalTypeId::MAP, RegisterMapConstructors, nullptr},
     {"union", LogicalTypeId::UNION, RegisterUnionConstructors, nullptr},
     {"bit", LogicalTypeId::BIT, RegisterBitConstructors, BindBitType},
     {"bitstring", LogicalTypeId::BIT, RegisterBitConstructors, BindBitType},
     {"variant", LogicalTypeId::VARIANT, nullptr, BindVariantType},
     {"bignum", LogicalTypeId::BIGNUM, nullptr, nullptr},
     {"varint", LogicalTypeId::BIGNUM, nullptr, nullptr},
     {"boolean", LogicalTypeId::BOOLEAN, nullptr, nullptr},
     {"bool", LogicalTypeId::BOOLEAN, nullptr, nullptr},
     {"logical", LogicalTypeId::BOOLEAN, nullptr, nullptr},
     {"uuid", LogicalTypeId::UUID, nullptr, nullptr},
     {"guid", LogicalTypeId::UUID, nullptr, nullptr},
     {"enum", LogicalTypeId::ENUM, RegisterEnumConstructors, nullptr},
     {"null", LogicalTypeId::SQLNULL, nullptr, nullptr},
     {"float", LogicalTypeId::FLOAT, RegisterFloatConstructors, BindDefaultFloatType},
     {"real", LogicalTypeId::FLOAT, nullptr, nullptr},
     {"float4", LogicalTypeId::FLOAT, nullptr, nullptr},
     {"double", LogicalTypeId::DOUBLE, nullptr, nullptr},
     {"float8", LogicalTypeId::DOUBLE, nullptr, nullptr},
     {"geometry", LogicalTypeId::GEOMETRY, RegisterGeometryConstructors, BindDefaultGeometryType},
     {"type", LogicalTypeId::TYPE, nullptr, nullptr}}};

TypeConstructorSet GetConstructors(const DefaultType &entry, const Identifier &name) {
	TypeConstructorSet result(name);
	if (entry.bind_without_modifiers) {
		result.AddFunction(TypeConstructor(TypeConstructor::Signature(), entry.bind_without_modifiers));
	}
	if (entry.register_constructors) {
		entry.register_constructors(result);
	} else if (!entry.bind_without_modifiers) {
		result.AddFunction(TypeConstructor::Identity(name));
	}
	return result;
}

optional_ptr<const DefaultType> TryGetDefaultTypeEntry(const Identifier &name) {
	static const case_insensitive_map_t<reference<const DefaultType>> lookup = [] {
		case_insensitive_map_t<reference<const DefaultType>> result;
		for (auto &type : BUILTIN_TYPES) {
			result.emplace(type.name, type);
		}
		return result;
	}();
	auto entry = lookup.find(name.GetIdentifierName());
	if (entry == lookup.end()) {
		return nullptr;
	}
	return &entry->second.get();
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// Default Type Generator
//----------------------------------------------------------------------------------------------------------------------
LogicalTypeId DefaultTypeGenerator::GetDefaultType(const Identifier &name) {
	auto entry = TryGetDefaultTypeEntry(name);
	return entry ? entry->type : LogicalTypeId::INVALID;
}

LogicalType DefaultTypeGenerator::TryDefaultBind(const string &name, const vector<pair<string, Value>> &params) {
	auto entry = TryGetDefaultTypeEntry(Identifier(name));
	if (!entry) {
		return LogicalTypeId::INVALID;
	}

	vector<TypeArgument> args;
	for (auto &param : params) {
		args.emplace_back(param.first, param.second);
	}

	// no context is available here, so only the built-in types are reachable
	return GetConstructors(*entry, Identifier(name)).Bind(nullptr, LogicalType(entry->type), args);
}

LogicalType DefaultTypeGenerator::TryBindWithoutParameters(const Identifier &name) {
	auto entry = TryGetDefaultTypeEntry(name);
	if (!entry) {
		return LogicalTypeId::INVALID;
	}
	LogicalType base_type(entry->type);
	if (entry->bind_without_modifiers) {
		vector<TypeArgument> modifiers;
		BindLogicalTypeInput input {nullptr, base_type, modifiers, QueryLocation()};
		return entry->bind_without_modifiers(input);
	}
	if (entry->register_constructors) {
		return LogicalTypeId::INVALID;
	}
	return base_type;
}

DefaultTypeGenerator::DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultTypeGenerator::CreateDefaultEntry(ClientContext &context,
                                                                  const Identifier &entry_name) {
	if (schema.name != DEFAULT_SCHEMA) {
		return nullptr;
	}
	auto entry = TryGetDefaultTypeEntry(entry_name);
	if (!entry || entry->type == LogicalTypeId::INVALID) {
		return nullptr;
	}
	CreateTypeInfo info;
	info.SetTypeName(entry_name);
	info.type = LogicalType(entry->type);
	info.internal = true;
	info.temporary = true;
	info.constructors = GetConstructors(*entry, entry_name);
	return make_uniq_base<CatalogEntry, TypeCatalogEntry>(catalog, schema, info);
}

vector<Identifier> DefaultTypeGenerator::GetDefaultEntries() {
	vector<Identifier> result;
	if (schema.name != DEFAULT_SCHEMA) {
		return result;
	}
	auto &internal_types = BUILTIN_TYPES;
	for (auto &type : internal_types) {
		result.emplace_back(StringUtil::Lower(type.name));
	}
	return result;
}

} // namespace duckdb
