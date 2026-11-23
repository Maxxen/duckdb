#include "duckdb/function/type_cache.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//! Whether two arguments of a type constructor are the same: a type compares as a type with its collations, not as
//! a value
static bool IsSameArgument(const TypeArgument &left, const TypeArgument &right) {
	auto &left_value = left.GetValue();
	auto &right_value = right.GetValue();
	if (left.GetName() != right.GetName() || left_value.type() != right_value.type() ||
	    left_value.IsNull() != right_value.IsNull()) {
		return false;
	}
	if (left_value.IsNull()) {
		return true;
	}
	if (left_value.type().id() == LogicalTypeId::TYPE) {
		return TypeValue::GetType(left_value).EqualsIncludingCollation(TypeValue::GetType(right_value));
	}
	return Value::NotDistinctFrom(left_value, right_value);
}

//! Hashes are combined in order, so that a type does not hash like the types it is made of
static hash_t CombineOrdered(hash_t hash, hash_t next) {
	return Hash<uint64_t>(hash ^ next);
}

static hash_t HashArgument(const TypeArgument &argument) {
	auto &name = argument.GetName();
	auto &value = argument.GetValue();
	auto hash = Hash(name.c_str(), name.size());
	if (value.IsNull()) {
		return hash;
	}
	if (value.type().id() == LogicalTypeId::TYPE) {
		return CombineOrdered(hash, TypeValue::GetType(value).Hash());
	}
	if (value.type().IsIntegral() && value.type().id() != LogicalTypeId::HUGEINT &&
	    value.type().id() != LogicalTypeId::UHUGEINT && value.type().id() != LogicalTypeId::UBIGINT) {
		return CombineOrdered(hash, Hash<int64_t>(value.GetValue<int64_t>()));
	}
	return CombineOrdered(hash, Hash<uint8_t>(static_cast<uint8_t>(value.type().id())));
}

LogicalType TypeCache::Construct(const Identifier &name, vector<TypeArgument> arguments) {
	hash_t hash = name.Hash();
	for (auto &argument : arguments) {
		hash = CombineOrdered(hash, HashArgument(argument));
	}
	auto constructed = entries.find(hash);
	if (constructed != entries.end()) {
		for (auto &entry : constructed->second) {
			if (!(entry.name == name) || entry.arguments.size() != arguments.size()) {
				continue;
			}
			bool same_arguments = true;
			for (idx_t i = 0; i < arguments.size() && same_arguments; i++) {
				same_arguments = IsSameArgument(entry.arguments[i], arguments[i]);
			}
			if (same_arguments) {
				return entry.type;
			}
		}
	}

	LogicalType type = LogicalType::INVALID;
	if (context) {
		auto entry = Catalog::GetEntry<TypeCatalogEntry>(*context, QualifiedName(name), OnEntryNotFound::RETURN_NULL);
		if (entry) {
			type = entry->constructors.Bind(context, entry->user_type, arguments);
		}
	} else {
		// without a context only the built-in types are found
		vector<pair<string, Value>> parameters;
		for (auto &argument : arguments) {
			parameters.emplace_back(argument.GetName(), argument.GetValue());
		}
		type = DefaultTypeGenerator::TryDefaultBind(name.GetIdentifierName(), parameters);
	}
	if (type.id() != LogicalTypeId::INVALID) {
		entries[hash].push_back(Entry {name, std::move(arguments), type});
	}
	return type;
}

} // namespace duckdb
