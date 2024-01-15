#include "duckdb/common/exception.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct SizeModifiers {
	int64_t width;
	int64_t scale;
};

static idx_t GetSizeModifiers(duckdb_libpgquery::PGTypeName &type_name, const LogicalType &base_type,
                              SizeModifiers &modifiers) {
	idx_t modifier_idx = 0;
	if (type_name.typmods) {
		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &const_val = *Transformer::PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
			if (const_val.type != duckdb_libpgquery::T_PGAConst ||
			    const_val.val.type != duckdb_libpgquery::T_PGInteger) {
				throw ParserException("Expected an integer constant as type modifier");
			}
			if (const_val.val.val.ival < 0) {
				throw ParserException("Negative modifier not supported");
			}
			if (modifier_idx == 0) {
				modifiers.width = const_val.val.val.ival;
				if (base_type == LogicalTypeId::BIT && const_val.location != -1) {
					modifiers.width = 0;
				}
			} else if (modifier_idx == 1) {
				modifiers.scale = const_val.val.val.ival;
			} else {
				throw ParserException("A maximum of two modifiers is supported");
			}
			modifier_idx++;
		}
	}
	return modifier_idx;
}

LogicalType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName &type_name) {
	if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
		throw ParserException("Expected a type");
	}
	auto stack_checker = StackCheck();

	if (type_name.names->length > 1) {
		// qualified typename
		vector<string> names;
		for (auto cell = type_name.names->head; cell; cell = cell->next) {
			names.push_back(PGPointerCast<duckdb_libpgquery::PGValue>(cell->data.ptr_value)->val.str);
		}
		switch (type_name.names->length) {
		case 2:
			return LogicalType::USER(INVALID_CATALOG, std::move(names[0]), std::move(names[1]));
		case 3:
			return LogicalType::USER(std::move(names[0]), std::move(names[1]), std::move(names[2]));
		default:
			throw ParserException(
			    "Too many qualifications for type name - expected [catalog.schema.name] or [schema.name]");
		}
	}
	auto name = PGPointerCast<duckdb_libpgquery::PGValue>(type_name.names->tail->data.ptr_value)->val.str;
	// transform it to the SQL type
	LogicalTypeId base_type = TransformStringToLogicalTypeId(name);

	LogicalType result_type;
	if (base_type == LogicalTypeId::LIST) {
		throw ParserException("LIST is not valid as a stand-alone type");
	} else if (base_type == LogicalTypeId::ENUM) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Enum needs a set of entries");
		}
		Vector enum_vector(LogicalType::VARCHAR, type_name.typmods->length);
		auto string_data = FlatVector::GetData<string_t>(enum_vector);
		idx_t pos = 0;
		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto constant_value = PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
			if (constant_value->type != duckdb_libpgquery::T_PGAConst ||
			    constant_value->val.type != duckdb_libpgquery::T_PGString) {
				throw ParserException("Enum type requires a set of strings as type modifiers");
			}
			string_data[pos++] = StringVector::AddString(enum_vector, constant_value->val.val.str);
		}
		return LogicalType::ENUM(enum_vector, type_name.typmods->length);
	} else if (base_type == LogicalTypeId::STRUCT) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Struct needs a name and entries");
		}
		child_list_t<LogicalType> children;
		case_insensitive_set_t name_collision_set;

		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &type_val = *PGPointerCast<duckdb_libpgquery::PGList>(node->data.ptr_value);
			if (type_val.length != 2) {
				throw ParserException("Struct entry needs an entry name and a type name");
			}

			auto entry_name_node = PGPointerCast<duckdb_libpgquery::PGValue>(type_val.head->data.ptr_value);
			D_ASSERT(entry_name_node->type == duckdb_libpgquery::T_PGString);
			auto entry_type_node = PGPointerCast<duckdb_libpgquery::PGTypeName>(type_val.tail->data.ptr_value);
			D_ASSERT(entry_type_node->type == duckdb_libpgquery::T_PGTypeName);

			auto entry_name = string(entry_name_node->val.str);
			D_ASSERT(!entry_name.empty());

			if (name_collision_set.find(entry_name) != name_collision_set.end()) {
				throw ParserException("Duplicate struct entry name \"%s\"", entry_name);
			}
			name_collision_set.insert(entry_name);
			auto entry_type = TransformTypeName(*entry_type_node);

			children.push_back(make_pair(entry_name, entry_type));
		}
		D_ASSERT(!children.empty());
		result_type = LogicalType::STRUCT(children);

	} else if (base_type == LogicalTypeId::MAP) {
		if (!type_name.typmods || type_name.typmods->length != 2) {
			throw ParserException("Map type needs exactly two entries, key and value type");
		}
		auto key_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->head->data.ptr_value));
		auto value_type =
		    TransformTypeName(*PGPointerCast<duckdb_libpgquery::PGTypeName>(type_name.typmods->tail->data.ptr_value));

		result_type = LogicalType::MAP(std::move(key_type), std::move(value_type));
	} else if (base_type == LogicalTypeId::UNION) {
		if (!type_name.typmods || type_name.typmods->length == 0) {
			throw ParserException("Union type needs at least one member");
		}
		if (type_name.typmods->length > (int)UnionType::MAX_UNION_MEMBERS) {
			throw ParserException("Union types can have at most %d members", UnionType::MAX_UNION_MEMBERS);
		}

		child_list_t<LogicalType> children;
		case_insensitive_set_t name_collision_set;

		for (auto node = type_name.typmods->head; node; node = node->next) {
			auto &type_val = *PGPointerCast<duckdb_libpgquery::PGList>(node->data.ptr_value);
			if (type_val.length != 2) {
				throw ParserException("Union type member needs a tag name and a type name");
			}

			auto entry_name_node = PGPointerCast<duckdb_libpgquery::PGValue>(type_val.head->data.ptr_value);
			D_ASSERT(entry_name_node->type == duckdb_libpgquery::T_PGString);
			auto entry_type_node = PGPointerCast<duckdb_libpgquery::PGTypeName>(type_val.tail->data.ptr_value);
			D_ASSERT(entry_type_node->type == duckdb_libpgquery::T_PGTypeName);

			auto entry_name = string(entry_name_node->val.str);
			D_ASSERT(!entry_name.empty());

			if (name_collision_set.find(entry_name) != name_collision_set.end()) {
				throw ParserException("Duplicate union type tag name \"%s\"", entry_name);
			}

			name_collision_set.insert(entry_name);

			auto entry_type = TransformTypeName(*entry_type_node);
			children.push_back(make_pair(entry_name, entry_type));
		}
		D_ASSERT(!children.empty());
		result_type = LogicalType::UNION(std::move(children));
	} else {
		SizeModifiers modifiers;
		if (base_type == LogicalTypeId::DECIMAL) {
			// default decimal width/scale
			modifiers.width = 18;
			modifiers.scale = 3;
		} else {
			modifiers.width = 0;
			modifiers.scale = 0;
		}
		switch (base_type) {
		case LogicalTypeId::VARCHAR: {
			if (GetSizeModifiers(type_name, base_type, modifiers) > 1) {
				throw ParserException("VARCHAR only supports a single modifier");
			}
			// FIXME: create CHECK constraint based on varchar width
			modifiers.width = 0;
			result_type = LogicalType::VARCHAR;
		} break;
		case LogicalTypeId::DECIMAL: {
			auto modifier_count = GetSizeModifiers(type_name, base_type, modifiers);
			if (modifier_count > 2) {
				throw ParserException("DECIMAL only supports a maximum of two modifiers");
			}
			if (modifier_count == 1) {
				// only width is provided: set scale to 0
				modifiers.scale = 0;
			}
			if (modifiers.width <= 0 || modifiers.width > Decimal::MAX_WIDTH_DECIMAL) {
				throw ParserException("Width must be between 1 and %d!", (int)Decimal::MAX_WIDTH_DECIMAL);
			}
			if (modifiers.scale > modifiers.width) {
				throw ParserException("Scale cannot be bigger than width");
			}
			result_type = LogicalType::DECIMAL(modifiers.width, modifiers.scale);
		} break;
		case LogicalTypeId::INTERVAL:
			if (GetSizeModifiers(type_name, base_type, modifiers) > 1) {
				throw ParserException("INTERVAL only supports a single modifier");
			}
			modifiers.width = 0;
			result_type = LogicalType::INTERVAL;
			break;
		case LogicalTypeId::USER: {
			string user_type_name {name};
			vector<Value> type_mods;

			if (type_name.typmods) {
				for (auto node = type_name.typmods->head; node; node = node->next) {
					if (type_mods.size() > 9) {
						throw ParserException("'%s': a maximum of 9 type modifiers is allowed", user_type_name);
					}
					auto &const_val = *PGPointerCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
					if (const_val.type != duckdb_libpgquery::T_PGAConst) {
						throw ParserException("Expected a constant as type modifier");
					}
					auto const_expr = TransformValue(const_val.val);
					type_mods.push_back(std::move(const_expr->value));
				}
			}

			result_type = LogicalType::USER(user_type_name, type_mods);
			break;
		}
		case LogicalTypeId::BIT: {
			GetSizeModifiers(type_name, base_type, modifiers);
			if (!modifiers.width && type_name.typmods) {
				throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
			}
			result_type = LogicalType(base_type);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			auto modifier_count = GetSizeModifiers(type_name, base_type, modifiers);
			if (modifier_count == 0) {
				result_type = LogicalType::TIMESTAMP;
			} else {
				if (modifier_count > 1) {
					throw ParserException("TIMESTAMP only supports a single modifier");
				}
				if (modifiers.width > 10) {
					throw ParserException("TIMESTAMP only supports until nano-second precision (9)");
				}
				if (modifiers.width == 0) {
					result_type = LogicalType::TIMESTAMP_S;
				} else if (modifiers.width <= 3) {
					result_type = LogicalType::TIMESTAMP_MS;
				} else if (modifiers.width <= 6) {
					result_type = LogicalType::TIMESTAMP;
				} else {
					result_type = LogicalType::TIMESTAMP_NS;
				}
			}
		} break;
		default:
			if (GetSizeModifiers(type_name, base_type, modifiers) > 0) {
				throw ParserException("Type %s does not support any modifiers!", LogicalType(base_type).ToString());
			}
			result_type = LogicalType(base_type);
			break;
		}
	}
	if (type_name.arrayBounds) {
		// array bounds: turn the type into a list
		idx_t extra_stack = 0;
		for (auto cell = type_name.arrayBounds->head; cell != nullptr; cell = cell->next) {
			StackCheck(extra_stack++);
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(cell->data.ptr_value);
			if (val->type != duckdb_libpgquery::T_PGInteger) {
				throw ParserException("Expected integer value as array bound");
			}
			auto array_size = val->val.ival;
			if (array_size < 0) {
				// -1 if bounds are empty
				result_type = LogicalType::LIST(result_type);
			} else if (array_size == 0) {
				// Empty arrays are not supported
				throw ParserException("Arrays must have a size of at least 1");
			} else if (array_size > static_cast<int64_t>(ArrayType::MAX_ARRAY_SIZE)) {
				throw ParserException("Arrays must have a size of at most %d", ArrayType::MAX_ARRAY_SIZE);
			} else {
				result_type = LogicalType::ARRAY(result_type, array_size);
			}
		}
	}
	return result_type;
}

} // namespace duckdb
