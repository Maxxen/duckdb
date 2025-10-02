#include "duckdb/common/complex_json.hpp"

#include "yyjson.hpp"

namespace duckdb {
ComplexJSON::ComplexJSON(const string &str) : str_value(str), type(ComplexJSONType::VALUE) {
}

ComplexJSON::ComplexJSON() : type(ComplexJSONType::VALUE) {
}

void ComplexJSON::AddObjectEntry(const string &key, unique_ptr<ComplexJSON> object) {
	type = ComplexJSONType::OBJECT;
	obj_value[key] = std::move(object);
}

void ComplexJSON::AddArrayElement(unique_ptr<ComplexJSON> object) {
	type = ComplexJSONType::ARRAY;
	arr_value.push_back(std::move(object));
}

ComplexJSON &ComplexJSON::GetObject(const string &key) {
	if (type == ComplexJSONType::OBJECT) {
		if (obj_value.find(key) == obj_value.end()) {
			throw InvalidInputException("Complex JSON Key not found");
		}
		return *obj_value[key];
	}
	throw InvalidInputException("ComplexJson is not an object");
}

ComplexJSON &ComplexJSON::GetArrayElement(const idx_t &index) {
	if (type == ComplexJSONType::ARRAY) {
		if (index >= arr_value.size()) {
			throw InvalidInputException("Complex JSON array element out of bounds");
		}
		return *arr_value[index];
	}
	throw InvalidInputException("ComplexJson is not an array");
}

unordered_map<string, string> ComplexJSON::Flatten() const {
	unordered_map<string, string> result;
	for (auto &obj : obj_value) {
		result[obj.first] = obj.second->GetValueRecursive(*obj.second);
	}
	return result;
}


JsonValue JsonValue::TryParse(const char *json) {
	return TryParse(json, strlen(json));
}
JsonValue JsonValue::TryParse(const std::string &json) {
	return TryParse(json.c_str(), json.size());
}

static JsonValue ConstructRecursive(duckdb_yyjson::yyjson_val *val) {
	switch (duckdb_yyjson::yyjson_get_type(val)) {
		case YYJSON_TYPE_NULL:
			return JsonValue(JsonKind::JSONNULL);
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
			return JsonValue(duckdb_yyjson::yyjson_get_bool(val));
		case YYJSON_TYPE_NUM:
			return JsonValue(duckdb_yyjson::yyjson_get_real(val));
		case YYJSON_TYPE_STR: {
			auto str = duckdb_yyjson::yyjson_get_str(val);
			auto len = duckdb_yyjson::yyjson_get_len(val);
			return JsonValue(string(str, len));
		}
		case YYJSON_TYPE_ARR: {
			vector<JsonValue> arr;;
			size_t idx, max;
			duckdb_yyjson::yyjson_val *elem;
			duckdb_yyjson::yyjson_arr_foreach(val, idx, max, elem) {
				arr->push_back(ConstructRecursive(elem));
			}
			return JsonValue(std::move(arr));
		}
		case YYJSON_TYPE_OBJ: {
			unordered_map<string, JsonValue> obj;
			size_t idx, max;
			duckdb_yyjson::yyjson_val *key, *value;
			duckdb_yyjson::yyjson_obj_foreach(val, idx, max, key, value) {
				auto key_str = duckdb_yyjson::yyjson_get_str(key);
				auto key_len = duckdb_yyjson::yyjson_get_len(key);
				obj[string(key_str, key_len)] = ConstructRecursive(value);
			}
			return JsonValue(std::move(obj));
		}
		default:
			throw InvalidInputException("Failed to parse JSON value");
	}
}

JsonValue JsonValue::TryParse(const char *json, idx_t size) {
	auto doc = duckdb_yyjson::yyjson_read(json, size, 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse JSON");
	}
	try {
		const auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
		auto result = ConstructRecursive(root);
		duckdb_yyjson::yyjson_doc_free(doc);
		return result;
	} catch (...) {
		duckdb_yyjson::yyjson_doc_free(doc);
		throw;
	}
}



} // namespace duckdb
