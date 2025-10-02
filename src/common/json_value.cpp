#include "yyjson.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/json_value.hpp"

namespace duckdb {

static JsonValue FromYYJSONRecursive(yyjson_val *val) {
	switch (yyjson_get_type(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return JsonValue(JsonKind::JSON_NULL);
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		return JsonValue(yyjson_get_bool(val));
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		return JsonValue(yyjson_get_uint(val));
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		return JsonValue(yyjson_get_sint(val));
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		return JsonValue(yyjson_get_real(val));
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE: {
		const auto str = yyjson_get_str(val);
		const auto len = yyjson_get_len(val);
		return JsonValue(string(str, len));
	}
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE: {
		const auto str = yyjson_get_raw(val);
		const auto len = yyjson_get_len(val);
		return JsonValue(string(str, len));
	}
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
		vector<JsonValue> arr;
		size_t idx, max;
		yyjson_val *elem;
		yyjson_arr_foreach(val, idx, max, elem) {
			arr.push_back(FromYYJSONRecursive(elem));
		}
		return JsonValue(std::move(arr));
	}
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
		unordered_map<string, JsonValue> obj;
		size_t idx, max;
		yyjson_val *key, *value;
		yyjson_obj_foreach(val, idx, max, key, value) {
			const auto key_str = yyjson_get_str(key);
			const auto key_len = yyjson_get_len(key);
			obj[string(key_str, key_len)] = FromYYJSONRecursive(value);
		}
		return JsonValue(std::move(obj));
	}
	default:
		throw InvalidInputException("Failed to parse JSON value");
	}
}

JsonValue JsonValue::FromYYJSON(yyjson_doc *doc) {
	try {
		const auto root = yyjson_doc_get_root(doc);
		auto result = FromYYJSONRecursive(root);
		yyjson_doc_free(doc);
		return result;
	} catch (...) {
		yyjson_doc_free(doc);
		throw;
	}
}

static yyjson_mut_val *ToYYJSONRecursive(yyjson_mut_doc *doc, const JsonValue &value) {
	switch (value.GetKind()) {
	case JsonKind::JSON_NULL:
		return yyjson_mut_null(doc);
	case JsonKind::BOOLEAN:
		return yyjson_mut_bool(doc, value.AsBool());
	case JsonKind::NUMBER:
		return yyjson_mut_real(doc, value.AsNumber());
	case JsonKind::STRING: {
		auto &str = value.AsString();
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	}
	case JsonKind::ARRAY: {
		const auto arr = yyjson_mut_arr(doc);
		for (auto &elem : value.AsArray()) {
			const auto child = ToYYJSONRecursive(doc, elem);
			yyjson_mut_arr_add_val(arr, child);
		}
		return arr;
	}
	case JsonKind::OBJECT: {
		const auto obj = yyjson_mut_obj(doc);
		for (auto &entry : value.AsObject()) {
			const auto key = yyjson_mut_strncpy(doc, entry.first.c_str(), entry.first.size());
			const auto child = ToYYJSONRecursive(doc, entry.second);
			yyjson_mut_obj_add(obj, key, child);
		}
		return obj;
	}
	default:
		throw InvalidInputException("Invalid JSON kind");
	}
}

yyjson_mut_doc *JsonValue::ToYYJSON(const JsonValue &value) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	try {
		const auto root = ToYYJSONRecursive(doc, value);
		yyjson_mut_doc_set_root(doc, root);
		return doc;
	} catch (...) {
		yyjson_mut_doc_free(doc);
		throw;
	}
}

JsonValue JsonValue::TryParse(const char *json, bool ignore_errors) {
	return TryParse(json, strlen(json), ignore_errors);
}
JsonValue JsonValue::TryParse(const std::string &json, bool ignore_errors) {
	return TryParse(json.c_str(), json.size(), ignore_errors);
}

JsonValue JsonValue::TryParse(const char *json, idx_t size, bool ignore_errors) {
	if (size == 0) {
		return JsonValue(JsonKind::OBJECT);
	}

	constexpr auto flags = YYJSON_READ_ALLOW_INVALID_UNICODE;
	const auto doc = yyjson_read(json, size, flags);
	if (!doc) {
		if (ignore_errors) {
			return JsonValue(JsonKind::OBJECT);
		}
		throw SerializationException("Failed to parse JSON string: %s", string(json));
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || yyjson_get_type(root) != YYJSON_TYPE_OBJ) {
		yyjson_doc_free(doc);
		if (ignore_errors) {
			return JsonValue(JsonKind::OBJECT);
		}
		throw SerializationException("Failed to parse JSON string: %s", string(json));
	}

	return FromYYJSON(doc);
}

string JsonValue::ToString() const {
	const auto doc = ToYYJSON(*this);
	yyjson_write_err err;
	size_t len;
	constexpr auto flags = YYJSON_WRITE_ALLOW_INVALID_UNICODE;
	char *json = yyjson_mut_write_opts(doc, flags, nullptr, &len, &err);
	if (!json) {
		yyjson_mut_doc_free(doc);
		throw SerializationException("Failed to write JSON string: %s", err.msg);
	}
	// Create a string from the JSON
	string result(json, len);

	// Free the JSON and the document
	free(json);
	yyjson_mut_doc_free(doc);

	// Return the result
	return result;
}

} // namespace duckdb
