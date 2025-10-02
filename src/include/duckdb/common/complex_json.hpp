//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/complex_json.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <string>
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

enum class JsonKind : uint8_t {
	JSONNULL = 0,
	BOOLEAN,
	NUMBER,
	STRING,
	ARRAY,
	OBJECT
};

class JsonValue {
public:
	// NOLINTBEGIN (allow implicit conversions)
	JsonValue();
	JsonValue(JsonKind kind);
	JsonValue(const string &value);
	JsonValue(const char* value);
	JsonValue(double value);
	JsonValue(unordered_map<string, JsonValue> &&object);
	JsonValue(vector<JsonValue> &&array);
	// Avoid ambiguous calls with implicit conversion from bool
	template <typename T, typename = typename std::enable_if<std::is_same<T, bool>::value>::type>
	JsonValue(T value);
	// NOLINTEND (allow implicit conversions)

	JsonValue(const JsonValue &other) = delete;
	JsonValue &operator=(const JsonValue &other) = delete;

	JsonValue(JsonValue &&other) noexcept;
	JsonValue &operator=(JsonValue &&other) noexcept;

	~JsonValue();

	static JsonValue TryParse(const char* json);
	static JsonValue TryParse(const std::string& json);
	static JsonValue TryParse(const char* json, idx_t size);

	JsonKind GetKind() const &;

	// "Is" Methods
	bool IsNull() const;
	bool IsBool() const;
	bool IsNumber() const;
	bool IsString() const;
	bool IsArray() const;
	bool IsObject() const;

	template<class T>
	bool Is() const & = delete;

	// "As" Methods
	const bool& AsBool() const;
	const string& AsString() const;
	const double& AsNumber() const;

	template<class T>
	const T& As() const = delete;

	// Array and Object Methods
	idx_t Count() const;

	template<class T>
	void Push(const T &value);

	template<class T>
	void Push(T &&value);

	template<class T>
	void Push(const string& key, const T& value);

	template<class T>
	void Push(const string& key, T&& value);

	// Throws if the index/key does not exist
	const JsonValue &Get(idx_t index) const;
	const JsonValue &Get(const string &key) const;
	JsonValue &Get(idx_t index);
	JsonValue &Get(const string &key);

	// Creates a new "null" entry if the key/index does not exist
	JsonValue& operator[](const string &key);
	JsonValue& operator[](idx_t index);

	// Throws if the key/index does not exist
	const JsonValue& operator[](const string &key) const;
	const JsonValue& operator[](idx_t index) const;

private:

	JsonKind kind;

	using JsonString = string;
	using JsonArray = unique_ptr<vector<JsonValue>>;
	using JsonObject = unique_ptr<unordered_map<string, JsonValue>>;

	union {
		bool		bit;
		double		num;
		JsonString	str;
		JsonArray	arr;
		JsonObject	obj;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

inline JsonValue::JsonValue(JsonKind kind) : kind(kind) {
	switch (kind) {
		case JsonKind::ARRAY:
			arr = make_uniq<vector<JsonValue>>();
			break;
		case JsonKind::OBJECT:
			obj = make_uniq<unordered_map<string, JsonValue>>();
			break;
		default:
			break;
	}
}

inline JsonValue::JsonValue() : kind(JsonKind::JSONNULL) {
}

inline JsonValue::JsonValue(const string &value) : kind(JsonKind::STRING), str(value) {
}

inline JsonValue::JsonValue(const char* value) : kind(JsonKind::STRING), str(value) {
}

inline JsonValue::JsonValue(double value) : kind(JsonKind::NUMBER), num(value) {
}

inline JsonValue::JsonValue(unordered_map<string, JsonValue> &&object) :
	kind(JsonKind::OBJECT), obj(make_uniq<unordered_map<string, JsonValue>>(std::move(object))) {
}

inline JsonValue::JsonValue(vector<JsonValue> &&array) :
	kind(JsonKind::ARRAY), arr(make_uniq<vector<JsonValue>>(std::move(array))) {
}

// Avoid ambiguous calls with implicit conversion from bool
template <typename T, typename = typename std::enable_if<std::is_same<T, bool>::value>::type>
inline JsonValue::JsonValue(T value) : kind(JsonKind::BOOLEAN), bit(value) {
}

//----------------------------------------------------------------------------------------------------------------------
// Accessors
//----------------------------------------------------------------------------------------------------------------------

inline JsonKind JsonValue::GetKind() const & {
	return kind;
}

inline JsonValue &JsonValue::operator[](const string &key) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	return obj->operator[](key);
}

inline JsonValue &JsonValue::operator[](idx_t index) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= arr->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return arr->operator[](index);
}

inline const JsonValue &JsonValue::operator[](const string &key) const {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	const auto entry = obj->find(key);
	if (entry == obj->end()) {
		throw InvalidInputException("Key not found in JSON object!");
	}
	return entry->second;
}

inline const JsonValue &JsonValue::operator[](idx_t index) const {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= arr->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return arr->operator[](index);
}

inline JsonValue &JsonValue::Get(idx_t index) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= arr->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return arr->operator[](index);
}

inline const JsonValue &JsonValue::Get(idx_t index) const {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= arr->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return arr->operator[](index);
}

inline JsonValue &JsonValue::Get(const string &key) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	const auto entry = obj->find(key);
	if (entry == obj->end()) {
		throw InvalidInputException("Key not found in JSON object!");
	}
	return entry->second;
}

inline const JsonValue &JsonValue::Get(const string &key) const {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	const auto entry = obj->find(key);
	if (entry == obj->end()) {
		throw InvalidInputException("Key not found in JSON object!");
	}
	return entry->second;
}

//----------------------------------------------------------------------------------------------------------------------
// "Is" Methods
//----------------------------------------------------------------------------------------------------------------------

inline bool JsonValue::IsNull() const {
	return kind == JsonKind::JSONNULL;
}

inline bool JsonValue::IsBool() const {
	return kind == JsonKind::BOOLEAN;
}

inline bool JsonValue::IsNumber() const {
	return kind == JsonKind::NUMBER;
}

inline bool JsonValue::IsString() const {
	return kind == JsonKind::STRING;
}

inline bool JsonValue::IsArray() const {
	return kind == JsonKind::ARRAY;
}

inline bool JsonValue::IsObject() const {
	return kind == JsonKind::OBJECT;
}

// clang-format off
template <> inline bool JsonValue::Is<bool>() const & { return IsBool(); }
template <> inline bool JsonValue::Is<double>() const & { return IsNumber(); }
template <> inline bool JsonValue::Is<string>() const & { return IsString(); }
// clang-format on

//----------------------------------------------------------------------------------------------------------------------
// "As" Methods
//----------------------------------------------------------------------------------------------------------------------

inline const bool &JsonValue::AsBool() const {
	if (kind != JsonKind::BOOLEAN) {
		throw InvalidTypeException("Cannot convert non-BOOLEAN JSON value to bool!");
	}
	return bit;
}

inline const string &JsonValue::AsString() const {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert non-STRING JSON value to string!");
	}
	return str;
}

inline const double &JsonValue::AsNumber() const {
	if (kind != JsonKind::NUMBER) {
		throw InvalidTypeException("Cannot convert non-NUMBER JSON value to double!");
	}
	return num;
}

// clang-format off
template <> inline const bool &JsonValue::As<bool>() const { return AsBool(); }
template <> inline const double &JsonValue::As<double>() const { return AsNumber(); }
template <> inline const string &JsonValue::As<string>() const { return AsString(); }
// clang-format on

//----------------------------------------------------------------------------------------------------------------------
// Array & Object Methods
//----------------------------------------------------------------------------------------------------------------------

inline idx_t JsonValue::Count() const {
	switch (kind) {
		case JsonKind::ARRAY: return arr->size();
		case JsonKind::OBJECT: return obj->size();
		default: throw InvalidTypeException("Cannot get Count of non-ARRAY/OBJECT JSON value!");
	}
}

template<class T>
inline void JsonValue::Push(const T &value) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot push to non-ARRAY JSON value!");
	}
	arr->push_back(value);
}

template<class T>
inline void JsonValue::Push(T &&value) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot push to non-ARRAY JSON value!");
	}
	arr->push_back(std::forward<T>(value));
}

template<class T>
inline void JsonValue::Push(const string& key, const T& value) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot push to non-OBJECT JSON value!");
	}
	obj->emplace(key, value);
}

template<class T>
inline void JsonValue::Push(const string& key, T&& value) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot push to non-OBJECT JSON value!");
	}
	obj->emplace(key, std::forward<T>(value));
}

//----------------------------------------------------------------------------------------------------------------------
// Move semantics & Destructor
//----------------------------------------------------------------------------------------------------------------------

inline JsonValue::JsonValue(JsonValue &&other) noexcept : kind(other.kind) {
	switch (kind) {
	case JsonKind::BOOLEAN:
		bit = other.bit;
	break;
	case JsonKind::NUMBER:
		num = other.num;
	break;
	case JsonKind::STRING:
		str = std::move(other.str);
	break;
	case JsonKind::ARRAY:
		arr = std::move(other.arr);
	break;
	case JsonKind::OBJECT:
		obj = std::move(other.obj);
	break;
	default:
		break;
	}
}

inline JsonValue &JsonValue::operator=(JsonValue &&other) noexcept {
	if (&other == this) {
		return *this;
	}

	// Destroy this
	this->~JsonValue();

	// Steal data from other
	kind = other.kind;
	switch (kind) {
	case JsonKind::BOOLEAN:
		bit = other.bit;
		break;
	case JsonKind::NUMBER:
		num = other.num;
		break;
	case JsonKind::STRING:
		str = std::move(other.str);
		break;
	case JsonKind::ARRAY:
		arr = std::move(other.arr);
		break;
	case JsonKind::OBJECT:
		obj = std::move(other.obj);
		break;
	default:
		break;
	}
	return *this;
}

inline JsonValue::~JsonValue() {
	switch (kind) {
		case JsonKind::STRING:
			str.~basic_string();
		break;
		case JsonKind::ARRAY:
			arr.~unique_ptr();
		break;
		case JsonKind::OBJECT:
			obj.~unique_ptr();
		break;
		default: break;
	}
}


enum class ComplexJSONType : uint8_t { VALUE = 0, OBJECT = 1, ARRAY = 2 };

//! Custom struct to handle both strings and nested JSON objects
struct ComplexJSON {
	//! Constructor for string values
	explicit ComplexJSON(const string &str);
	//! Basic empty constructor
	ComplexJSON();
	//! Adds entry to the underlying map, also sets the type to OBJECT
	void AddObjectEntry(const string &key, unique_ptr<ComplexJSON> object);
	//! Adds element to the underlying list, also sets the type to ARRAY
	void AddArrayElement(unique_ptr<ComplexJSON> object);
	//! Gets a ComplexJSON object from the map
	ComplexJSON &GetObject(const string &key);
	//! Gets a ComplexJSON element from the list
	ComplexJSON &GetArrayElement(const idx_t &index);
	//! Gets a string version of the underlying ComplexJSON object from the map
	string GetValue(const string &key) const;
	//! Gets a string version of the underlying ComplexJSON array from the list
	string GetValue(const idx_t &index) const;
	//! Recursive function for GetValue
	static string GetValueRecursive(const ComplexJSON &child);
	//! Flattens this json to a top level key -> nested json
	unordered_map<string, string> Flatten() const;

private:
	//! Basic string value, in case this is the last value of a nested json
	string str_value;
	//! If this is a json object a map of key/value
	unordered_map<string, unique_ptr<ComplexJSON>> obj_value;
	//! If this is a json array a list of values
	vector<unique_ptr<ComplexJSON>> arr_value;
	//! If this json is an object (i.e., map or not)
	ComplexJSONType type;
};

} // namespace duckdb
