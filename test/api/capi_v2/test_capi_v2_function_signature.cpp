#include "capi_v2_test_helpers.hpp"

// ---------------------------------------------------------------------------
// Function signature handle (C surface): setters, getters, and the eager
// default-value cast. Registration-time Verify() rejections are covered in the
// scalar, aggregate, and table suites; this file exercises the handle in
// isolation.
// ---------------------------------------------------------------------------

TEST_CASE("V2 signature: create, populate, read back", "[capi_v2][signature]") {
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto dbl = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
	auto bigint = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_function_signature_create(&sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(sig != nullptr);

	// Empty signature: no params, no varargs, no return type.
	idx_t count = 99;
	REQUIRE(duckdb_v2_function_signature_get_parameter_count(sig, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	bool flag = true;
	REQUIRE(duckdb_v2_function_signature_has_varargs(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(flag);
	REQUIRE(duckdb_v2_function_signature_has_return_type(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(flag);

	// A required parameter, a defaulted parameter, varargs, and a return type.
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, V2Str("a"), integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_handle def = V2Int64Value(5); // BIGINT 5, eagerly cast to INTEGER
	REQUIRE(duckdb_v2_function_signature_add_parameter_default(sig, V2Str("b"), integer, def, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&def);
	REQUIRE(duckdb_v2_function_signature_set_varargs(sig, dbl, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, bigint, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_function_signature_get_parameter_count(sig, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);

	// Parameter names (borrowed) and the strlen/len agreement pin.
	duckdb_v2_identifier_t name = {nullptr, 0};
	REQUIRE(duckdb_v2_function_signature_get_parameter_name(sig, 0, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "a");
	REQUIRE(strlen(name.ptr) == name.len);
	REQUIRE(duckdb_v2_function_signature_get_parameter_name(sig, 1, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "b");

	// Parameter types (owned copies).
	duckdb_v2_logical_type_handle t0 = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_type(sig, 0, &t0, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id0 = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(t0, &id0, nullptr);
	REQUIRE(id0 == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&t0);

	// has_default per parameter.
	REQUIRE(duckdb_v2_function_signature_parameter_has_default(sig, 0, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(flag);
	REQUIRE(duckdb_v2_function_signature_parameter_has_default(sig, 1, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(flag);

	// The default was eagerly cast to the concrete parameter type (INTEGER 5).
	duckdb_v2_value_handle got = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_default(sig, 1, &got, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_handle got_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(got, &got_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID got_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(got_type, &got_id, nullptr);
	REQUIRE(got_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&got_type);
	REQUIRE(V2LeafPayloadConsume<int32_t>(got) == 5);

	// varargs / return type presence and owned copies.
	REQUIRE(duckdb_v2_function_signature_has_varargs(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(flag);
	duckdb_v2_logical_type_handle va = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_varargs(sig, &va, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID va_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(va, &va_id, nullptr);
	REQUIRE(va_id == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
	duckdb_v2_logical_type_destroy(&va);

	REQUIRE(duckdb_v2_function_signature_has_return_type(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(flag);
	duckdb_v2_logical_type_handle rt = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_return_type(sig, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID rt_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(rt, &rt_id, nullptr);
	REQUIRE(rt_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_logical_type_destroy(&rt);

	// Destroy nulls the slot and is idempotent.
	REQUIRE(duckdb_v2_function_signature_destroy(&sig) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(sig == nullptr);
	REQUIRE(duckdb_v2_function_signature_destroy(&sig) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_destroy(&integer);
	duckdb_v2_logical_type_destroy(&dbl);
	duckdb_v2_logical_type_destroy(&bigint);
}

TEST_CASE("V2 signature: typed-NULL default on an ANY parameter is stored as-is", "[capi_v2][signature]") {
	auto any = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_function_signature_create(&sig, nullptr) == DUCKDB_V2_ERROR_NONE);

	// A typed NULL (NULL of INTEGER) as the default for an ANY parameter. Because
	// the parameter type is ANY (incomplete), the value is stored unchanged rather
	// than cast, so its INTEGER type and NULL-ness survive.
	duckdb_v2_value_handle null_int = nullptr;
	REQUIRE(duckdb_v2_value_create_null(integer, &null_int, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_add_parameter_default(sig, V2Str("x"), any, null_int, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&null_int);

	duckdb_v2_value_handle got = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_default(sig, 0, &got, nullptr) == DUCKDB_V2_ERROR_NONE);
	bool is_null = false;
	REQUIRE(duckdb_v2_value_is_null(got, &is_null, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(is_null);
	duckdb_v2_logical_type_handle got_type = nullptr;
	REQUIRE(duckdb_v2_value_get_logical_type(got, &got_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID got_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(got_type, &got_id, nullptr);
	REQUIRE(got_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_logical_type_destroy(&got_type);
	duckdb_v2_value_destroy(&got);

	// The ANY parameter itself keeps type ANY.
	duckdb_v2_logical_type_handle pt = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_type(sig, 0, &pt, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID pt_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(pt, &pt_id, nullptr);
	REQUIRE(pt_id == DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
	duckdb_v2_logical_type_destroy(&pt);

	duckdb_v2_function_signature_destroy(&sig);
	duckdb_v2_logical_type_destroy(&any);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 signature: add_parameter_default rejects a non-castable default", "[capi_v2][signature]") {
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_function_signature_create(&sig, nullptr) == DUCKDB_V2_ERROR_NONE);

	// 'abc' does not cast to INTEGER; the eager cast fails with INVALID_INPUT.
	duckdb_v2_value_handle bad = V2VarcharValue("abc");
	REQUIRE(duckdb_v2_function_signature_add_parameter_default(sig, V2Str("x"), integer, bad, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_value_destroy(&bad);

	// The rejected add did not append a parameter.
	idx_t count = 99;
	REQUIRE(duckdb_v2_function_signature_get_parameter_count(sig, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);

	duckdb_v2_function_signature_destroy(&sig);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 signature: setter and getter error paths", "[capi_v2][signature]") {
	auto integer = V2TypeOf(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_function_signature_create(&sig, nullptr) == DUCKDB_V2_ERROR_NONE);

	// NULL / empty parameter name rejected.
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, V2Str(""), integer, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	// NULL / INVALID varargs type rejected.
	REQUIRE(duckdb_v2_function_signature_set_varargs(sig, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, V2Str("a"), integer, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Out-of-range index on every by-index getter.
	duckdb_v2_identifier_t name = {nullptr, 0};
	REQUIRE(duckdb_v2_function_signature_get_parameter_name(sig, 5, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_type(sig, 5, &t, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	bool flag = false;
	REQUIRE(duckdb_v2_function_signature_parameter_has_default(sig, 5, &flag, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	// A parameter without a default: get_parameter_default errors.
	duckdb_v2_value_handle v = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_parameter_default(sig, 0, &v, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// varargs / return type absent: the getters error.
	duckdb_v2_logical_type_handle va = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_varargs(sig, &va, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_logical_type_handle rt = nullptr;
	REQUIRE(duckdb_v2_function_signature_get_return_type(sig, &rt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_function_signature_destroy(&sig);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 signature: setters reject an INVALID type", "[capi_v2][signature]") {
	// Deliberate V1 interop pin: V2 cannot construct an INVALID logical type
	// (logical_type_create_from_id excludes the sentinel), but V1 handles are
	// interchangeable and duckdb_create_logical_type(DUCKDB_TYPE_INVALID) yields
	// one. INVALID doubles as the unset sentinel, so every setter refuses it
	// instead of reporting success for a write that reads back as absent.
	auto invalid = V1ToV2(duckdb_create_logical_type(DUCKDB_TYPE_INVALID));

	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_function_signature_create(&sig, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, V2Str("x"), invalid, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_function_signature_set_varargs(sig, invalid, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, invalid, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// The rejected writes left the signature untouched.
	idx_t count = 99;
	REQUIRE(duckdb_v2_function_signature_get_parameter_count(sig, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	bool flag = true;
	REQUIRE(duckdb_v2_function_signature_has_varargs(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(flag);
	REQUIRE(duckdb_v2_function_signature_has_return_type(sig, &flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(flag);

	duckdb_v2_function_signature_destroy(&sig);
	duckdb_v2_logical_type_destroy(&invalid);
}
