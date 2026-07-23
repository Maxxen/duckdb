#include "catch.hpp"
#include "capi_v2_test_helpers.hpp"

// ---------------------------------------------------------------------------
// Function property channel (set_property / get_property)
// ---------------------------------------------------------------------------

namespace {

// Drives `body` with a live context, mirroring the registration pattern used by
// the scalar/aggregate function tests.
template <class FN>
void WithContext(FN &&body) {
	duckdb_v2_environment_handle env = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	body(conn, nullptr);

	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

} // namespace

TEST_CASE("V2 scalar function properties: round-trip and validation", "[capi_v2][function_property]") {
	WithContext([](duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err) {
		duckdb_v2_scalar_function_builder_handle builder = nullptr;
		REQUIRE(duckdb_v2_scalar_function_builder_create(&builder, err) == DUCKDB_V2_ERROR_NONE);

		DUCKDB_V2_FUNCTION_PROPERTY_VALUE value = DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;

		// Default stability is CONSISTENT.
		REQUIRE(duckdb_v2_scalar_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY, &value,
		                                                       err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT);

		// Round-trip VOLATILE.
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                       DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE,
		                                                       err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY, &value,
		                                                       err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE);

		// Round-trip a boolean-style property expressed as named values.
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
		                                                       DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE,
		                                                       err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_scalar_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY, &value,
		                                                       err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE);

		// A value that belongs to a different key is rejected.
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                       DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL,
		                                                       err) == DUCKDB_V2_ERROR_INPUT_INVALID);

		// A value in the key's block but not a defined enumerator is rejected.
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                       static_cast<DUCKDB_V2_FUNCTION_PROPERTY_VALUE>(0x010005),
		                                                       err) == DUCKDB_V2_ERROR_INPUT_INVALID);

		// An aggregate-only key is not valid for a scalar function (set and get).
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
		                                                       DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO,
		                                                       err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_scalar_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
		                                                       &value, err) == DUCKDB_V2_ERROR_INPUT_INVALID);

		// Null-argument guards.
		REQUIRE(duckdb_v2_scalar_function_builder_set_property(nullptr, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                       DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE,
		                                                       err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_scalar_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY, nullptr,
		                                                       err) == DUCKDB_V2_ERROR_INPUT_INVALID);

		REQUIRE(duckdb_v2_scalar_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
	});
}

TEST_CASE("V2 aggregate function properties: common and aggregate keys", "[capi_v2][function_property]") {
	WithContext([](duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err) {
		duckdb_v2_aggregate_function_builder_handle builder = nullptr;
		REQUIRE(duckdb_v2_aggregate_function_builder_create(&builder, err) == DUCKDB_V2_ERROR_NONE);

		DUCKDB_V2_FUNCTION_PROPERTY_VALUE value = DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;

		// Aggregate-specific key defaults to YES, round-trips to NO.
		REQUIRE(duckdb_v2_aggregate_function_builder_get_property(
		            builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT, &value, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES);

		REQUIRE(duckdb_v2_aggregate_function_builder_set_property(
		            builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
		            DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_aggregate_function_builder_get_property(
		            builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT, &value, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO);

		// A common (base) key works on aggregate functions too.
		REQUIRE(duckdb_v2_aggregate_function_builder_set_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                          DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE,
		                                                          err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_aggregate_function_builder_get_property(builder, DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
		                                                          &value, err) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(value == DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE);

		// Cross-key value mismatch is still rejected.
		REQUIRE(duckdb_v2_aggregate_function_builder_set_property(
		            builder, DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
		            DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO, err) == DUCKDB_V2_ERROR_INPUT_INVALID);

		REQUIRE(duckdb_v2_aggregate_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
	});
}
