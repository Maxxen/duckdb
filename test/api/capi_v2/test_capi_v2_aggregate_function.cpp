#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"

#include <algorithm>
#include <atomic>
#include <vector>

// ---------------------------------------------------------------------------
// Aggregate Function API
// ---------------------------------------------------------------------------

namespace {

// The aggregate state is a non-trivial object with a real lifetime: it owns a heap-allocated
// buffer that must be constructed in the init callback and destroyed in the destroy callback.
// We collect every input value into the buffer and compute the median over all of them in finalize.
using MedianState = std::vector<int32_t>;

// Returns true if row `idx` is valid (not NULL). A NULL validity mask means "all valid".
bool RowIsValid(const uint64_t *validity, idx_t idx) {
	return validity == nullptr || (validity[idx >> 6] & (1ULL << (idx & 63)));
}

void SizeCallback(duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_aggregate_function_size_set_size(info, sizeof(MedianState), err) == DUCKDB_V2_ERROR_NONE);
}

void InitCallback(duckdb_v2_aggregate_function_init_info_handle info, duckdb_v2_error_info_handle *err) {
	// DuckDB hands us zero-initialized memory; construct the state object in place.
	void *state = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_init_get_state(info, &state, err) == DUCKDB_V2_ERROR_NONE);
	new (state) MedianState();
}

void UpdateCallback(duckdb_v2_aggregate_function_update_info_handle info, duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle input = nullptr;
	if (duckdb_v2_aggregate_function_update_get_input(info, &input, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_vector_handle input_vec = nullptr;
	if (duckdb_v2_data_chunk_get_vector(input, 0, &input_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_vector_view view;
	if (duckdb_v2_vector_get_view(input_vec, &view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	const auto data = static_cast<const int32_t *>(view.data);
	void **states_raw = nullptr;
	if (duckdb_v2_aggregate_function_update_get_states(info, &states_raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto states = reinterpret_cast<MedianState **>(states_raw);

	idx_t count = 0;
	if (duckdb_v2_aggregate_function_update_get_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		const auto idx = view.sel ? view.sel[i] : i;
		if (!RowIsValid(view.validity, idx)) {
			continue; // Skip NULLs
		}
		states[i]->push_back(data[idx]);
	}
}

void CombineCallback(duckdb_v2_aggregate_function_combine_info_handle info, duckdb_v2_error_info_handle *err) {
	void **sources_raw = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_sources(info, &sources_raw, err) == DUCKDB_V2_ERROR_NONE);
	const auto sources = reinterpret_cast<MedianState **>(sources_raw);

	void **targets_raw = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_targets(info, &targets_raw, err) == DUCKDB_V2_ERROR_NONE);
	const auto targets = reinterpret_cast<MedianState **>(targets_raw);

	idx_t count = 0;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);

	for (idx_t i = 0; i < count; i++) {
		auto &source = *sources[i];
		auto &target = *targets[i];
		target.insert(target.end(), source.begin(), source.end());
	}
}

void FinalizeCallback(duckdb_v2_aggregate_function_finalize_info_handle info, duckdb_v2_error_info_handle *err) {
	void **states_raw = nullptr;
	if (duckdb_v2_aggregate_function_finalize_get_states(info, &states_raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto states = reinterpret_cast<MedianState **>(states_raw);

	duckdb_v2_vector_handle result_vec = nullptr;
	if (duckdb_v2_aggregate_function_finalize_get_result(info, &result_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	int32_t *result = nullptr;
	if (duckdb_v2_vector_get_data_mutable(result_vec, (void **)&result, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	idx_t count = 0;
	if (duckdb_v2_aggregate_function_finalize_get_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	idx_t result_offset = 0;
	if (duckdb_v2_aggregate_function_finalize_get_result_offset(info, &result_offset, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		auto &values = *states[i];

		std::sort(values.begin(), values.end());

		// Median of an odd-sized set is the middle element; for an even-sized set we take the
		// lower of the two middle elements to keep the result an integer.
		const auto median = values.empty() ? 0 : values[(values.size() - 1) / 2];

		result[result_offset + i] = median;
	}
}

void DestroyCallback(duckdb_v2_aggregate_function_destroy_info_handle info, duckdb_v2_error_info_handle *err) {
	void **states_raw = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_destroy_get_states(info, &states_raw, err) == DUCKDB_V2_ERROR_NONE);
	const auto states = reinterpret_cast<MedianState **>(states_raw);

	idx_t count = 0;
	REQUIRE(duckdb_v2_aggregate_function_destroy_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);

	for (idx_t i = 0; i < count; i++) {
		states[i]->~MedianState();
	}
}

} // namespace

TEST_CASE("V2 aggregate: median with stateful aggregate", "[capi_v2][aggregate]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);

	// Register the aggregate within a transaction so we have a context.
	duckdb_v2_connection_execute_with_context(
	    conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_aggregate_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_aggregate_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_handle type = nullptr;
		    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_name(builder, V2Str("my_median"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_add_parameter(builder, V2Str("x"), type, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_return_type(builder, type, err) == DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_size_callback(builder, SizeCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_init_callback(builder, InitCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_update_callback(builder, UpdateCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_combine_callback(builder, CombineCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_finalize_callback(builder, FinalizeCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_destroy_callback(builder, DestroyCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(builder == nullptr);

		    duckdb_v2_logical_type_destroy(&type);
	    },
	    nullptr, nullptr);

	SECTION("median over a single group") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(conn, "SELECT my_median(i) AS result FROM (VALUES (1), (2), (3), (4), (5)) AS t(i)", &result,
		                nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);

		duckdb_v2_vector_handle result_vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &result_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_view result_view;
		REQUIRE(duckdb_v2_vector_get_view(result_vec, &result_view, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(result_view.count == 1);

		// Median of 1,2,3,4,5 is 3.
		REQUIRE((static_cast<const int32_t *>(result_view.data))[0] == 3);

		REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("median over multiple groups") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(conn,
		                "SELECT g, my_median(i) AS result FROM (VALUES (0, 10), (0, 20), (0, 30), "
		                "(1, 1), (1, 2)) AS t(g, i) GROUP BY g ORDER BY g",
		                &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);

		duckdb_v2_vector_handle result_vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 1, &result_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_view result_view;
		REQUIRE(duckdb_v2_vector_get_view(result_vec, &result_view, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(result_view.count == 2);

		const auto result_data = static_cast<const int32_t *>(result_view.data);
		const auto idx0 = result_view.sel ? result_view.sel[0] : 0;
		const auto idx1 = result_view.sel ? result_view.sel[1] : 1;

		REQUIRE(result_data[idx0] == 20); // median of 10,20,30
		REQUIRE(result_data[idx1] == 1);  // median of 1,2 (lower middle)

		REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);
	}

	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

// ---------------------------------------------------------------------------
// Aggregate bind data: a bind callback sets bind data that threads through to
// the update/combine/finalize callbacks.
// ---------------------------------------------------------------------------

namespace {

std::atomic<bool> g_bind_data_seen_in_update {false};
std::atomic<int> g_bind_data_destroyed {0};

void BindDataBindCallback(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_context_handle,
                          duckdb_v2_error_info_handle *err) {
	// Compute a "multiplier" at bind time and hand it to execution as bind data.
	duckdb_v2_opaque bind_data {new int32_t(10),
	                            [](void *p) {
		                            g_bind_data_destroyed++;
		                            delete static_cast<int32_t *>(p);
	                            },
	                            nullptr};
	REQUIRE(duckdb_v2_aggregate_function_bind_set_bind_data(info, bind_data, err) == DUCKDB_V2_ERROR_NONE);
}

void BindDataSizeCallback(duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_aggregate_function_size_set_size(info, sizeof(int64_t), err) == DUCKDB_V2_ERROR_NONE);
}

void BindDataInitCallback(duckdb_v2_aggregate_function_init_info_handle info, duckdb_v2_error_info_handle *err) {
	void *state = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_init_get_state(info, &state, err) == DUCKDB_V2_ERROR_NONE);
	*static_cast<int64_t *>(state) = 0;
}

void BindDataUpdateCallback(duckdb_v2_aggregate_function_update_info_handle info, duckdb_v2_error_info_handle *err) {
	void *bind_data = nullptr;
	if (duckdb_v2_aggregate_function_update_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (bind_data) {
		g_bind_data_seen_in_update = true;
	}

	duckdb_v2_data_chunk_handle input = nullptr;
	if (duckdb_v2_aggregate_function_update_get_input(info, &input, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_handle input_vec = nullptr;
	if (duckdb_v2_data_chunk_get_vector(input, 0, &input_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_view view;
	if (duckdb_v2_vector_get_view(input_vec, &view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto data = static_cast<const int32_t *>(view.data);
	void **states_raw = nullptr;
	if (duckdb_v2_aggregate_function_update_get_states(info, &states_raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto states = reinterpret_cast<int64_t **>(states_raw);
	idx_t count = 0;
	if (duckdb_v2_aggregate_function_update_get_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		const auto idx = view.sel ? view.sel[i] : i;
		if (!RowIsValid(view.validity, idx)) {
			continue;
		}
		*states[i] += data[idx];
	}
}

void BindDataCombineCallback(duckdb_v2_aggregate_function_combine_info_handle info, duckdb_v2_error_info_handle *err) {
	void **sources_raw = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_sources(info, &sources_raw, err) == DUCKDB_V2_ERROR_NONE);
	const auto sources = reinterpret_cast<int64_t **>(sources_raw);
	void **targets_raw = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_targets(info, &targets_raw, err) == DUCKDB_V2_ERROR_NONE);
	const auto targets = reinterpret_cast<int64_t **>(targets_raw);
	idx_t count = 0;
	REQUIRE(duckdb_v2_aggregate_function_combine_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);
	for (idx_t i = 0; i < count; i++) {
		*targets[i] += *sources[i];
	}
}

void BindDataFinalizeCallback(duckdb_v2_aggregate_function_finalize_info_handle info,
                              duckdb_v2_error_info_handle *err) {
	// Use the bind-time multiplier handed to us via bind data.
	void *bind_data = nullptr;
	if (duckdb_v2_aggregate_function_finalize_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto multiplier = bind_data ? *static_cast<const int32_t *>(bind_data) : 1;
	void **states_raw = nullptr;
	if (duckdb_v2_aggregate_function_finalize_get_states(info, &states_raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	const auto states = reinterpret_cast<int64_t **>(states_raw);

	duckdb_v2_vector_handle result_vec = nullptr;
	if (duckdb_v2_aggregate_function_finalize_get_result(info, &result_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int32_t *result = nullptr;
	if (duckdb_v2_vector_get_data_mutable(result_vec, (void **)&result, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t count = 0;
	if (duckdb_v2_aggregate_function_finalize_get_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t result_offset = 0;
	if (duckdb_v2_aggregate_function_finalize_get_result_offset(info, &result_offset, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		result[result_offset + i] = static_cast<int32_t>(*states[i] * multiplier);
	}
}

} // namespace

TEST_CASE("V2 aggregate: bind data threads through to execution callbacks", "[capi_v2][aggregate]") {
	g_bind_data_seen_in_update = false;
	g_bind_data_destroyed = 0;

	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_execute_with_context(
	    conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_aggregate_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_aggregate_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_handle type = nullptr;
		    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_name(builder, V2Str("sum_times_ten"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_add_parameter(builder, V2Str("x"), type, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_return_type(builder, type, err) == DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_bind_callback(builder, BindDataBindCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_size_callback(builder, BindDataSizeCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_init_callback(builder, BindDataInitCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_update_callback(builder, BindDataUpdateCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_combine_callback(builder, BindDataCombineCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_finalize_callback(builder, BindDataFinalizeCallback,
		                                                                       err) == DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_destroy(&type);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, "SELECT sum_times_ten(i) AS result FROM (VALUES (1), (2), (3), (4), (5)) AS t(i)", &result,
	                nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	duckdb_v2_vector_handle result_vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &result_vec, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_vector_view result_view;
	REQUIRE(duckdb_v2_vector_get_view(result_vec, &result_view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_view.count == 1);

	// (1 + 2 + 3 + 4 + 5) * 10 = 150 -> proves the bind-time multiplier reached finalize.
	REQUIRE((static_cast<const int32_t *>(result_view.data))[0] == 150);
	// And the update callback saw the bind data too.
	REQUIRE(g_bind_data_seen_in_update.load());

	REQUIRE(duckdb_v2_data_chunk_destroy(&chunk) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&result) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);

	// The bind data was cleaned up via its destructor.
	REQUIRE(g_bind_data_destroyed.load() >= 1);
}

// ---------------------------------------------------------------------------
// Error-code round-trip: a code an update callback sets on its err slot
// surfaces to the caller as that code, not collapsed to INVALID_INPUT.
// ---------------------------------------------------------------------------

namespace {

void FailingSizeCallback(duckdb_v2_aggregate_function_size_info_handle info, duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_aggregate_function_size_set_size(info, sizeof(int64_t), err) == DUCKDB_V2_ERROR_NONE);
}

void FailingInitCallback(duckdb_v2_aggregate_function_init_info_handle info, duckdb_v2_error_info_handle *err) {
	void *state = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_init_get_state(info, &state, err) == DUCKDB_V2_ERROR_NONE);
	*static_cast<int64_t *>(state) = 0;
}

void FailingUpdateCallback(duckdb_v2_aggregate_function_update_info_handle info, duckdb_v2_error_info_handle *err) {
	// Name a specific, non-INVALID_INPUT error class from inside the callback.
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);
	duckdb_v2_error_info_set_text(*err, V2Str("failing_agg: value out of range"));
}

void FailingCombineCallback(duckdb_v2_aggregate_function_combine_info_handle info, duckdb_v2_error_info_handle *err) {
}

void FailingFinalizeCallback(duckdb_v2_aggregate_function_finalize_info_handle info, duckdb_v2_error_info_handle *err) {
}

} // namespace

TEST_CASE("V2 aggregate: a callback error code round-trips (not collapsed to INVALID_INPUT)", "[capi_v2][aggregate]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_execute_with_context(
	    conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_aggregate_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_aggregate_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_handle type = nullptr;
		    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_name(builder, V2Str("failing_agg"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_add_parameter(builder, V2Str("x"), type, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_return_type(builder, type, err) == DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_size_callback(builder, FailingSizeCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_init_callback(builder, FailingInitCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_update_callback(builder, FailingUpdateCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_combine_callback(builder, FailingCombineCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_set_finalize_callback(builder, FailingFinalizeCallback, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_destroy(&builder) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_destroy(&type);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, "SELECT failing_agg(i) FROM (VALUES (1), (2), (3)) AS t(i)", &result, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(result != nullptr);

	// The aggregate executes lazily during stepping, so the callback's error
	// surfaces on the step return code, not from statement_execute.
	duckdb_v2_error_info_handle err = nullptr;
	DUCKDB_V2_ERROR step_rc = DUCKDB_V2_ERROR_NONE;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		step_rc = duckdb_v2_result_step(result, &chunk, &status, &err);
		if (step_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
			duckdb_v2_data_chunk_destroy(&chunk);
			continue;
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			step_rc = duckdb_v2_result_wait(result, &err);
			if (step_rc != DUCKDB_V2_ERROR_NONE) {
				break;
			}
			continue;
		}
		// FINISHED or CANCELLED with no error: the failure did not surface.
		break;
	}

	// The callback's OUT_OF_RANGE surfaces as itself, not as INVALID_INPUT.
	REQUIRE(step_rc == DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);

	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}
