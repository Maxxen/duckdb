#include "catch.hpp"
#include "capi_v2_internal.hpp"

#include <algorithm>
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

void SizeCallback(duckdb_v2_aggregate_function_size_args *args, duckdb_v2_error_info_ptr *err) {
	args->out_size = sizeof(MedianState);
}

void InitCallback(duckdb_v2_aggregate_function_init_args *args, duckdb_v2_error_info_ptr *err) {
	// DuckDB hands us zero-initialized memory; construct the state object in place.
	new (args->state) MedianState();
}

void UpdateCallback(duckdb_v2_aggregate_function_update_args *args, duckdb_v2_error_info_ptr *err) {
	duckdb_v2_vector_ptr input_vec = nullptr;
	if (duckdb_v2_data_chunk_get_vector(args->input, 0, &input_vec, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_vector_view view;
	if (duckdb_v2_vector_get_view(input_vec, &view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	const auto data = static_cast<const int32_t *>(view.data);
	const auto states = reinterpret_cast<MedianState **>(args->states);

	for (idx_t i = 0; i < args->count; i++) {
		const auto idx = view.sel ? view.sel[i] : i;
		if (!RowIsValid(view.validity, idx)) {
			continue; // Skip NULLs
		}
		states[i]->push_back(data[idx]);
	}
}

void CombineCallback(duckdb_v2_aggregate_function_combine_args *args, duckdb_v2_error_info_ptr *err) {
	const auto sources = reinterpret_cast<MedianState **>(args->sources);
	const auto targets = reinterpret_cast<MedianState **>(args->targets);

	for (idx_t i = 0; i < args->count; i++) {
		auto &source = *sources[i];
		auto &target = *targets[i];
		target.insert(target.end(), source.begin(), source.end());
	}
}

void FinalizeCallback(duckdb_v2_aggregate_function_finalize_args *args, duckdb_v2_error_info_ptr *err) {
	const auto states = reinterpret_cast<MedianState **>(args->states);

	int32_t *result = nullptr;
	if (duckdb_v2_vector_get_data_mutable(args->result, (void **)&result, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	for (idx_t i = 0; i < args->count; i++) {
		auto &values = *states[i];

		std::sort(values.begin(), values.end());

		// Median of an odd-sized set is the middle element; for an even-sized set we take the
		// lower of the two middle elements to keep the result an integer.
		const auto median = values.empty() ? 0 : values[(values.size() - 1) / 2];

		result[args->result_offset + i] = median;
	}
}

void DestroyCallback(duckdb_v2_aggregate_function_destroy_args *args, duckdb_v2_error_info_ptr *err) {
	const auto states = reinterpret_cast<MedianState **>(args->states);

	for (idx_t i = 0; i < args->count; i++) {
		states[i]->~MedianState();
	}
}

} // namespace

TEST_CASE("V2 aggregate: median with stateful aggregate", "[capi_v2][aggregate]") {
	duckdb_v2_environment_ptr env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_ptr db = nullptr;
	duckdb_v2_open(env, nullptr, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_ptr conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);

	// Register the aggregate within a transaction so we have a context.
	duckdb_v2_connection_execute_with_context(
	    conn,
	    [](duckdb_v2_context_ptr ctx, void *, duckdb_v2_error_info_ptr *err) {
		    duckdb_v2_aggregate_function_builder_ptr builder = nullptr;
		    REQUIRE(duckdb_v2_aggregate_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_logical_type_ptr type = nullptr;
		    REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type, err) ==
		            DUCKDB_V2_ERROR_NONE);

		    REQUIRE(duckdb_v2_aggregate_function_builder_set_name(builder, "my_median", err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_aggregate_function_builder_add_parameter(builder, "x", type, err) ==
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
		duckdb_v2_result_ptr result = nullptr;
		REQUIRE(duckdb_v2_connection_query(
		            conn, "SELECT my_median(i) AS result FROM (VALUES (1), (2), (3), (4), (5)) AS t(i)", &result,
		            nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_ptr chunk = nullptr;
		REQUIRE(duckdb_v2_result_get_chunk(result, 0, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_ptr result_vec = nullptr;
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
		duckdb_v2_result_ptr result = nullptr;
		REQUIRE(duckdb_v2_connection_query(conn,
		                                   "SELECT g, my_median(i) AS result FROM (VALUES (0, 10), (0, 20), (0, 30), "
		                                   "(1, 1), (1, 2)) AS t(g, i) GROUP BY g ORDER BY g",
		                                   &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_ptr chunk = nullptr;
		REQUIRE(duckdb_v2_result_get_chunk(result, 0, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_vector_ptr result_vec = nullptr;
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
