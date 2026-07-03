#include "catch.hpp"
#include "capi_v2_internal.hpp"
#include "capi_v2_test_helpers.hpp"
#include "test_helpers.hpp"

// ---------------------------------------------------------------------------
// V2 table function tests.
// ---------------------------------------------------------------------------

// The V2 vector-write API sizes per vector; the table-function bridge derives
// the output chunk's cardinality from the first output vector after exec. So an
// exec callback reports its row count by sizing output vector 0.
static void SetChunkSize(duckdb_v2_data_chunk_handle chunk, idx_t size) {
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_set_size(vec, size, nullptr);
}

// ---------------------------------------------------------------------------
// Simplest table function: no parameters, two columns, 5 rows, no pushdown.
//
// CREATE FUNCTION counter() RETURNS TABLE (i INTEGER, s VARCHAR)
// Produces rows: (0, "row_0"), (1, "row_1"), ..., (4, "row_4")
// ---------------------------------------------------------------------------

static constexpr idx_t COUNTER_NUM_ROWS = 5;

struct CounterGlobalState {
	idx_t next_row = 0;
};

static void counter_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                         duckdb_v2_error_info_handle *err) {
	// Declare output columns
	duckdb_v2_logical_type_handle int_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &int_type, err);

	duckdb_v2_logical_type_handle varchar_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &varchar_type, err);

	duckdb_v2_table_function_bind_add_result_column(info, V2Str("i"), int_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("s"), varchar_type, err);

	// Set cardinality hint
	duckdb_v2_table_function_bind_set_cardinality(info, COUNTER_NUM_ROWS, true, err);

	duckdb_v2_logical_type_destroy(&int_type);
	duckdb_v2_logical_type_destroy(&varchar_type);
}

static void counter_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                                duckdb_v2_error_info_handle *err) {
	auto *state = new CounterGlobalState();
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<CounterGlobalState *>(p); }, nullptr}, err);
}

static void counter_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                         duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	auto &state = *static_cast<CounterGlobalState *>(global_state);

	if (state.next_row >= COUNTER_NUM_ROWS) {
		SetChunkSize(chunk, 0);
		return;
	}

	// Get writable data pointers for each column
	duckdb_v2_vector_handle int_vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &int_vec, err);
	int32_t *int_data = nullptr;
	duckdb_v2_vector_get_data_mutable(int_vec, reinterpret_cast<void **>(&int_data), err);

	duckdb_v2_vector_handle str_vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 1, &str_vec, err);

	idx_t count = 0;
	while (state.next_row < COUNTER_NUM_ROWS && count < 2048) {
		int_data[count] = static_cast<int32_t>(state.next_row);
		auto s = "row_" + std::to_string(state.next_row);
		V2VectorAssignString(str_vec, count, s.c_str(), s.size(), err);
		state.next_row++;
		count++;
	}

	SetChunkSize(chunk, count);
}

// Helper: register the counter table function within a context callback
static void register_counter(duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *err) {
	duckdb_v2_table_function_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("counter"), err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, counter_bind, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, counter_init_global, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, counter_exec, err) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_builder_destroy(&builder);
}

TEST_CASE("V2 table function: simplest end-to-end", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	// Register the function (needs a context)
	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) { register_counter(ctx, err); },
	    nullptr, nullptr);

	SECTION("basic query returns correct row count") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT i FROM counter()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);

		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == COUNTER_NUM_ROWS);

		// Verify values
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);

		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, nullptr);

		auto *data = static_cast<const int32_t *>(view.data);
		for (idx_t i = 0; i < COUNTER_NUM_ROWS; i++) {
			idx_t idx = view.sel ? view.sel[i] : i;
			REQUIRE(data[idx] == static_cast<int32_t>(i));
		}

		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("SELECT * returns both columns with correct values") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT * FROM counter()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		idx_t col_count = 0;
		col_count = V2ColumnCount(result);
		REQUIRE(col_count == 2);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);

		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == COUNTER_NUM_ROWS);

		duckdb_v2_vector_handle int_vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &int_vec, nullptr);
		duckdb_v2_vector_view int_view;
		duckdb_v2_vector_get_view(int_vec, &int_view, nullptr);
		auto *int_data = static_cast<const int32_t *>(int_view.data);

		duckdb_v2_vector_handle str_vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 1, &str_vec, nullptr);
		duckdb_v2_vector_view str_view;
		duckdb_v2_vector_get_view(str_vec, &str_view, nullptr);
		auto *str_data = static_cast<const duckdb_v2_varchar_t *>(str_view.data);

		for (idx_t i = 0; i < COUNTER_NUM_ROWS; i++) {
			idx_t idx = int_view.sel ? int_view.sel[i] : i;
			REQUIRE(int_data[idx] == static_cast<int32_t>(i));

			idx_t sidx = str_view.sel ? str_view.sel[i] : i;
			REQUIRE(RowValid(str_view, sidx));
			duckdb_v2_str s = {nullptr, 0};
			REQUIRE(duckdb_v2_varchar_decode(&str_data[sidx], &s, nullptr) == DUCKDB_V2_ERROR_NONE);
			auto expected = "row_" + std::to_string(i);
			REQUIRE(s == expected);
		}

		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("WHERE clause filters rows (engine-side, no pushdown)") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT i FROM counter() WHERE i > 2", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);

		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == 2); // rows 3 and 4

		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("empty result when filter excludes all") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT i FROM counter() WHERE i > 100", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(V2DrainRowCount(result) == 0);

		duckdb_v2_result_destroy(&result);
	}
}

// ---------------------------------------------------------------------------
// Parameterized table function: counter_n(n INTEGER, start := 0)
// Produces rows: (start), (start+1), ..., (start+n-1)
// Tests positional and named parameters.
// ---------------------------------------------------------------------------

struct CounterNBindData {
	int64_t n;
	int64_t start;
};

struct CounterNGlobalState {
	int64_t next_row;
	int64_t end_row;
};

static void counter_n_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                           duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle n_val = nullptr;
	REQUIRE(duckdb_v2_table_function_bind_get_parameter(info, 0, &n_val, err) == DUCKDB_V2_ERROR_NONE);
	int64_t n = V2LeafPayload<int64_t>(n_val);
	duckdb_v2_value_destroy(&n_val);

	int64_t start = 0;
	duckdb_v2_value_handle start_val = nullptr;
	duckdb_v2_error_info_handle local_err = nullptr;
	if (duckdb_v2_table_function_bind_get_named_parameter(info, V2Str("start"), &start_val, &local_err) ==
	    DUCKDB_V2_ERROR_NONE) {
		start = V2LeafPayload<int64_t>(start_val);
		duckdb_v2_value_destroy(&start_val);
	}
	duckdb_v2_error_info_destroy(&local_err);

	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("val"), bigint_type, err);
	duckdb_v2_logical_type_destroy(&bigint_type);

	auto *bd = new CounterNBindData {n, start};
	duckdb_v2_table_function_bind_set_bind_data(
	    info, {bd, [](void *p) { delete static_cast<CounterNBindData *>(p); }, nullptr}, err);

	duckdb_v2_table_function_bind_set_cardinality(info, static_cast<idx_t>(n), true, err);
}

static void counter_n_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                                  duckdb_v2_error_info_handle *err) {
	void *raw_bd = nullptr;
	duckdb_v2_table_function_init_get_bind_data(info, &raw_bd, err);
	auto *bd = static_cast<CounterNBindData *>(raw_bd);

	auto *state = new CounterNGlobalState();
	state->next_row = bd->start;
	state->end_row = bd->start + bd->n;
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<CounterNGlobalState *>(p); }, nullptr}, err);
}

static void counter_n_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                           duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	auto &state = *static_cast<CounterNGlobalState *>(global_state);

	if (state.next_row >= state.end_row) {
		SetChunkSize(chunk, 0);
		return;
	}

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	int64_t *data = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);

	idx_t count = 0;
	while (state.next_row < state.end_row && count < 2048) {
		data[count] = state.next_row;
		state.next_row++;
		count++;
	}

	SetChunkSize(chunk, count);
}

static void register_counter_n(duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *err) {
	duckdb_v2_table_function_builder_handle builder = nullptr;
	REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("counter_n"), err) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	REQUIRE(duckdb_v2_table_function_builder_add_parameter(builder, bigint_type, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_add_named_parameter(builder, V2Str("start"), bigint_type, err) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&bigint_type);

	REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, counter_n_bind, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, counter_n_init_global, err) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, counter_n_exec, err) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_builder_destroy(&builder);
}

TEST_CASE("V2 table function: positional parameter", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) { register_counter_n(ctx, err); },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT val FROM counter_n(3)", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 3);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	for (idx_t i = 0; i < 3; i++) {
		idx_t idx = view.sel ? view.sel[i] : i;
		REQUIRE(data[idx] == static_cast<int64_t>(i));
	}

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

TEST_CASE("V2 table function: named parameter", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) { register_counter_n(ctx, err); },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT val FROM counter_n(4, start := 10)", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 4);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	for (idx_t i = 0; i < 4; i++) {
		idx_t idx = view.sel ? view.sel[i] : i;
		REQUIRE(data[idx] == static_cast<int64_t>(10 + i));
	}

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

TEST_CASE("V2 table function: parameter out of range", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    register_counter_n(ctx, err);

		    // Verify that get_parameter with an out-of-range index fails
		    // We can't test this directly inside a bind callback here, but we
		    // test it by calling the function with the wrong number of args
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT val FROM counter_n()", &result, nullptr) != DUCKDB_V2_ERROR_NONE);
	if (result) {
		duckdb_v2_result_destroy(&result);
	}
}

// ---------------------------------------------------------------------------
// User data test: builder user_data flows to bind and init callbacks.
// The function produces a single column whose value is read from user_data.
// ---------------------------------------------------------------------------

struct UserDataConfig {
	int64_t magic_value;
};

static void userdata_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                          duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_table_function_bind_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ud != nullptr);
	auto *cfg = static_cast<UserDataConfig *>(ud);

	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("val"), bigint_type, err);
	duckdb_v2_logical_type_destroy(&bigint_type);

	auto *bd = new int64_t(cfg->magic_value);
	duckdb_v2_table_function_bind_set_bind_data(info, {bd, [](void *p) { delete static_cast<int64_t *>(p); }, nullptr},
	                                            err);
}

static void userdata_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                                 duckdb_v2_error_info_handle *err) {
	void *ud = nullptr;
	REQUIRE(duckdb_v2_table_function_init_get_user_data(info, &ud, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ud != nullptr);
	auto *cfg = static_cast<UserDataConfig *>(ud);
	REQUIRE(cfg->magic_value == 42);

	auto *state = new int64_t(0);
	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<int64_t *>(p); }, nullptr}, err);
}

static void userdata_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                          duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	void *bind_data = nullptr;
	duckdb_v2_table_function_exec_get_bind_data(info, &bind_data, err);

	auto &emitted = *static_cast<int64_t *>(global_state);
	if (emitted > 0) {
		SetChunkSize(chunk, 0);
		return;
	}

	auto magic = *static_cast<int64_t *>(bind_data);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	int64_t *data = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
	data[0] = magic;
	SetChunkSize(chunk, 1);
	emitted = 1;
}

TEST_CASE("V2 table function: builder user_data flows to bind and init", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	auto *cfg = new UserDataConfig {42};

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *payload, duckdb_v2_error_info_handle *err) {
		    auto *cfg_ptr = static_cast<UserDataConfig *>(payload);

		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("userdata_fn"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_user_data(
		                builder, {cfg_ptr, [](void *p) { delete static_cast<UserDataConfig *>(p); }, nullptr}, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, userdata_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, userdata_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, userdata_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    cfg, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT val FROM userdata_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 1);

	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	idx_t idx = view.sel ? view.sel[0] : 0;
	REQUIRE(data[idx] == 42);

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// Projection pushdown test: 3-column function, query selects columns 0 and 2.
// The init callback verifies projected column indices.
// ---------------------------------------------------------------------------

struct ProjGlobalState {
	idx_t emitted;
	std::vector<idx_t> projected_originals;
};

static void proj_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle bigint_type = nullptr;
	duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("a"), bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("b"), bigint_type, err);
	duckdb_v2_table_function_bind_add_result_column(info, V2Str("c"), bigint_type, err);
	duckdb_v2_logical_type_destroy(&bigint_type);
}

static void proj_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                             duckdb_v2_error_info_handle *err) {
	idx_t col_count = 0;
	REQUIRE(duckdb_v2_table_function_init_get_column_count(info, &col_count, err) == DUCKDB_V2_ERROR_NONE);

	auto *state = new ProjGlobalState();
	state->emitted = 0;
	for (idx_t i = 0; i < col_count; i++) {
		idx_t orig = 0;
		REQUIRE(duckdb_v2_table_function_init_get_column_index(info, i, &orig, err) == DUCKDB_V2_ERROR_NONE);
		state->projected_originals.push_back(orig);
	}

	duckdb_v2_table_function_init_set_global_state(
	    info, {state, [](void *p) { delete static_cast<ProjGlobalState *>(p); }, nullptr}, err);
}

static void proj_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	auto &state = *static_cast<ProjGlobalState *>(global_state);
	if (state.emitted > 0) {
		SetChunkSize(chunk, 0);
		return;
	}

	for (idx_t i = 0; i < state.projected_originals.size(); i++) {
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, i, &vec, err);
		int64_t *data = nullptr;
		duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
		data[0] = static_cast<int64_t>(state.projected_originals[i] * 10);
	}
	SetChunkSize(chunk, 1);
	state.emitted = 1;
}

TEST_CASE("V2 table function: projection pushdown", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("proj_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_projection_pushdown(builder, true, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, proj_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, proj_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, proj_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT a, c FROM proj_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);

	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 1);

	idx_t col_count = 0;
	col_count = V2ColumnCount(result);
	REQUIRE(col_count == 2);

	duckdb_v2_vector_handle vec0 = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec0, nullptr);
	duckdb_v2_vector_view view0;
	duckdb_v2_vector_get_view(vec0, &view0, nullptr);
	auto *d0 = static_cast<const int64_t *>(view0.data);
	REQUIRE(d0[view0.sel ? view0.sel[0] : 0] == 0);

	duckdb_v2_vector_handle vec1 = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 1, &vec1, nullptr);
	duckdb_v2_vector_view view1;
	duckdb_v2_vector_get_view(vec1, &view1, nullptr);
	auto *d1 = static_cast<const int64_t *>(view1.data);
	REQUIRE(d1[view1.sel ? view1.sel[0] : 0] == 20);

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// Cardinality: the optimizer's estimated cardinality (visible in EXPLAIN as
// "~N rows") comes from either the cardinality callback or the static value
// set via bind_set_cardinality. A function with one BIGINT column, zero rows.
// ---------------------------------------------------------------------------

struct CardState {
	bool done = false;
};

static void card_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_logical_type_create_from_id(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &t, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_bind_add_result_column(info, V2Str("v"), t, err) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&t);
}

// Variant whose bind sets a static cardinality (no cardinality callback).
static void card_bind_static(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                             duckdb_v2_error_info_handle *err) {
	card_bind(info, ctx, err);
	duckdb_v2_table_function_bind_set_cardinality(info, 555555, true, err);
}

static void card_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                             duckdb_v2_error_info_handle *err) {
	auto *s = new CardState();
	duckdb_v2_table_function_init_set_global_state(
	    info, {s, [](void *p) { delete static_cast<CardState *>(p); }, nullptr}, err);
}

static void card_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	SetChunkSize(chunk, 0);
}

static void card_cb(void *bind_data, idx_t *out_estimated, bool *out_is_exact, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	*out_estimated = 777777;
	*out_is_exact = false;
}

// Exact-cardinality callback returning a value distinct from card_bind_static's
// 555555, used to pin (a) callback precedence over the static value and (b) the
// is_exact=true branch via the callback path.
static void card_cb_exact(void *bind_data, idx_t *out_estimated, bool *out_is_exact, duckdb_v2_context_handle ctx,
                          duckdb_v2_error_info_handle *err) {
	*out_estimated = 123456;
	*out_is_exact = true;
}

// Concatenate all VARCHAR cells of a query result into one string, stripping
// thousands separators so numeric matches are locale/format independent.
static std::string RunQueryText(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(conn, sql, &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t col_count = 0;
	col_count = V2ColumnCount(result);

	std::string out;
	while (auto chunk = V2StepChunk(result)) {
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		for (idx_t col = 0; col < col_count; col++) {
			duckdb_v2_vector_handle vec = nullptr;
			duckdb_v2_data_chunk_get_vector(chunk, col, &vec, nullptr);
			duckdb_v2_vector_view view;
			duckdb_v2_vector_get_view(vec, &view, nullptr);
			auto *data = static_cast<const duckdb_v2_varchar_t *>(view.data);
			for (idx_t i = 0; i < size; i++) {
				idx_t idx = view.sel ? view.sel[i] : i;
				if (!RowValid(view, idx)) {
					continue;
				}
				duckdb_v2_str s = {nullptr, 0};
				if (duckdb_v2_varchar_decode(&data[idx], &s, nullptr) == DUCKDB_V2_ERROR_NONE && s.ptr) {
					for (idx_t k = 0; k < s.len; k++) {
						if (s.ptr[k] != ',') {
							out.push_back(s.ptr[k]);
						}
					}
					out.push_back('\n');
				}
			}
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	duckdb_v2_result_destroy(&result);
	return out;
}

TEST_CASE("V2 table function: cardinality callback reaches the optimizer", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("card_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, card_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, card_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_cardinality_callback(builder, card_cb, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	auto plan = RunQueryText(fix.conn, "EXPLAIN SELECT * FROM card_fn()");
	REQUIRE(plan.find("777777") != std::string::npos);
}

TEST_CASE("V2 table function: bind_set_cardinality reaches the optimizer", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("card_static_fn"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind_static, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, card_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, card_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	auto plan = RunQueryText(fix.conn, "EXPLAIN SELECT * FROM card_static_fn()");
	REQUIRE(plan.find("555555") != std::string::npos);
}

TEST_CASE("V2 table function: cardinality callback overrides static value (exact)", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	// card_bind_static sets a static cardinality of 555555; card_cb_exact returns
	// 123456 with is_exact=true. The callback must win over the static value.
	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("card_both_fn"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind_static, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, card_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, card_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_cardinality_callback(builder, card_cb_exact, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	auto plan = RunQueryText(fix.conn, "EXPLAIN SELECT * FROM card_both_fn()");
	REQUIRE(plan.find("123456") != std::string::npos); // callback estimate wins
	REQUIRE(plan.find("555555") == std::string::npos); // static value is overridden
}

// Progress is timer/progress-bar driven and the materialized-only V2 result API
// exposes no progress accessor, so neither end-to-end invocation nor the bridge's
// 0.0-1.0 -> 0-100 scaling can be observed deterministically here: the scaled
// value only reaches DuckDB's internal ProgressData, which is not surfaced.
// (Dropping the *100 in ProgressCallback would not fail this test.) When a
// streaming/pending result with a progress accessor lands in V2, add a test that
// asserts the reported percentage. For now this exercises the registration +
// table_scan_progress wiring and confirms a query with a progress callback runs
// and returns correct results.
static void prog_cb(void *bind_data, void *global_state, double *out_progress, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	*out_progress = 0.5;
}

TEST_CASE("V2 table function: progress callback registers and runs", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("prog_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, counter_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, counter_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, counter_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_progress_callback(builder, prog_cb, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT count(*) FROM prog_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	REQUIRE(data[view.sel ? view.sel[0] : 0] == static_cast<int64_t>(COUNTER_NUM_ROWS));

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// init_local reads the global state set in init_global via
// init_get_global_state, derives per-thread local state from it, and exec
// emits the derived value — proving the global-state getter works.
// ---------------------------------------------------------------------------

struct GsGlobal {
	int64_t magic;
	bool done;
};

static void gs_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                           duckdb_v2_error_info_handle *err) {
	auto *g = new GsGlobal {99, false};
	duckdb_v2_table_function_init_set_global_state(
	    info, {g, [](void *p) { delete static_cast<GsGlobal *>(p); }, nullptr}, err);
}

static void gs_init_local(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                          duckdb_v2_error_info_handle *err) {
	void *gs = nullptr;
	REQUIRE(duckdb_v2_table_function_init_get_global_state(info, &gs, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(gs != nullptr);
	auto *g = static_cast<GsGlobal *>(gs);
	auto *l = new int64_t(g->magic + 1);
	duckdb_v2_table_function_init_set_local_state(info, {l, [](void *p) { delete static_cast<int64_t *>(p); }, nullptr},
	                                              err);
}

static void gs_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	void *local_state = nullptr;
	duckdb_v2_table_function_exec_get_local_state(info, &local_state, err);

	auto &g = *static_cast<GsGlobal *>(global_state);
	if (g.done) {
		SetChunkSize(chunk, 0);
		return;
	}
	auto val = *static_cast<int64_t *>(local_state);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	int64_t *data = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
	data[0] = val;
	SetChunkSize(chunk, 1);
	g.done = true;
}

TEST_CASE("V2 table function: init_local reads global state", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("gs_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, gs_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_local_callback(builder, gs_init_local, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, gs_exec, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT v FROM gs_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 1);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	REQUIRE(data[view.sel ? view.sel[0] : 0] == 100); // magic(99) + 1, derived in init_local

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// init_global can read back the global state it just set via
// init_set_global_state (set/get is symmetric within init_global, not only
// after init_local wires the read view). The value emitted by exec is obtained
// *through* get_global_state inside init_global, so the test fails if the
// getter returns null/wrong there.
// ---------------------------------------------------------------------------

struct GsgGlobal {
	int64_t magic;
	int64_t readback;
	bool done;
};

static void gsg_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                            duckdb_v2_error_info_handle *err) {
	auto *g = new GsgGlobal {42, -1, false};
	duckdb_v2_table_function_init_set_global_state(
	    info, {g, [](void *p) { delete static_cast<GsgGlobal *>(p); }, nullptr}, err);

	// Read it back within the same init_global callback.
	void *gs = nullptr;
	REQUIRE(duckdb_v2_table_function_init_get_global_state(info, &gs, err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(gs == g);
	g->readback = static_cast<GsgGlobal *>(gs)->magic;
}

static void gsg_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                     duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	auto &g = *static_cast<GsgGlobal *>(global_state);
	if (g.done) {
		SetChunkSize(chunk, 0);
		return;
	}
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	int64_t *data = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
	data[0] = g.readback;
	SetChunkSize(chunk, 1);
	g.done = true;
}

TEST_CASE("V2 table function: init_global reads back the global state it set", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("gsg_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, gsg_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, gsg_exec, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT v FROM gsg_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	chunk = V2StepChunk(result);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size == 1);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view;
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto *data = static_cast<const int64_t *>(view.data);
	REQUIRE(data[view.sel ? view.sel[0] : 0] == 42); // readback obtained via get_global_state in init_global

	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
}

// ---------------------------------------------------------------------------
// Complex filter pushdown: a function that claims `v = K` filters via the
// pushdown_complex_filter callback, extracting K with the expression API and
// emitting K+1. Because the function CONSUMES the filter, the engine does not
// re-apply it — so `WHERE v = 7` yields the row 8 (an engine-applied v=7 would
// have dropped the 8, proving the filter was genuinely pushed down).
// ---------------------------------------------------------------------------

struct PdBindData {
	bool handled;
	int64_t captured;
};

struct PdGlobal {
	bool done;
};

static void pd_bind(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	card_bind(info, ctx, err); // one BIGINT column "v"
	auto *bd = new PdBindData {false, 0};
	duckdb_v2_table_function_bind_set_bind_data(
	    info, {bd, [](void *p) { delete static_cast<PdBindData *>(p); }, nullptr}, err);
}

static void pd_init_global(duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
                           duckdb_v2_error_info_handle *err) {
	auto *g = new PdGlobal {false};
	duckdb_v2_table_function_init_set_global_state(
	    info, {g, [](void *p) { delete static_cast<PdGlobal *>(p); }, nullptr}, err);
}

static void pd_pushdown(void *bind_data, duckdb_v2_table_function_filter_info_handle info, duckdb_v2_context_handle ctx,
                        duckdb_v2_error_info_handle *err) {
	auto &bd = *static_cast<PdBindData *>(bind_data);

	idx_t count = 0;
	REQUIRE(duckdb_v2_table_function_filter_get_count(info, &count, err) == DUCKDB_V2_ERROR_NONE);

	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_expression_handle expr = nullptr;
		REQUIRE(duckdb_v2_table_function_filter_get_expression(info, i, &expr, err) == DUCKDB_V2_ERROR_NONE);

		DUCKDB_V2_EXPRESSION_TYPE type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
		duckdb_v2_expression_get_type(expr, &type, err);
		if (type != DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL) {
			continue;
		}

		idx_t nchild = 0;
		duckdb_v2_expression_get_child_count(expr, &nchild, err);
		if (nchild != 2) {
			continue;
		}

		// Identify the column-ref child and the constant child (either order).
		duckdb_v2_expression_handle col_expr = nullptr;
		duckdb_v2_expression_handle const_expr = nullptr;
		for (idx_t c = 0; c < 2; c++) {
			duckdb_v2_expression_handle child = nullptr;
			duckdb_v2_expression_get_child(expr, c, &child, err);
			DUCKDB_V2_EXPRESSION_CLASS cls = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
			duckdb_v2_expression_get_class(child, &cls, err);
			if (cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_COLUMN_REF) {
				col_expr = child;
			} else if (cls == DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONSTANT) {
				const_expr = child;
			}
		}
		if (!col_expr || !const_expr) {
			continue;
		}

		// The function has a single output column, so the binding must be column 0.
		idx_t col_index = 99;
		duckdb_v2_expression_get_column_binding(col_expr, nullptr, &col_index, err);
		REQUIRE(col_index == 0);

		duckdb_v2_value_handle val = nullptr;
		REQUIRE(duckdb_v2_expression_get_constant_value(const_expr, &val, err) == DUCKDB_V2_ERROR_NONE);
		int64_t k = V2LeafPayload<int64_t>(val);
		duckdb_v2_value_destroy(&val);

		bd.captured = k;
		bd.handled = true;
		REQUIRE(duckdb_v2_table_function_filter_mark_handled(info, i, err) == DUCKDB_V2_ERROR_NONE);
	}
}

static void pd_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                    duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err);

	void *global_state = nullptr;
	duckdb_v2_table_function_exec_get_global_state(info, &global_state, err);

	void *bind_data = nullptr;
	duckdb_v2_table_function_exec_get_bind_data(info, &bind_data, err);

	auto &bd = *static_cast<PdBindData *>(bind_data);
	auto &g = *static_cast<PdGlobal *>(global_state);
	if (g.done) {
		SetChunkSize(chunk, 0);
		return;
	}
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err);
	int64_t *data = nullptr;
	duckdb_v2_vector_get_data_mutable(vec, reinterpret_cast<void **>(&data), err);
	// Emit captured+1 when a filter was claimed (so the result proves the engine
	// did NOT re-apply it), or a sentinel when no filter reached us.
	data[0] = bd.handled ? bd.captured + 1 : -999;
	SetChunkSize(chunk, 1);
	g.done = true;
}

TEST_CASE("V2 table function: complex filter pushdown", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("pushdown_fn"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, pd_bind, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, pd_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, pd_exec, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback(builder, pd_pushdown, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	SECTION("filter is claimed and applied by the function") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT v FROM pushdown_fn() WHERE v = 7", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == 1);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		auto *data = static_cast<const int64_t *>(view.data);
		// captured(7)+1 = 8 survives ⇒ engine did not re-apply WHERE v=7 ⇒ pushed down.
		REQUIRE(data[view.sel ? view.sel[0] : 0] == 8);

		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&result);
	}

	SECTION("no filter reaches the callback for an unfiltered scan") {
		duckdb_v2_result_handle result = nullptr;
		REQUIRE(V2Query(fix.conn, "SELECT v FROM pushdown_fn()", &result, nullptr) == DUCKDB_V2_ERROR_NONE);

		duckdb_v2_data_chunk_handle chunk = nullptr;
		chunk = V2StepChunk(result);
		REQUIRE(chunk != nullptr);
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		REQUIRE(size == 1);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view;
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		auto *data = static_cast<const int64_t *>(view.data);
		REQUIRE(data[view.sel ? view.sel[0] : 0] == -999); // sentinel: no filter claimed

		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&result);
	}
}

// ---------------------------------------------------------------------------
// Filter info exposes the builder user_data, mirroring the other phase info
// handles: the pushdown callback reaches registration-scoped state without
// routing through bind data.
// ---------------------------------------------------------------------------

static int fud_marker = 0;
static void *g_fud_seen = nullptr;

static void fud_pushdown(void *bind_data, duckdb_v2_table_function_filter_info_handle info,
                         duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *err) {
	REQUIRE(duckdb_v2_table_function_filter_get_user_data(info, &g_fud_seen, err) == DUCKDB_V2_ERROR_NONE);
	// Null-arg misuse; pass no err slot so the deliberate failure does not
	// mark the callback as failed.
	void *dummy = nullptr;
	REQUIRE(duckdb_v2_table_function_filter_get_user_data(nullptr, &dummy, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
	REQUIRE(duckdb_v2_table_function_filter_get_user_data(info, nullptr, nullptr) == DUCKDB_V2_ERROR_INVALID_INPUT);
}

TEST_CASE("V2 table function: filter info exposes builder user_data", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	g_fud_seen = nullptr;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("fud_fn"), err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_user_data(builder, {&fud_marker, nullptr, nullptr}, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, card_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, card_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback(builder, fud_pushdown, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT v FROM fud_fn() WHERE v = 3", &result, nullptr) == DUCKDB_V2_ERROR_NONE);
	V2DrainRowCount(result);
	duckdb_v2_result_destroy(&result);

	REQUIRE(g_fud_seen == &fud_marker);
}

// ---------------------------------------------------------------------------
// Error-code fidelity: a callback that signals a specific V2 error code surfaces
// that code to the query, rather than being collapsed to a generic category.
// The exec callback below signals RESOURCE_OUT_OF_MEMORY; the old behavior threw
// a hardcoded InvalidInputException, so the query would have reported
// INVALID_INPUT. Now the thrown exception's type is derived from the signalled
// code and round-trips back to RESOURCE_OUT_OF_MEMORY.
// ---------------------------------------------------------------------------

static void errcode_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
                         duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY);
	duckdb_v2_error_info_set_text(*err, V2Str("synthetic OOM from exec"));
}

TEST_CASE("V2 table function: callback error code round-trips to the query", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder, V2Str("errcode_fn"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, card_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_init_global_callback(builder, card_init_global, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, errcode_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) == DUCKDB_V2_ERROR_NONE);
		    duckdb_v2_table_function_builder_destroy(&builder);
	    },
	    nullptr, nullptr);

	// The exec callback only runs once the result is stepped; the error
	// surfaces from the step, not from statement_execute.
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle qerr = nullptr;
	REQUIRE(V2Query(fix.conn, "SELECT v FROM errcode_fn()", &result, &qerr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_error_code_t rc = DUCKDB_V2_ERROR_NONE;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(result, &chunk, &status, &qerr);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		REQUIRE(status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED); // must fail before finishing
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY); // not collapsed to INVALID_INPUT
	REQUIRE(qerr != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(qerr, &msg);
	REQUIRE(V2StrTo(msg).find("synthetic OOM") != std::string::npos);
	duckdb_v2_error_info_destroy(&qerr);
	if (result) {
		duckdb_v2_result_destroy(&result);
	}
}

TEST_CASE("V2 table function: builder validation", "[capi_v2][table_function]") {
	V2EnvFixture fix;

	duckdb_v2_connection_execute_with_context(
	    fix.conn,
	    [](duckdb_v2_context_handle ctx, void *, duckdb_v2_error_info_handle *err) {
		    duckdb_v2_table_function_builder_handle builder = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder, err) == DUCKDB_V2_ERROR_NONE);

		    // Register without name should fail
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder, counter_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder, counter_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder, err) != DUCKDB_V2_ERROR_NONE);

		    // Register without exec callback should fail
		    duckdb_v2_table_function_builder_handle builder2 = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder2, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder2, V2Str("incomplete"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_bind_callback(builder2, counter_bind, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder2, err) != DUCKDB_V2_ERROR_NONE);

		    // Register without bind callback should fail
		    duckdb_v2_table_function_builder_handle builder3 = nullptr;
		    REQUIRE(duckdb_v2_table_function_builder_create(ctx, &builder3, err) == DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_name(builder3, V2Str("incomplete2"), err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_set_exec_callback(builder3, counter_exec, err) ==
		            DUCKDB_V2_ERROR_NONE);
		    REQUIRE(duckdb_v2_table_function_builder_register(ctx, builder3, err) != DUCKDB_V2_ERROR_NONE);

		    duckdb_v2_table_function_builder_destroy(&builder);
		    duckdb_v2_table_function_builder_destroy(&builder2);
		    duckdb_v2_table_function_builder_destroy(&builder3);
	    },
	    nullptr, nullptr);
}
