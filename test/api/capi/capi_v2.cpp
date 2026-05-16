#include "duckdb_v2.h"

typedef struct duckdb_extension *duckdb_extension_ptr;

static void my_func_bind(duckdb_v2_scalar_function_bind_info_ptr info, duckdb_v2_context_ptr ctx,
                         duckdb_v2_error_info_ptr err) {

	duckdb_v2_scalar_function_bind_set_data(info, (void *)0x1234, nullptr, nullptr, nullptr);

	duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_set_text(err, "This is a custom error message from my_func_bind.");
}

static void my_func_exec(duckdb_v2_scalar_function_invoke_info_ptr info, duckdb_v2_context_ptr ctx,
                         duckdb_v2_error_info_ptr err) {
	duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_set_text(err, "This is a custom error message from my_func_exec.");
}

static void my_func_worker(duckdb_v2_scalar_function_state_info_ptr info, duckdb_v2_context_ptr ctx,
                           duckdb_v2_error_info_ptr err) {
	duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_set_text(err, "This is a custom error message from my_func_worker.");
}

static void my_extension_duckdb_entry_v2(duckdb_extension_ptr loader, duckdb_v2_context_ptr ctx,
                                         duckdb_v2_error_info_ptr err) {
	duckdb_v2_scalar_function_ptr builder = NULL;

	if (duckdb_v2_scalar_function_create(ctx, err, &builder) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	if (duckdb_v2_scalar_function_set_bind_callback(builder, my_func_bind) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	if (duckdb_v2_scalar_function_set_invoke_callback(builder, my_func_exec) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	if (duckdb_v2_scalar_function_set_state_callback(builder, my_func_worker) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	/*
	if (duckdb_v2_scalar_function_builder_set_signature(builder, "(a INT, b INT := 5) -> INT", err) !=
	    DUCKDB_V2_ERROR_NONE) {
	    return;
	}
	*/

	duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR_INVALID_INPUT);
	duckdb_v2_error_info_set_text(err, "This is a custom error message from my_extension.");
	return;
}
