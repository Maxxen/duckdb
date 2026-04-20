#include "duckdb_v2.h"

#include <stdio.h>

static void my_bind(duckdb_ctx_ptr context, duckdb_scalar_func_bind_args_ptr args) {
    // do some binding
}

static void my_init(duckdb_ctx_ptr context, duckdb_scalar_func_init_args_ptr args) {
    // do some initialization
}

static void my_exec(duckdb_ctx_ptr context, duckdb_scalar_func_exec_args_ptr args) {

    // Setup all the data we need
    duckdb_data_chunk input = NULL;
    duckdb_vector lhs_input = NULL;
    duckdb_vector rhs_input = NULL;
    duckdb_vector result = NULL;
    uint64_t row_count = 0;
    void* lhs_data_ptr;
    void* rhs_data_ptr;
    void* result_data_ptr;

    if(!duckdb_scalar_function_exec_get_input(context, args, input)){
        return;
    }
    if(!duckdb_scalar_function_exec_get_result(context, args, result)){
        return;
    }
    if (!duckdb_data_chunk_get_vector(context, input, 0, &lhs_input)) {
        return;
    }
    if (!duckdb_data_chunk_get_vector(context, input, 1, &rhs_input)) {
        return;
    }
    if (!duckdb_data_chunk_get_size(context, input, &row_count)) {
        return;
    }
    if (!duckdb_vector_get_data(context, lhs_input, &lhs_data_ptr)) {
        return;
    }
    if (!duckdb_vector_get_data(context, rhs_input, &rhs_data_ptr)) {
        return;
    }
    if (!duckdb_vector_get_data(context, result, &result_data_ptr)) {
        return;
    }

    int32_t* lhs_data = (int32_t*)lhs_data_ptr;
    int32_t* rhs_data = (int32_t*)rhs_data_ptr;
    int32_t* result_data = (int32_t*)result_data_ptr;

    // do some execution
    for (uint64_t row_idx = 0; row_idx < row_count; row_idx++) {

        int32_t lhs_value = lhs_data[row_idx];
        int32_t rhs_value = rhs_data[row_idx];

        if (lhs_value < 0 || rhs_value < 0) {

            // Set an error on the context and return early if we encounter a negative value
            duckdb_error err = NULL;
            if (!duckdb_error_create(&err)) {
                return;
            }

            duckdb_error_set_code(err, DUCKDB_ERROR_INVALID_INPUT);
            duckdb_error_set_text(err, "negative result not allowed");
            duckdb_context_set_error(context, err);

            duckdb_error_destroy(&err);
            return;
        }

        // Otherwise, compute the result
        result_data[row_idx] = lhs_value + rhs_value;
    }
}

static void print_error(duckdb_context context) {

    duckdb_error error = NULL;
    duckdb_context_get_error(context, &error);

    if (error) {
        const char *text;
        duckdb_error_code code;

        duckdb_error_get_text(error, &text);
        duckdb_error_get_code(error, &code);

        printf("Error (%d): %s\n", code, text);

        duckdb_error_destroy(&error);
    }
}

void on_load(duckdb_context context) {

    duckdb_scalar_function function = NULL;

    if (!duckdb_scalar_function_create(context, &function)) {
        goto cleanup;
    }

    if (!duckdb_scalar_function_set_name(context, function, "my_func")) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_set_signature(context, function, "(INTEGER, INTEGER) -> INTEGER")) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_set_on_conflict(context, function, DUCKDB_ON_CONFLICT_ERROR)) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_set_bind_callback(context, function, my_bind)) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_set_init_callback(context, function, my_init)) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_set_exec_callback(context, function, my_exec)) {
        goto cleanup;
    }
    if (!duckdb_scalar_function_register(context, function)) {
        goto cleanup;
    }

    // Return normally here
    cleanup:

        // Check for errors
        if (duckdb_context_has_error(context)) {
            print_error(context);
        }

        duckdb_scalar_function_destroy(context, &function);
}

int main() {

    // Get context from somewhere
    duckdb_context context;
    on_load(context);
}