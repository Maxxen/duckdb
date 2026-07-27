#pragma once

#include "duckdb/main/capi_v2/duckdb_v2_engine.hpp"

//===--------------------------------------------------------------------===//
// Function pointer struct
//===--------------------------------------------------------------------===//
typedef struct {
	// v2.0.0
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_set_code)(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR code);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_set_text)(duckdb_v2_error_info_handle info, duckdb_v2_str text);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_destroy)(duckdb_v2_error_info_handle *info);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_create)
	(duckdb_v2_cast_function_builder_handle *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_set_source_type)
	(duckdb_v2_cast_function_builder_handle func, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_set_target_type)
	(duckdb_v2_cast_function_builder_handle func, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_set_implicit_cast_cost)
	(duckdb_v2_cast_function_builder_handle func, int64_t cost, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_set_exec_callback)
	(duckdb_v2_cast_function_builder_handle func, duckdb_v2_cast_function_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_set_user_data)
	(duckdb_v2_cast_function_builder_handle func, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_cast_function_builder_handle func, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_cast_function_builder_handle func,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_cast_function_builder_destroy)(duckdb_v2_cast_function_builder_handle *func);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_user_data)
	(duckdb_v2_cast_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_input)
	(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_vector_handle *out_input,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_output)
	(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_vector_handle *out_output,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_count)
	(duckdb_v2_cast_function_exec_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_mode)
	(duckdb_v2_cast_function_exec_info_handle info, DUCKDB_V2_CAST_MODE *out_mode, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_create_with_connection)
	(duckdb_v2_connection_handle conn, const duckdb_v2_logical_type_handle *types_array, idx_t types_count,
	 duckdb_v2_column_data_collection_handle *out_collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_create_with_context)
	(duckdb_v2_context_handle context, const duckdb_v2_logical_type_handle *types_array, idx_t types_count,
	 duckdb_v2_column_data_collection_handle *out_collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_column_data_collection_destroy)(duckdb_v2_column_data_collection_handle *collection);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_combine)
	(duckdb_v2_column_data_collection_handle target, duckdb_v2_column_data_collection_handle *source,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_reset)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_row_count)
	(duckdb_v2_column_data_collection_handle collection, idx_t *out_row_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_append_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append_state_destroy)
	(duckdb_v2_column_data_collection_append_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_column_data_collection_append_state_handle state,
	 duckdb_v2_data_chunk_handle chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_scan_state_create)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_column_data_collection_scan_state_handle *out_state,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_scan_state_destroy)
	(duckdb_v2_column_data_collection_scan_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_scan)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_column_data_collection_scan_state_handle state,
	 duckdb_v2_data_chunk_handle out_chunk, bool *did_produce_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_shared_scan_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_shared_scan_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_shared_scan_state_destroy)
	(duckdb_v2_column_data_collection_shared_scan_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_worker_scan_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_worker_scan_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_worker_scan_state_destroy)
	(duckdb_v2_column_data_collection_worker_scan_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_parallel_scan)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_shared_scan_state_handle shared_state,
	 duckdb_v2_column_data_collection_worker_scan_state_handle worker_state, duckdb_v2_data_chunk_handle out_chunk,
	 bool *did_produce_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_create)
	(duckdb_v2_identifier_t name, duckdb_v2_str setting, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_option_destroy)(duckdb_v2_option_handle *option);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_name)
	(duckdb_v2_option_handle option, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_setting)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_setting, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_default_setting)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_default_setting, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_description)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_description, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_target_scope)
	(duckdb_v2_option_handle option, DUCKDB_V2_OPTION_TARGET_SCOPE *out_target_scope, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_alias_count)
	(duckdb_v2_option_handle option, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_alias)
	(duckdb_v2_option_handle option, idx_t index, duckdb_v2_identifier_t *out_alias, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_create_with_alias)
	(duckdb_v2_logical_type_handle base_type, duckdb_v2_identifier_t alias_name,
	 duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_builder_create)
	(duckdb_v2_custom_type_builder_handle *out_builder, duckdb_v2_error_info_handle *err);
	void (*duckdb_v2_custom_type_builder_destroy)(duckdb_v2_custom_type_builder_handle *builder);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_custom_type_builder_handle builder, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_custom_type_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_builder_set_name)
	(duckdb_v2_custom_type_builder_handle builder, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_builder_set_base_type)
	(duckdb_v2_custom_type_builder_handle builder, duckdb_v2_logical_type_handle base_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_create)
	(const duckdb_v2_logical_type_handle *types, idx_t column_count, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_data_chunk_destroy)(duckdb_v2_data_chunk_handle *chunk);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_size)
	(duckdb_v2_data_chunk_handle chunk, idx_t *out_size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_vector_count)
	(duckdb_v2_data_chunk_handle chunk, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_vector)
	(duckdb_v2_data_chunk_handle chunk, idx_t index, duckdb_v2_vector_handle *out_vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_open)
	(duckdb_v2_environment_handle env, duckdb_v2_str path, duckdb_v2_option_handle *options, idx_t option_count,
	 duckdb_v2_database_handle *out_db, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_close)(duckdb_v2_database_handle *db);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_set)
	(duckdb_v2_database_handle db, duckdb_v2_option_handle option, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get)
	(duckdb_v2_database_handle db, duckdb_v2_identifier_t name, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get_count)
	(duckdb_v2_database_handle db, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get_by_index)
	(duckdb_v2_database_handle db, idx_t index, duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_library_version)(char **out_version, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_create_environment)
	(duckdb_v2_environment_handle *out_env, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_destroy_environment)(duckdb_v2_environment_handle *env);
	DUCKDB_V2_ERROR(*duckdb_v2_environment_database_count)
	(duckdb_v2_environment_handle env, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_get_code)(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR *out_code);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_get_text)(duckdb_v2_error_info_handle info, duckdb_v2_str *out_text);
	DUCKDB_V2_ERROR(*duckdb_v2_error_info_get_raw_message)
	(duckdb_v2_error_info_handle info, duckdb_v2_str *out_raw_message);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_class)
	(duckdb_v2_expression_handle expression, DUCKDB_V2_EXPRESSION_CLASS *out_class, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_type)
	(duckdb_v2_expression_handle expression, DUCKDB_V2_EXPRESSION_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_return_type)
	(duckdb_v2_expression_handle expression, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_child_count)
	(duckdb_v2_expression_handle expression, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_child)
	(duckdb_v2_expression_handle expression, idx_t index, duckdb_v2_expression_handle *out_child,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_function_name)
	(duckdb_v2_expression_handle expression, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_constant_value)
	(duckdb_v2_expression_handle expression, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_column_binding)
	(duckdb_v2_expression_handle expression, idx_t *out_table_index, idx_t *out_column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_reference_index)
	(duckdb_v2_expression_handle expression, idx_t *out_index, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_get_from_context)
	(duckdb_v2_context_handle context, duckdb_v2_file_system_handle *out_file_system, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_get_from_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_file_system_handle *out_file_system,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_open)
	(duckdb_v2_file_system_handle file_system, duckdb_v2_str file_path, uint64_t file_flags,
	 duckdb_v2_file_handle_handle *out_file_handle, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_read)
	(duckdb_v2_file_handle_handle file_handle, void *buffer, int64_t buffer_size, int64_t *bytes_read,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_write)
	(duckdb_v2_file_handle_handle file_handle, const void *buffer, int64_t buffer_size, int64_t *bytes_written,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_tell)
	(duckdb_v2_file_handle_handle file_handle, int64_t *position, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_size)
	(duckdb_v2_file_handle_handle file_handle, int64_t *size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_seek)
	(duckdb_v2_file_handle_handle file_handle, int64_t position, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_sync)
	(duckdb_v2_file_handle_handle file_handle, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_handle_close)
	(duckdb_v2_file_handle_handle file_handle, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_handle_destroy)(duckdb_v2_file_handle_handle *file_handle);
	DUCKDB_V2_ERROR(*duckdb_v2_bind_arguments_get_count)
	(duckdb_v2_bind_arguments_handle args, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bind_arguments_get_type)
	(duckdb_v2_bind_arguments_handle args, idx_t index, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bind_arguments_get_name)
	(duckdb_v2_bind_arguments_handle args, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bind_arguments_fold)
	(duckdb_v2_bind_arguments_handle args, duckdb_v2_context_handle ctx, idx_t index, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_identifier_render_quoted)
	(duckdb_v2_identifier_t name, char **out_text, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_create)
	(duckdb_v2_log_storage_builder_handle *out_builder, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_set_name)
	(duckdb_v2_log_storage_builder_handle builder, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_set_user_data)
	(duckdb_v2_log_storage_builder_handle builder, duckdb_v2_opaque user_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_set_log_callback)
	(duckdb_v2_log_storage_builder_handle builder, duckdb_v2_log_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_register_with_database)
	(duckdb_v2_database_handle db, duckdb_v2_log_storage_builder_handle builder, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_log_storage_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_log_storage_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_log_storage_builder_destroy)(duckdb_v2_log_storage_builder_handle *builder);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_log)
	(duckdb_v2_connection_handle conn, DUCKDB_V2_LOG_LEVEL level, duckdb_v2_str message,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_log)
	(duckdb_v2_context_handle context, DUCKDB_V2_LOG_LEVEL level, duckdb_v2_str message,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_create_from_id)
	(DUCKDB_V2_LOGICAL_TYPE_ID type_id, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_create_from_text)
	(duckdb_v2_context_handle ctx, duckdb_v2_str text, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_from_text)
	(duckdb_v2_connection_handle conn, duckdb_v2_str text, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_create_from_args)
	(duckdb_v2_context_handle ctx, duckdb_v2_identifier_t name, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_from_args)
	(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_copy)
	(duckdb_v2_logical_type_handle type, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_logical_type_destroy)(duckdb_v2_logical_type_handle *type);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_is_equal)
	(duckdb_v2_logical_type_handle left, duckdb_v2_logical_type_handle right, bool *result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_id)
	(duckdb_v2_logical_type_handle type, DUCKDB_V2_LOGICAL_TYPE_ID *out_id, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_name)
	(duckdb_v2_logical_type_handle type, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_to_text)
	(duckdb_v2_logical_type_handle type, char **out_text, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_param_count)
	(duckdb_v2_logical_type_handle type, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_param)
	(duckdb_v2_logical_type_handle type, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_parse)
	(duckdb_v2_str text, duckdb_v2_qname_handle *out_qname, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_create)
	(const duckdb_v2_identifier_t *parts, idx_t part_count, duckdb_v2_qname_handle *out_qname,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_get_part_count)
	(duckdb_v2_qname_handle qname, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_get_part)
	(duckdb_v2_qname_handle qname, idx_t index, duckdb_v2_identifier_t *out_part, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_render)
	(duckdb_v2_qname_handle qname, char **out_text, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_equals)
	(duckdb_v2_qname_handle left, duckdb_v2_qname_handle right, bool *result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_hash)
	(duckdb_v2_qname_handle qname, uint64_t *out_hash, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_qname_destroy)(duckdb_v2_qname_handle *qname);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_register_with_database)
	(duckdb_v2_database_handle db, duckdb_v2_replacement_scan_callback_fn callback, duckdb_v2_opaque user_data,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_replacement_scan_callback_fn callback, duckdb_v2_opaque user_data,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_catalog_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_schema_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_table_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_user_data)
	(duckdb_v2_replacement_scan_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_function_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_add_parameter)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_add_named_parameter)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t name, duckdb_v2_value_handle value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_schema_get_count)
	(duckdb_v2_schema_handle schema, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_schema_get_field)
	(duckdb_v2_schema_handle schema, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_schema_destroy)(duckdb_v2_schema_handle *schema);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_create)
	(duckdb_v2_function_signature_handle *out_sig, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_function_signature_destroy)(duckdb_v2_function_signature_handle *sig);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_add_parameter)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_identifier_t name, duckdb_v2_logical_type_handle type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_add_parameter_default)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_identifier_t name, duckdb_v2_logical_type_handle type,
	 duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_set_varargs)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_set_return_type)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_parameter_count)
	(duckdb_v2_function_signature_handle sig, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_parameter_name)
	(duckdb_v2_function_signature_handle sig, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_parameter_type)
	(duckdb_v2_function_signature_handle sig, idx_t index, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_parameter_has_default)
	(duckdb_v2_function_signature_handle sig, idx_t index, bool *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_parameter_default)
	(duckdb_v2_function_signature_handle sig, idx_t index, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_has_varargs)
	(duckdb_v2_function_signature_handle sig, bool *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_varargs)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_has_return_type)
	(duckdb_v2_function_signature_handle sig, bool *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_get_return_type)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_string_heap_allocate)
	(duckdb_v2_string_heap_handle heap, idx_t byte_len, uint8_t **out_ptr, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_value_destroy)(duckdb_v2_value_handle *value);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_null)
	(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_from_data)
	(duckdb_v2_logical_type_handle type, const void *data, idx_t len, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_data)
	(duckdb_v2_value_handle value, const void **out_data, idx_t *out_len, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bignum)
	(const uint8_t *data, idx_t length, bool is_negative, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_bignum)
	(duckdb_v2_value_handle value, uint8_t **out_data, idx_t *out_length, bool *out_is_negative,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_variant)
	(duckdb_v2_value_handle value, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_type)
	(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_type)
	(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create)
	(duckdb_v2_logical_type_handle type, const duckdb_v2_value_handle *children, idx_t child_count,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_child_count)
	(duckdb_v2_value_handle value, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_child)
	(duckdb_v2_value_handle value, idx_t index, duckdb_v2_value_handle *out_child, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_cast_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_value_handle value, duckdb_v2_logical_type_handle target_type,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_cast_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_value_handle value, duckdb_v2_logical_type_handle target_type,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_is_null)
	(duckdb_v2_value_handle value, bool *out_is_null, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_logical_type)
	(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_to_string)
	(duckdb_v2_value_handle value, char **out_string, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_logical_type)
	(duckdb_v2_vector_handle vector, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_vector_type)
	(duckdb_v2_vector_handle vector, DUCKDB_V2_VECTOR_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_view)
	(duckdb_v2_vector_handle vector, duckdb_v2_vector_view *out_view, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_size)
	(duckdb_v2_vector_handle vector, idx_t *out_size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_size)
	(duckdb_v2_vector_handle vector, idx_t size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_value)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_value)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_data_mutable)
	(duckdb_v2_vector_handle vector, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_vector_flatten)(duckdb_v2_vector_handle vector, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_reference)
	(duckdb_v2_vector_handle vector, duckdb_v2_vector_handle source, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_make_constant)
	(duckdb_v2_vector_handle vector, duckdb_v2_value_handle value, idx_t count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_make_sequence)
	(duckdb_v2_vector_handle vector, int64_t start, int64_t increment, idx_t count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_null)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_flat_get_validity_mutable)
	(duckdb_v2_vector_handle vector, uint64_t **out_validity, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_constant_set_valid)
	(duckdb_v2_vector_handle vector, bool validity, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_string_heap)
	(duckdb_v2_vector_handle vector, duckdb_v2_string_heap_handle *out_heap, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_child_count)
	(duckdb_v2_vector_handle vector, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_child)
	(duckdb_v2_vector_handle vector, idx_t index, duckdb_v2_vector_handle *out_child, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bignum_decode)
	(const duckdb_v2_bignum_t *bignum, uint8_t **out_data, idx_t *out_length, bool *out_is_negative,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_create)
	(duckdb_v2_aggregate_function_builder_handle *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_destroy)
	(duckdb_v2_aggregate_function_builder_handle *builder);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_name)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_identifier_t name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_signature)
	(duckdb_v2_aggregate_function_builder_handle func, duckdb_v2_function_signature_handle sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_property)
	(duckdb_v2_aggregate_function_builder_handle func, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_get_property)
	(duckdb_v2_aggregate_function_builder_handle func, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_bind_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_size_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_size_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_init_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_update_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_update_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_combine_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_combine_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_finalize_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_finalize_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_destroy_callback)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_aggregate_function_destroy_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_set_user_data)
	(duckdb_v2_aggregate_function_builder_handle builder, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_aggregate_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_aggregate_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_function_name)
	(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_user_data)
	(duckdb_v2_aggregate_function_bind_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_arguments)
	(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_bind_arguments_handle *out_arguments,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_set_bind_data)
	(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_size_get_user_data)
	(duckdb_v2_aggregate_function_size_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_size_set_size)
	(duckdb_v2_aggregate_function_size_info_handle info, idx_t size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_user_data)
	(duckdb_v2_aggregate_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_state)
	(duckdb_v2_aggregate_function_init_info_handle info, void **out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_user_data)
	(duckdb_v2_aggregate_function_update_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_bind_data)
	(duckdb_v2_aggregate_function_update_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_count)
	(duckdb_v2_aggregate_function_update_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_input)
	(duckdb_v2_aggregate_function_update_info_handle info, duckdb_v2_data_chunk_handle *out_input,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_states)
	(duckdb_v2_aggregate_function_update_info_handle info, void ***out_states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_user_data)
	(duckdb_v2_aggregate_function_combine_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_bind_data)
	(duckdb_v2_aggregate_function_combine_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_count)
	(duckdb_v2_aggregate_function_combine_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_sources)
	(duckdb_v2_aggregate_function_combine_info_handle info, void ***out_sources, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_targets)
	(duckdb_v2_aggregate_function_combine_info_handle info, void ***out_targets, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_user_data)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_bind_data)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_count)
	(duckdb_v2_aggregate_function_finalize_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_states)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void ***out_states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_result)
	(duckdb_v2_aggregate_function_finalize_info_handle info, duckdb_v2_vector_handle *out_result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_result_offset)
	(duckdb_v2_aggregate_function_finalize_info_handle info, idx_t *out_offset, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_user_data)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_bind_data)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_count)
	(duckdb_v2_aggregate_function_destroy_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_states)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void ***out_states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_to_arrow_stream)
	(duckdb_v2_result_handle *result, idx_t batch_size, struct ArrowArrayStream *out_stream,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_types_to_arrow_schema)
	(duckdb_v2_context_handle context, const duckdb_v2_logical_type_handle *types, const duckdb_v2_str *names,
	 idx_t count, struct ArrowSchema *out_schema, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_to_arrow_array)
	(duckdb_v2_context_handle context, duckdb_v2_data_chunk_handle chunk, struct ArrowArray *out_array,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_conversion_plan_create)
	(duckdb_v2_context_handle context, struct ArrowSchema *schema, duckdb_v2_arrow_conversion_plan_handle *out_plan,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_array_to_data_chunk)
	(duckdb_v2_context_handle context, struct ArrowArray *array, duckdb_v2_arrow_conversion_plan_handle plan,
	 duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_conversion_plan_get_schema)
	(duckdb_v2_arrow_conversion_plan_handle plan, duckdb_v2_schema_handle *out_schema,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_arrow_conversion_plan_destroy)(duckdb_v2_arrow_conversion_plan_handle *plan);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_create)
	(duckdb_v2_connection_handle conn, duckdb_v2_qname_handle name, duckdb_v2_table_description_handle *out_desc,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_get_qname)
	(duckdb_v2_table_description_handle desc, duckdb_v2_qname_handle *out_qname, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_get_schema)
	(duckdb_v2_table_description_handle desc, duckdb_v2_schema_handle *out_schema, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_column_is_generated)
	(duckdb_v2_table_description_handle desc, idx_t index, bool *out_is_generated, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_column_has_default)
	(duckdb_v2_table_description_handle desc, idx_t index, bool *out_has_default, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_is_readonly)
	(duckdb_v2_table_description_handle desc, bool *out_readonly, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_table_description_destroy)(duckdb_v2_table_description_handle *desc);
	DUCKDB_V2_ERROR(*duckdb_v2_connect)
	(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_disconnect)(duckdb_v2_connection_handle *conn);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_set)
	(duckdb_v2_connection_handle conn, duckdb_v2_option_handle option, DUCKDB_V2_SETTING_SCOPE scope,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get)
	(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get_count)
	(duckdb_v2_connection_handle conn, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get_by_index)
	(duckdb_v2_connection_handle conn, idx_t index, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_create_extension)
	(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_interrupt)
	(duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_query_progress)
	(duckdb_v2_connection_handle conn, duckdb_v2_query_progress_handle *out_progress, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_percentage)
	(duckdb_v2_query_progress_handle progress, double *out_percentage, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_rows_processed)
	(duckdb_v2_query_progress_handle progress, uint64_t *out_rows_processed, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_total_rows_to_process)
	(duckdb_v2_query_progress_handle progress, uint64_t *out_total_rows_to_process, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_query_progress_destroy)(duckdb_v2_query_progress_handle *progress);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_create)
	(duckdb_v2_copy_function_builder_handle *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_name)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_bind_callback)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_copy_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_init_callback)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_copy_function_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_batch_callback)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_copy_function_batch_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_flush_callback)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_copy_function_flush_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_finalize_callback)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_copy_function_finalize_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_set_user_data)
	(duckdb_v2_copy_function_builder_handle builder, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_copy_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_copy_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_copy_function_builder_destroy)(duckdb_v2_copy_function_builder_handle *builder);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_bind_get_user_data)
	(duckdb_v2_copy_function_bind_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_bind_get_column_count)
	(duckdb_v2_copy_function_bind_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_bind_get_column_type)
	(duckdb_v2_copy_function_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_bind_get_column_name)
	(duckdb_v2_copy_function_bind_info_handle info, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_bind_set_bind_data)
	(duckdb_v2_copy_function_bind_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_init_get_user_data)
	(duckdb_v2_copy_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_init_get_bind_data)
	(duckdb_v2_copy_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_init_get_file_path)
	(duckdb_v2_copy_function_init_info_handle info, duckdb_v2_str *out_path, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_init_set_init_data)
	(duckdb_v2_copy_function_init_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_batch_get_user_data)
	(duckdb_v2_copy_function_batch_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_batch_get_bind_data)
	(duckdb_v2_copy_function_batch_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_batch_get_init_data)
	(duckdb_v2_copy_function_batch_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_batch_get_input)
	(duckdb_v2_copy_function_batch_info_handle info, duckdb_v2_column_data_collection_handle *out_input,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_batch_set_batch_data)
	(duckdb_v2_copy_function_batch_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_flush_get_user_data)
	(duckdb_v2_copy_function_flush_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_flush_get_bind_data)
	(duckdb_v2_copy_function_flush_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_flush_get_init_data)
	(duckdb_v2_copy_function_flush_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_flush_get_batch_data)
	(duckdb_v2_copy_function_flush_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_finalize_get_user_data)
	(duckdb_v2_copy_function_finalize_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_finalize_get_bind_data)
	(duckdb_v2_copy_function_finalize_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_finalize_get_init_data)
	(duckdb_v2_copy_function_finalize_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_create)
	(duckdb_v2_scalar_function_builder_handle *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_name)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_bind_callback)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_scalar_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_init_callback)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_scalar_function_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_exec_callback)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_scalar_function_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_scalar_function_builder_handle func, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_scalar_function_builder_handle func,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_scalar_function_builder_destroy)(duckdb_v2_scalar_function_builder_handle *func);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_user_data)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_signature)
	(duckdb_v2_scalar_function_builder_handle func, duckdb_v2_function_signature_handle sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_set_property)
	(duckdb_v2_scalar_function_builder_handle func, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_builder_get_property)
	(duckdb_v2_scalar_function_builder_handle func, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_function_name)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_user_data)
	(duckdb_v2_scalar_function_bind_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_arguments)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_bind_arguments_handle *out_arguments,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_set_bind_data)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_set_return_type)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_logical_type_handle type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_get_function_name)
	(duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_get_user_data)
	(duckdb_v2_scalar_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_get_bind_data)
	(duckdb_v2_scalar_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_set_init_data)
	(duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_function_name)
	(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_user_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_bind_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_init_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_input)
	(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_data_chunk_handle *out_input,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_result)
	(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_vector_handle *out_result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_parse_sql)
	(duckdb_v2_connection_handle conn, const char *sql, duckdb_v2_statement_iterator_handle *out_iterator,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_iterator_next)
	(duckdb_v2_statement_iterator_handle iterator, duckdb_v2_sql_statement_handle *out_statement,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_bind)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement, duckdb_v2_schema_handle *out_schema,
	 duckdb_v2_schema_handle *out_parameters, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_add_collection)
	(duckdb_v2_sql_statement_handle statement, duckdb_v2_identifier_t name,
	 duckdb_v2_column_data_collection_handle collection, const duckdb_v2_identifier_t *column_names, idx_t column_count,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_sql_statement_destroy)(duckdb_v2_sql_statement_handle *statement);
	DUCKDB_V2_ERROR (*duckdb_v2_statement_iterator_destroy)(duckdb_v2_statement_iterator_handle *iterator);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_create)
	(duckdb_v2_table_function_builder_handle *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_table_function_builder_destroy)(duckdb_v2_table_function_builder_handle *builder);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_name)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_signature)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_function_signature_handle sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_user_data)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_bind_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_bind_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_init_global_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_init_global_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_init_local_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_init_local_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_exec_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_exec_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_cardinality_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_cardinality_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_progress_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_progress_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback)
	(duckdb_v2_table_function_builder_handle builder, duckdb_v2_table_function_pushdown_complex_filter_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_set_projection_pushdown)
	(duckdb_v2_table_function_builder_handle builder, bool enable, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_register_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_table_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_builder_register_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_table_function_builder_handle builder,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_add_result_column)
	(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_identifier_t name, duckdb_v2_logical_type_handle type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_arguments)
	(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_bind_arguments_handle *out_arguments,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_set_bind_data)
	(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_set_cardinality)
	(duckdb_v2_table_function_bind_info_handle info, idx_t cardinality, bool is_exact,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_user_data)
	(duckdb_v2_table_function_bind_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_get_bind_data)
	(duckdb_v2_table_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_set_global_state)
	(duckdb_v2_table_function_init_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_get_global_state)
	(duckdb_v2_table_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_set_local_state)
	(duckdb_v2_table_function_init_info_handle info, duckdb_v2_opaque data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_set_max_threads)
	(duckdb_v2_table_function_init_info_handle info, idx_t max_threads, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_get_column_count)
	(duckdb_v2_table_function_init_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_get_column_index)
	(duckdb_v2_table_function_init_info_handle info, idx_t projected_index, idx_t *out_original_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_get_user_data)
	(duckdb_v2_table_function_init_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_get_count)
	(duckdb_v2_table_function_filter_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_get_expression)
	(duckdb_v2_table_function_filter_info_handle info, idx_t index, duckdb_v2_expression_handle *out_expression,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_mark_handled)
	(duckdb_v2_table_function_filter_info_handle info, idx_t index, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_get_column_count)
	(duckdb_v2_table_function_filter_info_handle info, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_get_column_index)
	(duckdb_v2_table_function_filter_info_handle info, idx_t index, idx_t *out_column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_get_user_data)
	(duckdb_v2_table_function_filter_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_bind_data)
	(duckdb_v2_table_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_global_state)
	(duckdb_v2_table_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_local_state)
	(duckdb_v2_table_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_output_chunk)
	(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_user_data)
	(duckdb_v2_table_function_exec_info_handle info, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_prepare)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement, bool require_cacheable,
	 duckdb_v2_prepared_statement_handle *out_prepared, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_prepared_execute)
	(duckdb_v2_prepared_statement_handle prepared, const duckdb_v2_identifier_t *parameter_names,
	 const duckdb_v2_value_handle *parameter_values, idx_t parameter_count, duckdb_v2_result_handle *out_result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_prepared_reuses_plan)
	(duckdb_v2_prepared_statement_handle prepared, bool *out_reuses, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_prepared_statement_destroy)(duckdb_v2_prepared_statement_handle *prepared);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_execute)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement,
	 const duckdb_v2_identifier_t *parameter_names, const duckdb_v2_value_handle *parameter_values,
	 idx_t parameter_count, duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_result_destroy)(duckdb_v2_result_handle *result);
	DUCKDB_V2_ERROR(*duckdb_v2_result_step)
	(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk, DUCKDB_V2_RESULT_STEP_STATUS *out_status,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_fetch_chunk)
	(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_result_wait)(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_render_box)
	(duckdb_v2_result_handle *result, idx_t max_rows, idx_t max_width, idx_t max_col_width, duckdb_v2_str null_value,
	 idx_t render_mode, idx_t limit, char **out_text, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_drain)
	(duckdb_v2_result_handle result, idx_t *out_rows_changed, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_result_type)
	(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_statement_type)
	(duckdb_v2_result_handle result, DUCKDB_V2_STATEMENT_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_schema)
	(duckdb_v2_result_handle result, duckdb_v2_schema_handle *out_schema, duckdb_v2_error_info_handle *err);
} duckdb_ext_api_v2;

//===--------------------------------------------------------------------===//
// Struct Create Method
//===--------------------------------------------------------------------===//
inline duckdb_ext_api_v2 CreateAPIv2(void) {
	duckdb_ext_api_v2 result;
	result.duckdb_v2_error_info_set_code = duckdb_v2_error_info_set_code;
	result.duckdb_v2_error_info_set_text = duckdb_v2_error_info_set_text;
	result.duckdb_v2_error_info_destroy = duckdb_v2_error_info_destroy;
	result.duckdb_v2_cast_function_builder_create = duckdb_v2_cast_function_builder_create;
	result.duckdb_v2_cast_function_builder_set_source_type = duckdb_v2_cast_function_builder_set_source_type;
	result.duckdb_v2_cast_function_builder_set_target_type = duckdb_v2_cast_function_builder_set_target_type;
	result.duckdb_v2_cast_function_builder_set_implicit_cast_cost =
	    duckdb_v2_cast_function_builder_set_implicit_cast_cost;
	result.duckdb_v2_cast_function_builder_set_exec_callback = duckdb_v2_cast_function_builder_set_exec_callback;
	result.duckdb_v2_cast_function_builder_set_user_data = duckdb_v2_cast_function_builder_set_user_data;
	result.duckdb_v2_cast_function_builder_register_with_connection =
	    duckdb_v2_cast_function_builder_register_with_connection;
	result.duckdb_v2_cast_function_builder_register_with_extension =
	    duckdb_v2_cast_function_builder_register_with_extension;
	result.duckdb_v2_cast_function_builder_destroy = duckdb_v2_cast_function_builder_destroy;
	result.duckdb_v2_cast_function_exec_get_user_data = duckdb_v2_cast_function_exec_get_user_data;
	result.duckdb_v2_cast_function_exec_get_input = duckdb_v2_cast_function_exec_get_input;
	result.duckdb_v2_cast_function_exec_get_output = duckdb_v2_cast_function_exec_get_output;
	result.duckdb_v2_cast_function_exec_get_count = duckdb_v2_cast_function_exec_get_count;
	result.duckdb_v2_cast_function_exec_get_mode = duckdb_v2_cast_function_exec_get_mode;
	result.duckdb_v2_column_data_collection_create_with_connection =
	    duckdb_v2_column_data_collection_create_with_connection;
	result.duckdb_v2_column_data_collection_create_with_context = duckdb_v2_column_data_collection_create_with_context;
	result.duckdb_v2_column_data_collection_destroy = duckdb_v2_column_data_collection_destroy;
	result.duckdb_v2_column_data_collection_combine = duckdb_v2_column_data_collection_combine;
	result.duckdb_v2_column_data_collection_reset = duckdb_v2_column_data_collection_reset;
	result.duckdb_v2_column_data_collection_row_count = duckdb_v2_column_data_collection_row_count;
	result.duckdb_v2_column_data_collection_append_state_create = duckdb_v2_column_data_collection_append_state_create;
	result.duckdb_v2_column_data_collection_append_state_destroy =
	    duckdb_v2_column_data_collection_append_state_destroy;
	result.duckdb_v2_column_data_collection_append = duckdb_v2_column_data_collection_append;
	result.duckdb_v2_column_data_collection_scan_state_create = duckdb_v2_column_data_collection_scan_state_create;
	result.duckdb_v2_column_data_collection_scan_state_destroy = duckdb_v2_column_data_collection_scan_state_destroy;
	result.duckdb_v2_column_data_collection_scan = duckdb_v2_column_data_collection_scan;
	result.duckdb_v2_column_data_collection_shared_scan_state_create =
	    duckdb_v2_column_data_collection_shared_scan_state_create;
	result.duckdb_v2_column_data_collection_shared_scan_state_destroy =
	    duckdb_v2_column_data_collection_shared_scan_state_destroy;
	result.duckdb_v2_column_data_collection_worker_scan_state_create =
	    duckdb_v2_column_data_collection_worker_scan_state_create;
	result.duckdb_v2_column_data_collection_worker_scan_state_destroy =
	    duckdb_v2_column_data_collection_worker_scan_state_destroy;
	result.duckdb_v2_column_data_collection_parallel_scan = duckdb_v2_column_data_collection_parallel_scan;
	result.duckdb_v2_option_create = duckdb_v2_option_create;
	result.duckdb_v2_option_destroy = duckdb_v2_option_destroy;
	result.duckdb_v2_option_get_name = duckdb_v2_option_get_name;
	result.duckdb_v2_option_get_setting = duckdb_v2_option_get_setting;
	result.duckdb_v2_option_get_default_setting = duckdb_v2_option_get_default_setting;
	result.duckdb_v2_option_get_description = duckdb_v2_option_get_description;
	result.duckdb_v2_option_get_target_scope = duckdb_v2_option_get_target_scope;
	result.duckdb_v2_option_get_alias_count = duckdb_v2_option_get_alias_count;
	result.duckdb_v2_option_get_alias = duckdb_v2_option_get_alias;
	result.duckdb_v2_logical_type_create_with_alias = duckdb_v2_logical_type_create_with_alias;
	result.duckdb_v2_custom_type_builder_create = duckdb_v2_custom_type_builder_create;
	result.duckdb_v2_custom_type_builder_destroy = duckdb_v2_custom_type_builder_destroy;
	result.duckdb_v2_custom_type_builder_register_with_connection =
	    duckdb_v2_custom_type_builder_register_with_connection;
	result.duckdb_v2_custom_type_builder_register_with_extension =
	    duckdb_v2_custom_type_builder_register_with_extension;
	result.duckdb_v2_custom_type_builder_set_name = duckdb_v2_custom_type_builder_set_name;
	result.duckdb_v2_custom_type_builder_set_base_type = duckdb_v2_custom_type_builder_set_base_type;
	result.duckdb_v2_data_chunk_create = duckdb_v2_data_chunk_create;
	result.duckdb_v2_data_chunk_destroy = duckdb_v2_data_chunk_destroy;
	result.duckdb_v2_data_chunk_get_size = duckdb_v2_data_chunk_get_size;
	result.duckdb_v2_data_chunk_get_vector_count = duckdb_v2_data_chunk_get_vector_count;
	result.duckdb_v2_data_chunk_get_vector = duckdb_v2_data_chunk_get_vector;
	result.duckdb_v2_open = duckdb_v2_open;
	result.duckdb_v2_close = duckdb_v2_close;
	result.duckdb_v2_database_option_set = duckdb_v2_database_option_set;
	result.duckdb_v2_database_option_get = duckdb_v2_database_option_get;
	result.duckdb_v2_database_option_get_count = duckdb_v2_database_option_get_count;
	result.duckdb_v2_database_option_get_by_index = duckdb_v2_database_option_get_by_index;
	result.duckdb_v2_library_version = duckdb_v2_library_version;
	result.duckdb_v2_create_environment = duckdb_v2_create_environment;
	result.duckdb_v2_destroy_environment = duckdb_v2_destroy_environment;
	result.duckdb_v2_environment_database_count = duckdb_v2_environment_database_count;
	result.duckdb_v2_error_info_get_code = duckdb_v2_error_info_get_code;
	result.duckdb_v2_error_info_get_text = duckdb_v2_error_info_get_text;
	result.duckdb_v2_error_info_get_raw_message = duckdb_v2_error_info_get_raw_message;
	result.duckdb_v2_expression_get_class = duckdb_v2_expression_get_class;
	result.duckdb_v2_expression_get_type = duckdb_v2_expression_get_type;
	result.duckdb_v2_expression_get_return_type = duckdb_v2_expression_get_return_type;
	result.duckdb_v2_expression_get_child_count = duckdb_v2_expression_get_child_count;
	result.duckdb_v2_expression_get_child = duckdb_v2_expression_get_child;
	result.duckdb_v2_expression_get_function_name = duckdb_v2_expression_get_function_name;
	result.duckdb_v2_expression_get_constant_value = duckdb_v2_expression_get_constant_value;
	result.duckdb_v2_expression_get_column_binding = duckdb_v2_expression_get_column_binding;
	result.duckdb_v2_expression_get_reference_index = duckdb_v2_expression_get_reference_index;
	result.duckdb_v2_file_system_get_from_context = duckdb_v2_file_system_get_from_context;
	result.duckdb_v2_file_system_get_from_connection = duckdb_v2_file_system_get_from_connection;
	result.duckdb_v2_file_system_open = duckdb_v2_file_system_open;
	result.duckdb_v2_file_handle_read = duckdb_v2_file_handle_read;
	result.duckdb_v2_file_handle_write = duckdb_v2_file_handle_write;
	result.duckdb_v2_file_handle_tell = duckdb_v2_file_handle_tell;
	result.duckdb_v2_file_handle_size = duckdb_v2_file_handle_size;
	result.duckdb_v2_file_handle_seek = duckdb_v2_file_handle_seek;
	result.duckdb_v2_file_handle_sync = duckdb_v2_file_handle_sync;
	result.duckdb_v2_file_handle_close = duckdb_v2_file_handle_close;
	result.duckdb_v2_file_handle_destroy = duckdb_v2_file_handle_destroy;
	result.duckdb_v2_bind_arguments_get_count = duckdb_v2_bind_arguments_get_count;
	result.duckdb_v2_bind_arguments_get_type = duckdb_v2_bind_arguments_get_type;
	result.duckdb_v2_bind_arguments_get_name = duckdb_v2_bind_arguments_get_name;
	result.duckdb_v2_bind_arguments_fold = duckdb_v2_bind_arguments_fold;
	result.duckdb_v2_identifier_render_quoted = duckdb_v2_identifier_render_quoted;
	result.duckdb_v2_log_storage_builder_create = duckdb_v2_log_storage_builder_create;
	result.duckdb_v2_log_storage_builder_set_name = duckdb_v2_log_storage_builder_set_name;
	result.duckdb_v2_log_storage_builder_set_user_data = duckdb_v2_log_storage_builder_set_user_data;
	result.duckdb_v2_log_storage_builder_set_log_callback = duckdb_v2_log_storage_builder_set_log_callback;
	result.duckdb_v2_log_storage_builder_register_with_database = duckdb_v2_log_storage_builder_register_with_database;
	result.duckdb_v2_log_storage_builder_register_with_extension =
	    duckdb_v2_log_storage_builder_register_with_extension;
	result.duckdb_v2_log_storage_builder_destroy = duckdb_v2_log_storage_builder_destroy;
	result.duckdb_v2_connection_log = duckdb_v2_connection_log;
	result.duckdb_v2_context_log = duckdb_v2_context_log;
	result.duckdb_v2_logical_type_create_from_id = duckdb_v2_logical_type_create_from_id;
	result.duckdb_v2_logical_type_create_from_text = duckdb_v2_logical_type_create_from_text;
	result.duckdb_v2_logical_type_get_from_text = duckdb_v2_logical_type_get_from_text;
	result.duckdb_v2_logical_type_create_from_args = duckdb_v2_logical_type_create_from_args;
	result.duckdb_v2_logical_type_get_from_args = duckdb_v2_logical_type_get_from_args;
	result.duckdb_v2_logical_type_copy = duckdb_v2_logical_type_copy;
	result.duckdb_v2_logical_type_destroy = duckdb_v2_logical_type_destroy;
	result.duckdb_v2_logical_type_is_equal = duckdb_v2_logical_type_is_equal;
	result.duckdb_v2_logical_type_get_id = duckdb_v2_logical_type_get_id;
	result.duckdb_v2_logical_type_get_name = duckdb_v2_logical_type_get_name;
	result.duckdb_v2_logical_type_to_text = duckdb_v2_logical_type_to_text;
	result.duckdb_v2_logical_type_get_param_count = duckdb_v2_logical_type_get_param_count;
	result.duckdb_v2_logical_type_get_param = duckdb_v2_logical_type_get_param;
	result.duckdb_v2_qname_parse = duckdb_v2_qname_parse;
	result.duckdb_v2_qname_create = duckdb_v2_qname_create;
	result.duckdb_v2_qname_get_part_count = duckdb_v2_qname_get_part_count;
	result.duckdb_v2_qname_get_part = duckdb_v2_qname_get_part;
	result.duckdb_v2_qname_render = duckdb_v2_qname_render;
	result.duckdb_v2_qname_equals = duckdb_v2_qname_equals;
	result.duckdb_v2_qname_hash = duckdb_v2_qname_hash;
	result.duckdb_v2_qname_destroy = duckdb_v2_qname_destroy;
	result.duckdb_v2_replacement_scan_register_with_database = duckdb_v2_replacement_scan_register_with_database;
	result.duckdb_v2_replacement_scan_register_with_extension = duckdb_v2_replacement_scan_register_with_extension;
	result.duckdb_v2_replacement_scan_get_catalog_name = duckdb_v2_replacement_scan_get_catalog_name;
	result.duckdb_v2_replacement_scan_get_schema_name = duckdb_v2_replacement_scan_get_schema_name;
	result.duckdb_v2_replacement_scan_get_table_name = duckdb_v2_replacement_scan_get_table_name;
	result.duckdb_v2_replacement_scan_get_user_data = duckdb_v2_replacement_scan_get_user_data;
	result.duckdb_v2_replacement_scan_set_function_name = duckdb_v2_replacement_scan_set_function_name;
	result.duckdb_v2_replacement_scan_add_parameter = duckdb_v2_replacement_scan_add_parameter;
	result.duckdb_v2_replacement_scan_add_named_parameter = duckdb_v2_replacement_scan_add_named_parameter;
	result.duckdb_v2_schema_get_count = duckdb_v2_schema_get_count;
	result.duckdb_v2_schema_get_field = duckdb_v2_schema_get_field;
	result.duckdb_v2_schema_destroy = duckdb_v2_schema_destroy;
	result.duckdb_v2_function_signature_create = duckdb_v2_function_signature_create;
	result.duckdb_v2_function_signature_destroy = duckdb_v2_function_signature_destroy;
	result.duckdb_v2_function_signature_add_parameter = duckdb_v2_function_signature_add_parameter;
	result.duckdb_v2_function_signature_add_parameter_default = duckdb_v2_function_signature_add_parameter_default;
	result.duckdb_v2_function_signature_set_varargs = duckdb_v2_function_signature_set_varargs;
	result.duckdb_v2_function_signature_set_return_type = duckdb_v2_function_signature_set_return_type;
	result.duckdb_v2_function_signature_get_parameter_count = duckdb_v2_function_signature_get_parameter_count;
	result.duckdb_v2_function_signature_get_parameter_name = duckdb_v2_function_signature_get_parameter_name;
	result.duckdb_v2_function_signature_get_parameter_type = duckdb_v2_function_signature_get_parameter_type;
	result.duckdb_v2_function_signature_parameter_has_default = duckdb_v2_function_signature_parameter_has_default;
	result.duckdb_v2_function_signature_get_parameter_default = duckdb_v2_function_signature_get_parameter_default;
	result.duckdb_v2_function_signature_has_varargs = duckdb_v2_function_signature_has_varargs;
	result.duckdb_v2_function_signature_get_varargs = duckdb_v2_function_signature_get_varargs;
	result.duckdb_v2_function_signature_has_return_type = duckdb_v2_function_signature_has_return_type;
	result.duckdb_v2_function_signature_get_return_type = duckdb_v2_function_signature_get_return_type;
	result.duckdb_v2_string_heap_allocate = duckdb_v2_string_heap_allocate;
	result.duckdb_v2_value_destroy = duckdb_v2_value_destroy;
	result.duckdb_v2_value_create_null = duckdb_v2_value_create_null;
	result.duckdb_v2_value_create_from_data = duckdb_v2_value_create_from_data;
	result.duckdb_v2_value_get_data = duckdb_v2_value_get_data;
	result.duckdb_v2_value_create_bignum = duckdb_v2_value_create_bignum;
	result.duckdb_v2_value_get_bignum = duckdb_v2_value_get_bignum;
	result.duckdb_v2_value_get_variant = duckdb_v2_value_get_variant;
	result.duckdb_v2_value_create_type = duckdb_v2_value_create_type;
	result.duckdb_v2_value_get_type = duckdb_v2_value_get_type;
	result.duckdb_v2_value_create = duckdb_v2_value_create;
	result.duckdb_v2_value_get_child_count = duckdb_v2_value_get_child_count;
	result.duckdb_v2_value_get_child = duckdb_v2_value_get_child;
	result.duckdb_v2_value_cast_with_connection = duckdb_v2_value_cast_with_connection;
	result.duckdb_v2_value_cast_with_context = duckdb_v2_value_cast_with_context;
	result.duckdb_v2_value_is_null = duckdb_v2_value_is_null;
	result.duckdb_v2_value_get_logical_type = duckdb_v2_value_get_logical_type;
	result.duckdb_v2_value_to_string = duckdb_v2_value_to_string;
	result.duckdb_v2_vector_get_logical_type = duckdb_v2_vector_get_logical_type;
	result.duckdb_v2_vector_get_vector_type = duckdb_v2_vector_get_vector_type;
	result.duckdb_v2_vector_get_view = duckdb_v2_vector_get_view;
	result.duckdb_v2_vector_get_size = duckdb_v2_vector_get_size;
	result.duckdb_v2_vector_set_size = duckdb_v2_vector_set_size;
	result.duckdb_v2_vector_get_value = duckdb_v2_vector_get_value;
	result.duckdb_v2_vector_set_value = duckdb_v2_vector_set_value;
	result.duckdb_v2_vector_get_data_mutable = duckdb_v2_vector_get_data_mutable;
	result.duckdb_v2_vector_flatten = duckdb_v2_vector_flatten;
	result.duckdb_v2_vector_reference = duckdb_v2_vector_reference;
	result.duckdb_v2_vector_make_constant = duckdb_v2_vector_make_constant;
	result.duckdb_v2_vector_make_sequence = duckdb_v2_vector_make_sequence;
	result.duckdb_v2_vector_set_null = duckdb_v2_vector_set_null;
	result.duckdb_v2_vector_flat_get_validity_mutable = duckdb_v2_vector_flat_get_validity_mutable;
	result.duckdb_v2_vector_constant_set_valid = duckdb_v2_vector_constant_set_valid;
	result.duckdb_v2_vector_get_string_heap = duckdb_v2_vector_get_string_heap;
	result.duckdb_v2_vector_get_child_count = duckdb_v2_vector_get_child_count;
	result.duckdb_v2_vector_get_child = duckdb_v2_vector_get_child;
	result.duckdb_v2_bignum_decode = duckdb_v2_bignum_decode;
	result.duckdb_v2_aggregate_function_builder_create = duckdb_v2_aggregate_function_builder_create;
	result.duckdb_v2_aggregate_function_builder_destroy = duckdb_v2_aggregate_function_builder_destroy;
	result.duckdb_v2_aggregate_function_builder_set_name = duckdb_v2_aggregate_function_builder_set_name;
	result.duckdb_v2_aggregate_function_builder_set_signature = duckdb_v2_aggregate_function_builder_set_signature;
	result.duckdb_v2_aggregate_function_builder_set_property = duckdb_v2_aggregate_function_builder_set_property;
	result.duckdb_v2_aggregate_function_builder_get_property = duckdb_v2_aggregate_function_builder_get_property;
	result.duckdb_v2_aggregate_function_builder_set_bind_callback =
	    duckdb_v2_aggregate_function_builder_set_bind_callback;
	result.duckdb_v2_aggregate_function_builder_set_size_callback =
	    duckdb_v2_aggregate_function_builder_set_size_callback;
	result.duckdb_v2_aggregate_function_builder_set_init_callback =
	    duckdb_v2_aggregate_function_builder_set_init_callback;
	result.duckdb_v2_aggregate_function_builder_set_update_callback =
	    duckdb_v2_aggregate_function_builder_set_update_callback;
	result.duckdb_v2_aggregate_function_builder_set_combine_callback =
	    duckdb_v2_aggregate_function_builder_set_combine_callback;
	result.duckdb_v2_aggregate_function_builder_set_finalize_callback =
	    duckdb_v2_aggregate_function_builder_set_finalize_callback;
	result.duckdb_v2_aggregate_function_builder_set_destroy_callback =
	    duckdb_v2_aggregate_function_builder_set_destroy_callback;
	result.duckdb_v2_aggregate_function_builder_set_user_data = duckdb_v2_aggregate_function_builder_set_user_data;
	result.duckdb_v2_aggregate_function_builder_register_with_connection =
	    duckdb_v2_aggregate_function_builder_register_with_connection;
	result.duckdb_v2_aggregate_function_builder_register_with_extension =
	    duckdb_v2_aggregate_function_builder_register_with_extension;
	result.duckdb_v2_aggregate_function_bind_get_function_name = duckdb_v2_aggregate_function_bind_get_function_name;
	result.duckdb_v2_aggregate_function_bind_get_user_data = duckdb_v2_aggregate_function_bind_get_user_data;
	result.duckdb_v2_aggregate_function_bind_get_arguments = duckdb_v2_aggregate_function_bind_get_arguments;
	result.duckdb_v2_aggregate_function_bind_set_bind_data = duckdb_v2_aggregate_function_bind_set_bind_data;
	result.duckdb_v2_aggregate_function_size_get_user_data = duckdb_v2_aggregate_function_size_get_user_data;
	result.duckdb_v2_aggregate_function_size_set_size = duckdb_v2_aggregate_function_size_set_size;
	result.duckdb_v2_aggregate_function_init_get_user_data = duckdb_v2_aggregate_function_init_get_user_data;
	result.duckdb_v2_aggregate_function_init_get_state = duckdb_v2_aggregate_function_init_get_state;
	result.duckdb_v2_aggregate_function_update_get_user_data = duckdb_v2_aggregate_function_update_get_user_data;
	result.duckdb_v2_aggregate_function_update_get_bind_data = duckdb_v2_aggregate_function_update_get_bind_data;
	result.duckdb_v2_aggregate_function_update_get_count = duckdb_v2_aggregate_function_update_get_count;
	result.duckdb_v2_aggregate_function_update_get_input = duckdb_v2_aggregate_function_update_get_input;
	result.duckdb_v2_aggregate_function_update_get_states = duckdb_v2_aggregate_function_update_get_states;
	result.duckdb_v2_aggregate_function_combine_get_user_data = duckdb_v2_aggregate_function_combine_get_user_data;
	result.duckdb_v2_aggregate_function_combine_get_bind_data = duckdb_v2_aggregate_function_combine_get_bind_data;
	result.duckdb_v2_aggregate_function_combine_get_count = duckdb_v2_aggregate_function_combine_get_count;
	result.duckdb_v2_aggregate_function_combine_get_sources = duckdb_v2_aggregate_function_combine_get_sources;
	result.duckdb_v2_aggregate_function_combine_get_targets = duckdb_v2_aggregate_function_combine_get_targets;
	result.duckdb_v2_aggregate_function_finalize_get_user_data = duckdb_v2_aggregate_function_finalize_get_user_data;
	result.duckdb_v2_aggregate_function_finalize_get_bind_data = duckdb_v2_aggregate_function_finalize_get_bind_data;
	result.duckdb_v2_aggregate_function_finalize_get_count = duckdb_v2_aggregate_function_finalize_get_count;
	result.duckdb_v2_aggregate_function_finalize_get_states = duckdb_v2_aggregate_function_finalize_get_states;
	result.duckdb_v2_aggregate_function_finalize_get_result = duckdb_v2_aggregate_function_finalize_get_result;
	result.duckdb_v2_aggregate_function_finalize_get_result_offset =
	    duckdb_v2_aggregate_function_finalize_get_result_offset;
	result.duckdb_v2_aggregate_function_destroy_get_user_data = duckdb_v2_aggregate_function_destroy_get_user_data;
	result.duckdb_v2_aggregate_function_destroy_get_bind_data = duckdb_v2_aggregate_function_destroy_get_bind_data;
	result.duckdb_v2_aggregate_function_destroy_get_count = duckdb_v2_aggregate_function_destroy_get_count;
	result.duckdb_v2_aggregate_function_destroy_get_states = duckdb_v2_aggregate_function_destroy_get_states;
	result.duckdb_v2_result_to_arrow_stream = duckdb_v2_result_to_arrow_stream;
	result.duckdb_v2_logical_types_to_arrow_schema = duckdb_v2_logical_types_to_arrow_schema;
	result.duckdb_v2_data_chunk_to_arrow_array = duckdb_v2_data_chunk_to_arrow_array;
	result.duckdb_v2_arrow_conversion_plan_create = duckdb_v2_arrow_conversion_plan_create;
	result.duckdb_v2_arrow_array_to_data_chunk = duckdb_v2_arrow_array_to_data_chunk;
	result.duckdb_v2_arrow_conversion_plan_get_schema = duckdb_v2_arrow_conversion_plan_get_schema;
	result.duckdb_v2_arrow_conversion_plan_destroy = duckdb_v2_arrow_conversion_plan_destroy;
	result.duckdb_v2_table_description_create = duckdb_v2_table_description_create;
	result.duckdb_v2_table_description_get_qname = duckdb_v2_table_description_get_qname;
	result.duckdb_v2_table_description_get_schema = duckdb_v2_table_description_get_schema;
	result.duckdb_v2_table_description_column_is_generated = duckdb_v2_table_description_column_is_generated;
	result.duckdb_v2_table_description_column_has_default = duckdb_v2_table_description_column_has_default;
	result.duckdb_v2_table_description_is_readonly = duckdb_v2_table_description_is_readonly;
	result.duckdb_v2_table_description_destroy = duckdb_v2_table_description_destroy;
	result.duckdb_v2_connect = duckdb_v2_connect;
	result.duckdb_v2_disconnect = duckdb_v2_disconnect;
	result.duckdb_v2_connection_option_set = duckdb_v2_connection_option_set;
	result.duckdb_v2_connection_option_get = duckdb_v2_connection_option_get;
	result.duckdb_v2_connection_option_get_count = duckdb_v2_connection_option_get_count;
	result.duckdb_v2_connection_option_get_by_index = duckdb_v2_connection_option_get_by_index;
	result.duckdb_v2_connection_create_extension = duckdb_v2_connection_create_extension;
	result.duckdb_v2_connection_interrupt = duckdb_v2_connection_interrupt;
	result.duckdb_v2_connection_query_progress = duckdb_v2_connection_query_progress;
	result.duckdb_v2_query_progress_get_percentage = duckdb_v2_query_progress_get_percentage;
	result.duckdb_v2_query_progress_get_rows_processed = duckdb_v2_query_progress_get_rows_processed;
	result.duckdb_v2_query_progress_get_total_rows_to_process = duckdb_v2_query_progress_get_total_rows_to_process;
	result.duckdb_v2_query_progress_destroy = duckdb_v2_query_progress_destroy;
	result.duckdb_v2_copy_function_builder_create = duckdb_v2_copy_function_builder_create;
	result.duckdb_v2_copy_function_builder_set_name = duckdb_v2_copy_function_builder_set_name;
	result.duckdb_v2_copy_function_builder_set_bind_callback = duckdb_v2_copy_function_builder_set_bind_callback;
	result.duckdb_v2_copy_function_builder_set_init_callback = duckdb_v2_copy_function_builder_set_init_callback;
	result.duckdb_v2_copy_function_builder_set_batch_callback = duckdb_v2_copy_function_builder_set_batch_callback;
	result.duckdb_v2_copy_function_builder_set_flush_callback = duckdb_v2_copy_function_builder_set_flush_callback;
	result.duckdb_v2_copy_function_builder_set_finalize_callback =
	    duckdb_v2_copy_function_builder_set_finalize_callback;
	result.duckdb_v2_copy_function_builder_set_user_data = duckdb_v2_copy_function_builder_set_user_data;
	result.duckdb_v2_copy_function_builder_register_with_connection =
	    duckdb_v2_copy_function_builder_register_with_connection;
	result.duckdb_v2_copy_function_builder_register_with_extension =
	    duckdb_v2_copy_function_builder_register_with_extension;
	result.duckdb_v2_copy_function_builder_destroy = duckdb_v2_copy_function_builder_destroy;
	result.duckdb_v2_copy_function_bind_get_user_data = duckdb_v2_copy_function_bind_get_user_data;
	result.duckdb_v2_copy_function_bind_get_column_count = duckdb_v2_copy_function_bind_get_column_count;
	result.duckdb_v2_copy_function_bind_get_column_type = duckdb_v2_copy_function_bind_get_column_type;
	result.duckdb_v2_copy_function_bind_get_column_name = duckdb_v2_copy_function_bind_get_column_name;
	result.duckdb_v2_copy_function_bind_set_bind_data = duckdb_v2_copy_function_bind_set_bind_data;
	result.duckdb_v2_copy_function_init_get_user_data = duckdb_v2_copy_function_init_get_user_data;
	result.duckdb_v2_copy_function_init_get_bind_data = duckdb_v2_copy_function_init_get_bind_data;
	result.duckdb_v2_copy_function_init_get_file_path = duckdb_v2_copy_function_init_get_file_path;
	result.duckdb_v2_copy_function_init_set_init_data = duckdb_v2_copy_function_init_set_init_data;
	result.duckdb_v2_copy_function_batch_get_user_data = duckdb_v2_copy_function_batch_get_user_data;
	result.duckdb_v2_copy_function_batch_get_bind_data = duckdb_v2_copy_function_batch_get_bind_data;
	result.duckdb_v2_copy_function_batch_get_init_data = duckdb_v2_copy_function_batch_get_init_data;
	result.duckdb_v2_copy_function_batch_get_input = duckdb_v2_copy_function_batch_get_input;
	result.duckdb_v2_copy_function_batch_set_batch_data = duckdb_v2_copy_function_batch_set_batch_data;
	result.duckdb_v2_copy_function_flush_get_user_data = duckdb_v2_copy_function_flush_get_user_data;
	result.duckdb_v2_copy_function_flush_get_bind_data = duckdb_v2_copy_function_flush_get_bind_data;
	result.duckdb_v2_copy_function_flush_get_init_data = duckdb_v2_copy_function_flush_get_init_data;
	result.duckdb_v2_copy_function_flush_get_batch_data = duckdb_v2_copy_function_flush_get_batch_data;
	result.duckdb_v2_copy_function_finalize_get_user_data = duckdb_v2_copy_function_finalize_get_user_data;
	result.duckdb_v2_copy_function_finalize_get_bind_data = duckdb_v2_copy_function_finalize_get_bind_data;
	result.duckdb_v2_copy_function_finalize_get_init_data = duckdb_v2_copy_function_finalize_get_init_data;
	result.duckdb_v2_scalar_function_builder_create = duckdb_v2_scalar_function_builder_create;
	result.duckdb_v2_scalar_function_builder_set_name = duckdb_v2_scalar_function_builder_set_name;
	result.duckdb_v2_scalar_function_builder_set_bind_callback = duckdb_v2_scalar_function_builder_set_bind_callback;
	result.duckdb_v2_scalar_function_builder_set_init_callback = duckdb_v2_scalar_function_builder_set_init_callback;
	result.duckdb_v2_scalar_function_builder_set_exec_callback = duckdb_v2_scalar_function_builder_set_exec_callback;
	result.duckdb_v2_scalar_function_builder_register_with_connection =
	    duckdb_v2_scalar_function_builder_register_with_connection;
	result.duckdb_v2_scalar_function_builder_register_with_extension =
	    duckdb_v2_scalar_function_builder_register_with_extension;
	result.duckdb_v2_scalar_function_builder_destroy = duckdb_v2_scalar_function_builder_destroy;
	result.duckdb_v2_scalar_function_builder_set_user_data = duckdb_v2_scalar_function_builder_set_user_data;
	result.duckdb_v2_scalar_function_builder_set_signature = duckdb_v2_scalar_function_builder_set_signature;
	result.duckdb_v2_scalar_function_builder_set_property = duckdb_v2_scalar_function_builder_set_property;
	result.duckdb_v2_scalar_function_builder_get_property = duckdb_v2_scalar_function_builder_get_property;
	result.duckdb_v2_scalar_function_bind_get_function_name = duckdb_v2_scalar_function_bind_get_function_name;
	result.duckdb_v2_scalar_function_bind_get_user_data = duckdb_v2_scalar_function_bind_get_user_data;
	result.duckdb_v2_scalar_function_bind_get_arguments = duckdb_v2_scalar_function_bind_get_arguments;
	result.duckdb_v2_scalar_function_bind_set_bind_data = duckdb_v2_scalar_function_bind_set_bind_data;
	result.duckdb_v2_scalar_function_bind_set_return_type = duckdb_v2_scalar_function_bind_set_return_type;
	result.duckdb_v2_scalar_function_init_get_function_name = duckdb_v2_scalar_function_init_get_function_name;
	result.duckdb_v2_scalar_function_init_get_user_data = duckdb_v2_scalar_function_init_get_user_data;
	result.duckdb_v2_scalar_function_init_get_bind_data = duckdb_v2_scalar_function_init_get_bind_data;
	result.duckdb_v2_scalar_function_init_set_init_data = duckdb_v2_scalar_function_init_set_init_data;
	result.duckdb_v2_scalar_function_exec_get_function_name = duckdb_v2_scalar_function_exec_get_function_name;
	result.duckdb_v2_scalar_function_exec_get_user_data = duckdb_v2_scalar_function_exec_get_user_data;
	result.duckdb_v2_scalar_function_exec_get_bind_data = duckdb_v2_scalar_function_exec_get_bind_data;
	result.duckdb_v2_scalar_function_exec_get_init_data = duckdb_v2_scalar_function_exec_get_init_data;
	result.duckdb_v2_scalar_function_exec_get_input = duckdb_v2_scalar_function_exec_get_input;
	result.duckdb_v2_scalar_function_exec_get_result = duckdb_v2_scalar_function_exec_get_result;
	result.duckdb_v2_parse_sql = duckdb_v2_parse_sql;
	result.duckdb_v2_statement_iterator_next = duckdb_v2_statement_iterator_next;
	result.duckdb_v2_statement_bind = duckdb_v2_statement_bind;
	result.duckdb_v2_statement_add_collection = duckdb_v2_statement_add_collection;
	result.duckdb_v2_sql_statement_destroy = duckdb_v2_sql_statement_destroy;
	result.duckdb_v2_statement_iterator_destroy = duckdb_v2_statement_iterator_destroy;
	result.duckdb_v2_table_function_builder_create = duckdb_v2_table_function_builder_create;
	result.duckdb_v2_table_function_builder_destroy = duckdb_v2_table_function_builder_destroy;
	result.duckdb_v2_table_function_builder_set_name = duckdb_v2_table_function_builder_set_name;
	result.duckdb_v2_table_function_builder_set_signature = duckdb_v2_table_function_builder_set_signature;
	result.duckdb_v2_table_function_builder_set_user_data = duckdb_v2_table_function_builder_set_user_data;
	result.duckdb_v2_table_function_builder_set_bind_callback = duckdb_v2_table_function_builder_set_bind_callback;
	result.duckdb_v2_table_function_builder_set_init_global_callback =
	    duckdb_v2_table_function_builder_set_init_global_callback;
	result.duckdb_v2_table_function_builder_set_init_local_callback =
	    duckdb_v2_table_function_builder_set_init_local_callback;
	result.duckdb_v2_table_function_builder_set_exec_callback = duckdb_v2_table_function_builder_set_exec_callback;
	result.duckdb_v2_table_function_builder_set_cardinality_callback =
	    duckdb_v2_table_function_builder_set_cardinality_callback;
	result.duckdb_v2_table_function_builder_set_progress_callback =
	    duckdb_v2_table_function_builder_set_progress_callback;
	result.duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback =
	    duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback;
	result.duckdb_v2_table_function_builder_set_projection_pushdown =
	    duckdb_v2_table_function_builder_set_projection_pushdown;
	result.duckdb_v2_table_function_builder_register_with_connection =
	    duckdb_v2_table_function_builder_register_with_connection;
	result.duckdb_v2_table_function_builder_register_with_extension =
	    duckdb_v2_table_function_builder_register_with_extension;
	result.duckdb_v2_table_function_bind_add_result_column = duckdb_v2_table_function_bind_add_result_column;
	result.duckdb_v2_table_function_bind_get_arguments = duckdb_v2_table_function_bind_get_arguments;
	result.duckdb_v2_table_function_bind_set_bind_data = duckdb_v2_table_function_bind_set_bind_data;
	result.duckdb_v2_table_function_bind_set_cardinality = duckdb_v2_table_function_bind_set_cardinality;
	result.duckdb_v2_table_function_bind_get_user_data = duckdb_v2_table_function_bind_get_user_data;
	result.duckdb_v2_table_function_init_get_bind_data = duckdb_v2_table_function_init_get_bind_data;
	result.duckdb_v2_table_function_init_set_global_state = duckdb_v2_table_function_init_set_global_state;
	result.duckdb_v2_table_function_init_get_global_state = duckdb_v2_table_function_init_get_global_state;
	result.duckdb_v2_table_function_init_set_local_state = duckdb_v2_table_function_init_set_local_state;
	result.duckdb_v2_table_function_init_set_max_threads = duckdb_v2_table_function_init_set_max_threads;
	result.duckdb_v2_table_function_init_get_column_count = duckdb_v2_table_function_init_get_column_count;
	result.duckdb_v2_table_function_init_get_column_index = duckdb_v2_table_function_init_get_column_index;
	result.duckdb_v2_table_function_init_get_user_data = duckdb_v2_table_function_init_get_user_data;
	result.duckdb_v2_table_function_filter_get_count = duckdb_v2_table_function_filter_get_count;
	result.duckdb_v2_table_function_filter_get_expression = duckdb_v2_table_function_filter_get_expression;
	result.duckdb_v2_table_function_filter_mark_handled = duckdb_v2_table_function_filter_mark_handled;
	result.duckdb_v2_table_function_filter_get_column_count = duckdb_v2_table_function_filter_get_column_count;
	result.duckdb_v2_table_function_filter_get_column_index = duckdb_v2_table_function_filter_get_column_index;
	result.duckdb_v2_table_function_filter_get_user_data = duckdb_v2_table_function_filter_get_user_data;
	result.duckdb_v2_table_function_exec_get_bind_data = duckdb_v2_table_function_exec_get_bind_data;
	result.duckdb_v2_table_function_exec_get_global_state = duckdb_v2_table_function_exec_get_global_state;
	result.duckdb_v2_table_function_exec_get_local_state = duckdb_v2_table_function_exec_get_local_state;
	result.duckdb_v2_table_function_exec_get_output_chunk = duckdb_v2_table_function_exec_get_output_chunk;
	result.duckdb_v2_table_function_exec_get_user_data = duckdb_v2_table_function_exec_get_user_data;
	result.duckdb_v2_statement_prepare = duckdb_v2_statement_prepare;
	result.duckdb_v2_prepared_execute = duckdb_v2_prepared_execute;
	result.duckdb_v2_prepared_reuses_plan = duckdb_v2_prepared_reuses_plan;
	result.duckdb_v2_prepared_statement_destroy = duckdb_v2_prepared_statement_destroy;
	result.duckdb_v2_statement_execute = duckdb_v2_statement_execute;
	result.duckdb_v2_result_destroy = duckdb_v2_result_destroy;
	result.duckdb_v2_result_step = duckdb_v2_result_step;
	result.duckdb_v2_result_fetch_chunk = duckdb_v2_result_fetch_chunk;
	result.duckdb_v2_result_wait = duckdb_v2_result_wait;
	result.duckdb_v2_result_render_box = duckdb_v2_result_render_box;
	result.duckdb_v2_result_drain = duckdb_v2_result_drain;
	result.duckdb_v2_result_get_result_type = duckdb_v2_result_get_result_type;
	result.duckdb_v2_result_get_statement_type = duckdb_v2_result_get_statement_type;
	result.duckdb_v2_result_get_schema = duckdb_v2_result_get_schema;
	return result;
}

#define DUCKDB_EXTENSION_API_V2_VERSION_MAJOR  2
#define DUCKDB_EXTENSION_API_V2_VERSION_MINOR  0
#define DUCKDB_EXTENSION_API_V2_VERSION_PATCH  0
#define DUCKDB_EXTENSION_API_V2_VERSION_STRING "v2.0.0"
