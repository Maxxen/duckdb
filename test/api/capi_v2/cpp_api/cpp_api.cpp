// Include the C++ API header (which includes the C API header)
#include "cpp_api.hpp"

// Include the DuckDB V2 Header
#include "duckdb_v2.h"

namespace duckdb_api {

//----------------------------------------------------------------------------------------------------------------------
// Error Handling Helpers
//----------------------------------------------------------------------------------------------------------------------

namespace {

// Perform a DuckDB C-API call, setup an error info object, and throw an exception if it fails.
// This is used to simplify error handling in the C++ wrapper.
template <class F, class... ARGS>
void CheckedAPICall(F &&func, ARGS &&... args) {
	duckdb_v2_error_info_ptr err = nullptr;
	auto code = func(std::forward<ARGS>(args)..., &err);
	if (code != DUCKDB_V2_ERROR_NONE) {
		const char *message_ptr = nullptr;
		if (err) {
			duckdb_v2_error_info_get_text(err, &message_ptr);
		}
		std::string message = message_ptr ? message_ptr : "unknown error";
		duckdb_v2_error_info_destroy(&err);
		throw Exception(code, message);
	}
}

// Catch any exceptions and propagate them via the error info out-parameter, returning an appropriate error code.
template <class T>
DUCKDB_V2_API_CALL_t WithExceptionGuard(duckdb_v2_error_info_ptr *err, T callback) {
	auto code = static_cast<DUCKDB_V2_API_CALL_t>(DUCKDB_V2_ERROR_NONE);
	auto text = std::string();

	try {
		// Invoke the callback
		callback();
	} catch (const Exception &ex) {
		code = ex.GetCode();
		text = ex.what();
	} catch (const std::exception &ex) {
		code = DUCKDB_V2_API_ERROR;
		text = ex.what();
	} catch (...) {
		code = DUCKDB_V2_API_ERROR;
		text = "An unknown error occurred.";
	}

	// Pass up to the caller via the out-parameter if they provided one; otherwise swallow.
	if (err && *err) {
		duckdb_v2_error_info_set_code(*err, code);
		duckdb_v2_error_info_set_text(*err, text.c_str());
	}

	return code;
}

} // namespace

//---------------------------------------------------------------------------
// Environment
//---------------------------------------------------------------------------

Environment::Environment() : detail::Handle() {
	CheckedAPICall(duckdb_v2_create_environment, &impl);
}

Environment::~Environment() {
	duckdb_v2_destroy_environment(&impl);
}

size_t Environment::GetOpenDatabaseCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_environment_database_count, impl, &count);
	return static_cast<size_t>(count);
}

Database Environment::Open(const std::string &path) {
	duckdb_v2_database_ptr db = nullptr;
	CheckedAPICall(duckdb_v2_open, impl, path.empty() ? nullptr : path.c_str(), nullptr, 0, &db);
	return detail::Factory::Make<Database>(db);
}

//---------------------------------------------------------------------------
// Database Option
//---------------------------------------------------------------------------

DatabaseOption::DatabaseOption(void *impl) : detail::Handle(impl) {
}

DatabaseOption::DatabaseOption(const std::string &name, const std::string &value) : detail::Handle() {
	CheckedAPICall(duckdb_v2_option_create, name.c_str(), value.c_str(), &impl);
}

std::string_view DatabaseOption::GetName() const {
	const char *name = nullptr;
	CheckedAPICall(duckdb_v2_option_get_name, impl, &name);
	return name ? std::string_view(name) : std::string_view();
}

std::string_view DatabaseOption::GetValue() const {
	const char *value = nullptr;
	CheckedAPICall(duckdb_v2_option_get_setting, impl, &value);
	return value ? std::string_view(value) : std::string_view();
}

std::string_view DatabaseOption::GetDefaultValue() const {
	const char *default_value = nullptr;
	CheckedAPICall(duckdb_v2_option_get_default_setting, impl, &default_value);
	return default_value ? std::string_view(default_value) : std::string_view();
}

std::string_view DatabaseOption::GetDescription() const {
	const char *description = nullptr;
	CheckedAPICall(duckdb_v2_option_get_description, impl, &description);
	return description ? std::string_view(description) : std::string_view();
}

size_t DatabaseOption::GetAliasCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_option_get_alias_count, impl, &count);
	return static_cast<size_t>(count);
}

std::string_view DatabaseOption::GetAliasByIndex(size_t index) const {
	const char *alias = nullptr;
	CheckedAPICall(duckdb_v2_option_get_alias, impl, static_cast<idx_t>(index), &alias);
	return alias ? std::string_view(alias) : std::string_view();
}

DatabaseOption::~DatabaseOption() {
	duckdb_v2_option_destroy(&impl);
}

//---------------------------------------------------------------------------
// Database
//---------------------------------------------------------------------------

Database::Database(void *impl) : detail::Handle(impl) {
}

Database::~Database() {
	duckdb_v2_close(&impl);
}

size_t Database::GetOptionCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_database_option_get_count, impl, &count);
	return static_cast<size_t>(count);
}

DatabaseOption Database::GetOptionByIndex(size_t index) const {
	duckdb_v2_option_ptr option = nullptr;
	CheckedAPICall(duckdb_v2_database_option_get_by_index, impl, static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

void Database::SetOption(const DatabaseOption &option) {
	CheckedAPICall(duckdb_v2_database_option_set, impl, detail::GetHandle(option));
}

Connection Database::Connect() {
	duckdb_v2_connection_ptr conn = nullptr;
	CheckedAPICall(duckdb_v2_connect, impl, &conn);
	return detail::Factory::Make<Connection>(conn);
}

//---------------------------------------------------------------------------
// Connection
//---------------------------------------------------------------------------

Connection::Connection(void *impl) : detail::Handle(impl) {
}

Connection::~Connection() {
	duckdb_v2_disconnect(&impl);
}

size_t Connection::GetOptionCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_connection_option_get_count, impl, &count);
	return static_cast<size_t>(count);
}

DatabaseOption Connection::GetOptionByIndex(size_t index) const {
	duckdb_v2_option_ptr option = nullptr;
	CheckedAPICall(duckdb_v2_connection_option_get_by_index, impl, static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

void Connection::SetOption(const DatabaseOption &option) {
	// TODO: Pass scope
	CheckedAPICall(duckdb_v2_connection_option_set, impl, detail::GetHandle(option), DUCKDB_V2_SETTING_SCOPE_AUTOMATIC);
}

void Connection::WithTransaction(std::function<void(const Context &ctx)> callback) {
	static auto trampoline = [](duckdb_v2_context_ptr ctx, void *user_data, duckdb_v2_error_info_ptr *err) {
		// Catch any exceptions thrown by the user callback and propagate them to the error info

		WithExceptionGuard(err, [&]() {
			// Unwrap the callback
			auto &callback = *static_cast<std::function<void(const Context &)> *>(user_data);

			// Make a Context wrapper for the provided ctx and pass it to the callback
			auto ctx_wrapper = detail::Factory::Make<Context>(ctx);

			// Invoke the user callback
			callback(ctx_wrapper);
		});
	};

	CheckedAPICall(duckdb_v2_connection_execute_with_context, impl, trampoline, &callback);
}

QueryResult Connection::Query(const std::string &sql) {
	duckdb_v2_result_ptr result = nullptr;
	CheckedAPICall(duckdb_v2_connection_query, impl, sql.c_str(), &result);
	return detail::Factory::Make<QueryResult>(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

Context::Context(void *impl) : detail::Handle(impl) {
}

Context::~Context() {
	// Context lifetime is managed by DuckDB, so we don't destroy the handle here
}

FileSystem Context::GetFileSystem() const {
	duckdb_v2_file_system_ptr fs = nullptr;
	CheckedAPICall(duckdb_v2_file_system_get_from_context, impl, &fs);
	return detail::Factory::Make<FileSystem>(fs);
}

//----------------------------------------------------------------------------------------------------------------------
// File System
//----------------------------------------------------------------------------------------------------------------------

FileSystem::FileSystem(void *impl) : detail::Handle(impl) {
}

FileSystem::~FileSystem() {
	// FileSystem lifetime is managed by DuckDB, so we don't destroy the handle here
}

FileHandle FileSystem::OpenFile(const std::string &path, FileFlags flags) const {
	duckdb_v2_file_handle_ptr handle = nullptr;

	// TODO: Verify file flags
	CheckedAPICall(duckdb_v2_file_system_open, impl, path.c_str(), static_cast<uint64_t>(flags), &handle);
	return detail::Factory::Make<FileHandle>(handle);
}

//----------------------------------------------------------------------------------------------------------------------
// File Handle
//----------------------------------------------------------------------------------------------------------------------

FileHandle::FileHandle(void *impl) : detail::Handle(impl) {
}

FileHandle::~FileHandle() {
	duckdb_v2_file_handle_destroy(&impl);
}

void FileHandle::Close() {
	CheckedAPICall(duckdb_v2_file_handle_close, impl);
}

void FileHandle::Sync() {
	CheckedAPICall(duckdb_v2_file_handle_sync, impl);
}

void FileHandle::Seek(int64_t position) {
	;
	CheckedAPICall(duckdb_v2_file_handle_seek, impl, position);
}

auto FileHandle::Tell() const -> int64_t {
	int64_t position = 0;
	CheckedAPICall(duckdb_v2_file_handle_tell, impl, &position);
	return position;
}

auto FileHandle::Size() const -> int64_t {
	int64_t size = 0;
	CheckedAPICall(duckdb_v2_file_handle_size, impl, &size);
	return size;
}

auto FileHandle::Read(void *buffer, int64_t size) -> int64_t {
	int64_t bytes_read = 0;
	CheckedAPICall(duckdb_v2_file_handle_read, impl, buffer, size, &bytes_read);
	return bytes_read;
}

auto FileHandle::Write(const void *buffer, int64_t size) -> int64_t {
	int64_t bytes_written = 0;
	CheckedAPICall(duckdb_v2_file_handle_write, impl, buffer, size, &bytes_written);
	return bytes_written;
}

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------

LogicalType::LogicalType(void *impl) : detail::Handle(impl) {
}

std::string_view LogicalType::GetAlias() const {
	const char *alias = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_get_alias, impl, &alias);
	return alias ? std::string_view(alias) : std::string_view();
}

LogicalType LogicalType::INTEGER() {
	duckdb_v2_logical_type_ptr type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type);
	return detail::Factory::Make<LogicalType>(type);
}

LogicalType LogicalType::VARCHAR() {
	duckdb_v2_logical_type_ptr type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &type);
	return detail::Factory::Make<LogicalType>(type);
}

LogicalType::~LogicalType() {
	duckdb_v2_logical_type_destroy(&impl);
}

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------

Value::Value(void *impl) : detail::Handle(impl) {
}

Value::~Value() {
	duckdb_v2_value_destroy(&impl);
}

auto Value::IsNull() const -> bool {
	bool is_null = false;
	CheckedAPICall(duckdb_v2_value_is_null, impl, &is_null);
	return is_null;
}

auto Value::GetLogicalType() const -> LogicalType {
	duckdb_v2_logical_type_ptr type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_logical_type, impl, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Value::ToString() const -> std::string {
	char *str = nullptr;
	CheckedAPICall(duckdb_v2_value_to_string, impl, &str);
	auto result = std::string(str);
	free(str); // TODO: This should use something like duckdb_v2_free to avoid potential cross-allocator issues.
	return result;
}

auto Value::AsBool() const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_value_get_bool, impl, &result);
	return result;
}

auto Value::AsI8() const -> int8_t {
	int8_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int8, impl, &result);
	return result;
}

auto Value::AsI16() const -> int16_t {
	int16_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int16, impl, &result);
	return result;
}

auto Value::AsU8() const -> uint8_t {
	uint8_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint8, impl, &result);
	return result;
}

auto Value::AsU16() const -> uint16_t {
	uint16_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint16, impl, &result);
	return result;
}

auto Value::AsI32() const -> int32_t {
	int32_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int32, impl, &result);
	return result;
}

auto Value::AsU32() const -> uint32_t {
	uint32_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint32, impl, &result);
	return result;
}

auto Value::AsI64() const -> int64_t {
	int64_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int64, impl, &result);
	return result;
}

auto Value::AsU64() const -> uint64_t {
	uint64_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint64, impl, &result);
	return result;
}

auto Value::AsF32() const -> float {
	float result = 0;
	CheckedAPICall(duckdb_v2_value_get_float, impl, &result);
	return result;
}

auto Value::AsF64() const -> double {
	double result = 0;
	CheckedAPICall(duckdb_v2_value_get_double, impl, &result);
	return result;
}

auto Value::AsVarchar() const -> std::string_view {
	const char *str = nullptr;
	idx_t size = 0;
	CheckedAPICall(duckdb_v2_value_get_varchar, impl, &str, &size);
	return str ? std::string_view(str, size) : std::string_view();
}

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------
Vector::Vector(void *impl) : detail::Handle(impl) {
}

Vector::~Vector() {
	/* Vectors are always borrowed, so we don't destroy the handle here */
}

auto Vector::GetDataMutable() -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_data_mutable, impl, &data);
	return data;
}

auto Vector::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_vector_get_child_count, impl, &count);
	return count;
}

auto Vector::GetChild(idx_t index) const -> Vector {
	duckdb_v2_vector_ptr child = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_child, impl, index, &child);
	return detail::Factory::Make<Vector>(child);
}

auto Vector::GetLogicalType() const -> LogicalType {
	duckdb_v2_logical_type_ptr type = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_logical_type, impl, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Vector::Flatten() const -> void {
	CheckedAPICall(duckdb_v2_vector_flatten, impl);
}

auto Vector::GetSize() const -> idx_t {
	idx_t size = 0;
	CheckedAPICall(duckdb_v2_vector_get_size, impl, &size);
	return size;
}

auto Vector::SetSize(idx_t size) -> void {
	CheckedAPICall(duckdb_v2_vector_set_size, impl, size);
}

//----------------------------------------------------------------------------------------------------------------------
// Data Chunk
//----------------------------------------------------------------------------------------------------------------------
DataChunk::DataChunk(void *impl, bool owned) : detail::Handle(impl), owned(owned) {
}

DataChunk::~DataChunk() {
	if (owned) {
		duckdb_v2_data_chunk_destroy(&impl);
	}
}

auto DataChunk::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_size, impl, &count);
	return count;
}

auto DataChunk::GetVectorCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector_count, impl, &count);
	return count;
}

auto DataChunk::GetVector(idx_t index) const -> Vector {
	duckdb_v2_vector_ptr vector = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector, impl, index, &vector);
	return detail::Factory::Make<Vector>(vector);
}

//----------------------------------------------------------------------------------------------------------------------
// Query Result
//----------------------------------------------------------------------------------------------------------------------

QueryResult::QueryResult(void *impl) : detail::Handle(impl) {
}

QueryResult::~QueryResult() {
	duckdb_v2_result_destroy(&impl);
}

auto QueryResult::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_result_column_count, impl, &count);
	return count;
}

auto QueryResult::GetColumnName(idx_t index) const -> std::string_view {
	const char *name = nullptr;
	idx_t name_length = 0; // TODO: This name length is redundant.
	CheckedAPICall(duckdb_v2_result_column_name, impl, index, &name, &name_length);
	return name ? std::string_view(name) : std::string_view();
}

auto QueryResult::GetColumnType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_ptr type = nullptr;
	CheckedAPICall(duckdb_v2_result_column_logical_type, impl, index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto QueryResult::GetRowsChanged() const -> idx_t {
	idx_t changed = 0;
	CheckedAPICall(duckdb_v2_result_rows_changed, impl, &changed);
	return changed;
}

auto QueryResult::GetChunkCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_result_chunk_count, impl, &count);
	return count;
}

auto QueryResult::GetChunk(idx_t index) const -> DataChunk {
	duckdb_v2_data_chunk_ptr chunk = nullptr;
	CheckedAPICall(duckdb_v2_result_get_chunk, impl, index, &chunk);
	return detail::Factory::Make<DataChunk>(chunk, true);
}

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

ScalarFunction::ScalarFunction(const Context &context) {
	CheckedAPICall(duckdb_v2_scalar_function_builder_create, detail::GetHandle(context), &impl);
}

ScalarFunction::~ScalarFunction() {
	duckdb_v2_scalar_function_builder_destroy(&impl);
}

auto ScalarFunction::SetName(const std::string &name) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_name, impl, name.c_str());
	return *this;
}

auto ScalarFunction::AddParameter(const std::string &name, const LogicalType &type) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_add_parameter, impl, name.c_str(), detail::GetHandle(type));
	return *this;
}

auto ScalarFunction::SetReturnType(const LogicalType &type) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_return_type, impl, detail::GetHandle(type));
	return *this;
}

struct ScalarFunctionInfo {
	ScalarFunction::BindCallback bind_callback = nullptr;
	ScalarFunction::InitCallback init_callback = nullptr;
	ScalarFunction::ExecCallback exec_callback = nullptr;
};

void *ScalarFunction::BindInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_scalar_function_bind_args *>(args)->out_bind_data;
}

void ScalarFunction::BindInput::SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
                                                    void (*destructor)(void *)) {
	duckdb_v2_scalar_function_bind_args *args_struct = static_cast<duckdb_v2_scalar_function_bind_args *>(args);
	args_struct->out_bind_data = data;
	args_struct->out_bind_data_copy = copy;
	args_struct->out_bind_data_equality = equals;
	args_struct->out_bind_data_destructor = destructor;
}

void *ScalarFunction::InitInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_scalar_function_init_args *>(args)->bind_data;
}

void *ScalarFunction::InitInput::GetWorkerStateInternal() const {
	return static_cast<duckdb_v2_scalar_function_init_args *>(args)->out_init_data;
}

void ScalarFunction::InitInput::SetWorkerStateInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_scalar_function_init_args *>(args);
	args_struct->out_init_data = data;
	args_struct->out_init_data_destructor = destructor;
}

void *ScalarFunction::ExecInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_scalar_function_exec_args *>(args)->bind_data;
}

void *ScalarFunction::ExecInput::GetWorkerStateInternal() const {
	return static_cast<duckdb_v2_scalar_function_exec_args *>(args)->init_data;
}

auto ScalarFunction::ExecInput::GetInputChunk() const -> DataChunk {
	auto chunk = static_cast<duckdb_v2_scalar_function_exec_args *>(args)->input;
	return detail::Factory::Make<DataChunk>(chunk, false);
}

auto ScalarFunction::ExecInput::GetResultVector() const -> Vector {
	auto vec = static_cast<duckdb_v2_scalar_function_exec_args *>(args)->result;
	return detail::Factory::Make<Vector>(vec);
}

auto ScalarFunction::SetBindCallback(BindCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, impl, nullptr);

		bind_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_bind_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<BindInput>(args);

			// Now call the user callback
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, impl, trampoline);

	// And set the bind callback. This will be set in the user_data once the function is registered.
	bind_callback = callback;

	return *this;
}
auto ScalarFunction::SetInitCallback(InitCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_init_callback, impl, nullptr);

		init_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_init_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<InitInput>(args);

			// Now call the user callback
			function.init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_init_callback, impl, trampoline);

	// And set the init callback. This will be set in the user_data once the function is registered.
	init_callback = callback;

	return *this;
}
auto ScalarFunction::SetExecCallback(ExecCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_exec_callback, impl, nullptr);

		exec_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_exec_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<ExecInput>(args);

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_exec_callback, impl, trampoline);

	// And set the exec callback. This will be set in the user_data once the function is registered.
	exec_callback = callback;

	return *this;
}

void ScalarFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_user_data, impl,
	               new ScalarFunctionInfo {bind_callback, init_callback, exec_callback},
	               detail::TypedDelete<ScalarFunctionInfo>);

	CheckedAPICall(duckdb_v2_scalar_function_builder_register, detail::GetHandle(ctx), impl);
}

//----------------------------------------------------------------------------------------------------------------------
// Aggregate Function
//----------------------------------------------------------------------------------------------------------------------

class AggregateFunctionInfo {
public:
	AggregateFunction::SizeCallback size_callback = nullptr;
	AggregateFunction::InitializeCallback initialize_callback = nullptr;
	AggregateFunction::UpdateCallback update_callback = nullptr;
	AggregateFunction::CombineCallback combine_callback = nullptr;
	AggregateFunction::FinalizeCallback finalize_callback = nullptr;
	AggregateFunction::DestroyCallback destroy_callback = nullptr;
};

AggregateFunction::AggregateFunction(const Context &ctx) {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_create, detail::GetHandle(ctx), &impl);
}

AggregateFunction::~AggregateFunction() {
	duckdb_v2_aggregate_function_builder_destroy(&impl);
}

auto AggregateFunction::SetName(const std::string &name) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_name, impl, name.c_str());
	return *this;
}

auto AggregateFunction::AddParameter(const std::string &name, const LogicalType &type) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_add_parameter, impl, name.c_str(), detail::GetHandle(type));
	return *this;
}

auto AggregateFunction::SetReturnType(const LogicalType &type) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_return_type, impl, detail::GetHandle(type));
	return *this;
}

class AggregateFunction::SizeInput::Inner {
public:
	idx_t size_in_bytes = 0;
};

void AggregateFunction::SizeInput::Reserve(idx_t size_in_bytes) {
	inner.size_in_bytes = size_in_bytes;
}

auto AggregateFunction::SetSizeCallback(SizeCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_size_callback, impl, nullptr);
		size_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_size_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			SizeInput::Inner inner;

			SizeInput input {inner};

			// Now call the user callback
			function.size_callback(input);

			// And write the result to the out-parameter
			args->out_size = inner.size_in_bytes;
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_size_callback, impl, trampoline);

	size_callback = callback;

	return *this;
}

class AggregateFunction::InitializeInput::Inner {
public:
	void *state = nullptr;
};

void *AggregateFunction::InitializeInput::GetStatePointer() const {
	return inner.state;
}

auto AggregateFunction::SetInitializeCallback(InitializeCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_init_callback, impl, nullptr);
		initialize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_init_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			InitializeInput::Inner inner;
			inner.state = args->state;

			InitializeInput input {inner};

			// Now call the user callback
			function.initialize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_init_callback, impl, trampoline);

	initialize_callback = callback;

	return *this;
}

class AggregateFunction::UpdateInput::Inner {
public:
	idx_t count;
	void **states;
	DataChunk chunk;
};

auto AggregateFunction::UpdateInput::GetInputChunk() const -> const DataChunk & {
	return inner.chunk;
}
auto AggregateFunction::UpdateInput::GetStateCount() const -> idx_t {
	return inner.count;
}
auto AggregateFunction::UpdateInput::GetStateArray() const -> void ** {
	return inner.states;
}

auto AggregateFunction::SetUpdateCallback(UpdateCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_update_callback, impl, nullptr);
		update_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_update_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			UpdateInput::Inner inner {args->count, args->states, detail::Factory::Make<DataChunk>(args->input, false)};

			UpdateInput input {inner};

			// Now call the user callback
			function.update_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_update_callback, impl, trampoline);

	update_callback = callback;

	return *this;
}

class AggregateFunction::CombineInput::Inner {
public:
	idx_t count;
	void **sources;
	void **targets;
};

auto AggregateFunction::CombineInput::GetStateCount() const -> idx_t {
	return inner.count;
}
auto AggregateFunction::CombineInput::GetSourceStateArray() const -> void ** {
	return inner.sources;
}
auto AggregateFunction::CombineInput::GetTargetStateArray() const -> void ** {
	return inner.targets;
}

auto AggregateFunction::SetCombineCallback(CombineCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_combine_callback, impl, nullptr);
		combine_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_combine_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			CombineInput::Inner inner {args->count, args->sources, args->targets};

			CombineInput input {inner};

			// Now call the user callback
			function.combine_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_combine_callback, impl, trampoline);

	combine_callback = callback;

	return *this;
}

class AggregateFunction::FinalizeInput::Inner {
public:
	idx_t count;
	void **states;
	Vector result_vector;
	idx_t result_offset;
};

auto AggregateFunction::FinalizeInput::GetStateCount() const -> idx_t {
	return inner.count;
}

auto AggregateFunction::FinalizeInput::GetStateArray() const -> void ** {
	return inner.states;
}

auto AggregateFunction::FinalizeInput::GetResultVector() const -> Vector & {
	return inner.result_vector;
}

auto AggregateFunction::FinalizeInput::GetResultOffset() const -> idx_t {
	return inner.result_offset;
}

auto AggregateFunction::SetFinalizeCallback(FinalizeCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_finalize_callback, impl, nullptr);
		finalize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_finalize_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			FinalizeInput::Inner inner {args->count, args->states, detail::Factory::Make<Vector>(args->result),
			                            args->result_offset};

			FinalizeInput input {inner};

			// Now call the user callback
			function.finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_finalize_callback, impl, trampoline);

	finalize_callback = callback;

	return *this;
}

class AggregateFunction::DestroyInput::Inner {
public:
	idx_t count;
	void **states;
};

auto AggregateFunction::DestroyInput::GetStateArray() const -> void ** {
	return inner.states;
}

auto AggregateFunction::DestroyInput::GetStateCount() const -> idx_t {
	return inner.count;
}

auto AggregateFunction::SetDestroyCallback(DestroyCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_destroy_callback, impl, nullptr);
		destroy_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_destroy_args *args, duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			DestroyInput::Inner inner {args->count, args->states};
			DestroyInput input {inner};

			// Now call the user callback
			function.destroy_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_destroy_callback, impl, trampoline);

	destroy_callback = callback;

	return *this;
}

void AggregateFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_user_data, impl,
	               new AggregateFunctionInfo {size_callback, initialize_callback, update_callback, combine_callback,
	                                          finalize_callback, destroy_callback},
	               detail::TypedDelete<AggregateFunctionInfo>);

	CheckedAPICall(duckdb_v2_aggregate_function_builder_register, detail::GetHandle(ctx), impl);
}

//----------------------------------------------------------------------------------------------------------------------
// Table Function
//----------------------------------------------------------------------------------------------------------------------
class TableFunctionInfo {
public:
	TableFunction::BindCallback bind_callback = nullptr;
	TableFunction::InitGlobalCallback init_global_callback = nullptr;
	TableFunction::InitLocalCallback init_local_callback = nullptr;
	TableFunction::ExecCallback exec_callback = nullptr;
};

TableFunction::TableFunction(const Context &ctx) {
	CheckedAPICall(duckdb_v2_table_function_builder_create, detail::GetHandle(ctx), &impl);
}

TableFunction::~TableFunction() {
	duckdb_v2_table_function_builder_destroy(&impl);
}

auto TableFunction::SetName(const std::string &name) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_set_name, impl, name.c_str());
	return *this;
}

auto TableFunction::AddParameter(const LogicalType &type) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_add_parameter, impl, detail::GetHandle(type));
	return *this;
}

auto TableFunction::AddNamedParameter(const std::string &name, const LogicalType &type) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_add_named_parameter, impl, name.c_str(), detail::GetHandle(type));
	return *this;
}

class TableFunction::BindInput::Inner {
public:
	duckdb_v2_table_function_bind_info_ptr info = nullptr;
};

void *TableFunction::BindInput::GetUserDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_user_data, inner.info, &data);
	return data;
}

void TableFunction::BindInput::SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
                                                   void (*destructor)(void *)) {
	CheckedAPICall(duckdb_v2_table_function_bind_set_bind_data, inner.info, data, copy, equals, destructor);
}

auto TableFunction::BindInput::AddResultColumn(const std::string &name, const LogicalType &type) -> void {
	CheckedAPICall(duckdb_v2_table_function_bind_add_result_column, inner.info, name.c_str(), detail::GetHandle(type));
}

auto TableFunction::BindInput::GetParameter(idx_t index) const -> Value {
	duckdb_v2_value_ptr value = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_parameter, inner.info, index, &value);
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::GetNamedParameter(const std::string &name) const -> Value {
	duckdb_v2_value_ptr value = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_named_parameter, inner.info, name.c_str(), &value);
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::TryGetParameter(idx_t index) const -> std::optional<Value> {
	duckdb_v2_value_ptr value = nullptr;
	const auto res = duckdb_v2_table_function_bind_get_parameter(inner.info, index, &value, nullptr);
	if (res != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::TryGetNamedParameter(const std::string &name) const -> std::optional<Value> {
	duckdb_v2_value_ptr value = nullptr;

	const auto res = duckdb_v2_table_function_bind_get_named_parameter(inner.info, name.c_str(), &value, nullptr);
	if (res != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::SetBindCallback(BindCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_bind_cb, impl, nullptr);
		bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_bind_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_bind_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			BindInput::Inner inner {info};
			BindInput input {inner};

			// Now call the user callback
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_bind_cb, impl, trampoline);

	bind_callback = callback;

	return *this;
}

class TableFunction::InitGlobalInput::Inner {
public:
	duckdb_v2_table_function_init_info_ptr info = nullptr;
};

auto TableFunction::InitGlobalInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_bind_data, inner.info, &data);
	return data;
}

auto TableFunction::InitGlobalInput::SetGlobalStateInternal(void *data, void (*destructor)(void *)) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_set_global_state, inner.info, data, destructor);
}

auto TableFunction::SetInitGlobalCallback(InitGlobalCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_init_global_cb, impl, nullptr);
		init_global_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_init_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			InitGlobalInput::Inner inner {info};
			InitGlobalInput input {inner};

			// Now call the user callback
			function.init_global_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_init_global_cb, impl, trampoline);

	init_global_callback = callback;

	return *this;
}

class TableFunction::InitLocalInput::Inner {
public:
	duckdb_v2_table_function_init_info_ptr info = nullptr;
};

auto TableFunction::InitLocalInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_bind_data, inner.info, &data);
	return data;
}

auto TableFunction::InitLocalInput::SetLocalStateInternal(void *data, void (*destructor)(void *)) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_set_local_state, inner.info, data, destructor);
}

auto TableFunction::SetInitLocalCallback(InitLocalCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_init_local_cb, impl, nullptr);
		init_local_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_init_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			InitLocalInput::Inner inner {info};
			InitLocalInput input {inner};

			// Now call the user callback
			function.init_local_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_init_local_cb, impl, trampoline);

	init_local_callback = callback;

	return *this;
}

class TableFunction::ExecInput::Inner {
public:
	duckdb_v2_table_function_exec_info_ptr info = nullptr;
	DataChunk output_chunk;
};

auto TableFunction::ExecInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_bind_data, inner.info, &data);
	return data;
}

auto TableFunction::ExecInput::GetGlobalStateInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_global_state, inner.info, &data);
	return data;
}

auto TableFunction::ExecInput::GetLocalStateInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_local_state, inner.info, &data);
	return data;
}

auto TableFunction::ExecInput::GetResultChunk() const -> DataChunk & {
	return inner.output_chunk;
}

auto TableFunction::SetExecCallback(ExecCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_exec_cb, impl, nullptr);
		exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_exec_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_exec_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			duckdb_v2_data_chunk_ptr output_chunk = nullptr;
			CheckedAPICall(duckdb_v2_table_function_exec_get_output_chunk, info, &output_chunk);

			ExecInput::Inner inner {info, detail::Factory::Make<DataChunk>(output_chunk, false)};
			ExecInput input {inner};

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_exec_cb, impl, trampoline);

	exec_callback = callback;

	return *this;
}

void TableFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_table_function_builder_set_user_data, impl,
	               new TableFunctionInfo {bind_callback, init_global_callback, init_local_callback, exec_callback},
	               detail::TypedDelete<TableFunctionInfo>);

	CheckedAPICall(duckdb_v2_table_function_builder_register, detail::GetHandle(ctx), impl);
}

} // namespace duckdb_api
