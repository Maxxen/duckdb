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

class ScalarFunction::BindArgs {
public:
	duckdb_v2_scalar_function_info_ptr info = nullptr;
};

class ScalarFunction::InitArgs {
public:
	duckdb_v2_scalar_function_info_ptr info = nullptr;
};

class ScalarFunction::ExecArgs {
public:
	duckdb_v2_scalar_function_info_ptr info = nullptr;
};

struct ScalarFunctionInfo {
	ScalarFunction::BindCallback bind_callback = nullptr;
	ScalarFunction::InitCallback init_callback = nullptr;
	ScalarFunction::ExecCallback exec_callback = nullptr;
};

void *ScalarFunction::BindInput::GetBindDataInternal() const {
	void *out_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_bind_data, args.info, &out_data);
	return out_data;
}

void ScalarFunction::BindInput::SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
                                                    void (*destructor)(void *)) {
	CheckedAPICall(duckdb_v2_scalar_function_set_bind_data, args.info, data, copy, equals, destructor);
}

void *ScalarFunction::InitInput::GetBindDataInternal() const {
	void *out_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_bind_data, args.info, &out_data);
	return out_data;
}

void *ScalarFunction::InitInput::GetWorkerStateInternal() const {
	void *out_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_init_data, args.info, &out_data);
	return out_data;
}

void ScalarFunction::InitInput::SetWorkerStateInternal(void *data, void (*destructor)(void *)) {
	CheckedAPICall(duckdb_v2_scalar_function_set_init_data, args.info, data, destructor);
}

void *ScalarFunction::ExecInput::GetBindDataInternal() const {
	void *out_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_bind_data, args.info, &out_data);
	return out_data;
}

void *ScalarFunction::ExecInput::GetWorkerStateInternal() const {
	void *out_data = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_init_data, args.info, &out_data);
	return out_data;
}

auto ScalarFunction::ExecInput::GetInputChunk() const -> DataChunk {
	duckdb_v2_data_chunk_ptr chunk = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_input_chunk, args.info, &chunk);
	return detail::Factory::Make<DataChunk>(chunk, false);
}

auto ScalarFunction::ExecInput::GetResultVector() const -> Vector {
	duckdb_v2_vector_ptr vec = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_get_result_vector, args.info, &vec);
	return detail::Factory::Make<Vector>(vec);
}

auto ScalarFunction::SetBindCallback(BindCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, impl, nullptr);

		bind_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data;
			CheckedAPICall(duckdb_v2_scalar_function_get_user_data, info, &user_data);

			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			BindArgs args {info};

			BindInput input(args);

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

	static auto trampoline = [](duckdb_v2_scalar_function_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data;
			CheckedAPICall(duckdb_v2_scalar_function_get_user_data, info, &user_data);

			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			InitArgs args {info};

			InitInput input(args);

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

	static auto trampoline = [](duckdb_v2_scalar_function_info_ptr info, duckdb_v2_context_ptr ctx,
	                            duckdb_v2_error_info_ptr *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data;
			CheckedAPICall(duckdb_v2_scalar_function_get_user_data, info, &user_data);

			const auto &function = *static_cast<ScalarFunctionInfo *>(user_data);

			ExecArgs args {info};

			ExecInput input(args);

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

} // namespace duckdb_api
