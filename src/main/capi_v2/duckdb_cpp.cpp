// Include the C++ API header (which includes the C API header)
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"

namespace duckdb_api {

//----------------------------------------------------------------------------------------------------------------------
// Internal Implementation Details
//----------------------------------------------------------------------------------------------------------------------

namespace detail {

// Map each C++ wrapper to its underlying C-API handle type. Declared here in the .cpp (not the header).
// This makes handle types stay private to the implementation and consumed by Handle<TYPE>::handle().

template <>
struct HandleTraits<DatabaseOption> {
	using handle = duckdb_v2_option_handle;
};
template <>
struct HandleTraits<FileHandle> {
	using handle = duckdb_v2_file_handle_handle;
};
template <>
struct HandleTraits<FileSystem> {
	using handle = duckdb_v2_file_system_handle;
};
template <>
struct HandleTraits<Context> {
	using handle = duckdb_v2_context_handle;
};
template <>
struct HandleTraits<Connection> {
	using handle = duckdb_v2_connection_handle;
};
template <>
struct HandleTraits<SqlStatement> {
	using handle = duckdb_v2_sql_statement_handle;
};
template <>
struct HandleTraits<StatementIterator> {
	using handle = duckdb_v2_statement_iterator_handle;
};
template <>
struct HandleTraits<Database> {
	using handle = duckdb_v2_database_handle;
};
template <>
struct HandleTraits<Environment> {
	using handle = duckdb_v2_environment_handle;
};
template <>
struct HandleTraits<LogicalType> {
	using handle = duckdb_v2_logical_type_handle;
};
template <>
struct HandleTraits<Value> {
	using handle = duckdb_v2_value_handle;
};
template <>
struct HandleTraits<Vector> {
	using handle = duckdb_v2_vector_handle;
};
template <>
struct HandleTraits<DataChunk> {
	using handle = duckdb_v2_data_chunk_handle;
};
template <>
struct HandleTraits<ColumnDataCollection> {
	using handle = duckdb_v2_column_data_collection_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::AppendState> {
	using handle = duckdb_v2_column_data_collection_append_state_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::ScanState> {
	using handle = duckdb_v2_column_data_collection_scan_state_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::SharedScanState> {
	using handle = duckdb_v2_column_data_collection_shared_scan_state_handle;
};
template <>
struct HandleTraits<ColumnDataCollection::WorkerScanState> {
	using handle = duckdb_v2_column_data_collection_worker_scan_state_handle;
};
template <>
struct HandleTraits<LogStorage> {
	using handle = duckdb_v2_log_storage_builder_handle;
};
template <>
struct HandleTraits<QueryResult> {
	using handle = duckdb_v2_result_handle;
};
template <>
struct HandleTraits<ScalarFunction> {
	using handle = duckdb_v2_scalar_function_builder_handle;
};
template <>
struct HandleTraits<AggregateFunction> {
	using handle = duckdb_v2_aggregate_function_builder_handle;
};
template <>
struct HandleTraits<TableFunction> {
	using handle = duckdb_v2_table_function_builder_handle;
};
template <>
struct HandleTraits<CopyFunction> {
	using handle = duckdb_v2_copy_function_builder_handle;
};
template <>
struct HandleTraits<CastFunction> {
	using handle = duckdb_v2_cast_function_builder_handle;
};
template <>
struct HandleTraits<CustomType> {
	using handle = duckdb_v2_custom_type_builder_handle;
};

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Error Handling Helpers
//----------------------------------------------------------------------------------------------------------------------

namespace {

// Perform a DuckDB C-API call, setup an error info object, and throw an exception if it fails.
// This is used to simplify error handling in the C++ wrapper.
template <class F, class... ARGS>
void CheckedAPICall(F &&func, ARGS &&... args) {
	duckdb_v2_error_info_handle err = nullptr;
	const auto code = func(std::forward<ARGS>(args)..., &err);
	if (code != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_str message_view = {nullptr, 0};
		duckdb_v2_str raw_view = {nullptr, 0};
		if (err) {
			duckdb_v2_error_info_get_text(err, &message_view);
			duckdb_v2_error_info_get_raw_message(err, &raw_view);
		}
		std::string message = message_view.ptr ? std::string(message_view.ptr, message_view.len) : "unknown error";
		std::string raw = raw_view.ptr ? std::string(raw_view.ptr, raw_view.len) : "";
		duckdb_v2_error_info_destroy(&err);
		throw Exception(code, std::move(message), std::move(raw));
	}
}

// Borrow a std::string as a length-delimited view for the C API.
duckdb_v2_str ToStr(const std::string &s) {
	return duckdb_v2_str {s.data(), s.size()};
}
// View a borrowed C-API string as a std::string_view ({NULL,0} -> empty).
std::string_view FromStr(duckdb_v2_str s) {
	return s.ptr ? std::string_view(s.ptr, s.len) : std::string_view();
}

// Catch any exceptions and propagate them via the error info out-parameter, returning an appropriate error code.
template <class T>
DUCKDB_V2_API_CALL_t WithExceptionGuard(duckdb_v2_error_info_handle *err, T callback) {
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
		duckdb_v2_error_info_set_text(*err, ToStr(text));
	}

	return code;
}

} // namespace

//---------------------------------------------------------------------------
// Environment
//---------------------------------------------------------------------------

Environment::Environment() {
	duckdb_v2_environment_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_create_environment, &_h);
	impl = _h;
}

Environment::~Environment() {
	auto _h = handle();
	duckdb_v2_destroy_environment(&_h);
}

size_t Environment::GetOpenDatabaseCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_environment_database_count, handle(), &count);
	return static_cast<size_t>(count);
}

Database Environment::Open(const std::string &path) {
	duckdb_v2_database_handle db = nullptr;
	CheckedAPICall(duckdb_v2_open, handle(), ToStr(path), nullptr, static_cast<idx_t>(0), &db);
	return detail::Factory::Make<Database>(db);
}

//---------------------------------------------------------------------------
// Database Option
//---------------------------------------------------------------------------

DatabaseOption::DatabaseOption(void *impl) : detail::Handle<DatabaseOption>(impl) {
}

DatabaseOption::DatabaseOption(const std::string &name, const std::string &value) {
	duckdb_v2_option_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_option_create, ToStr(name), ToStr(value), &_h);
	impl = _h;
}

std::string_view DatabaseOption::GetName() const {
	duckdb_v2_str name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_name, handle(), &name);
	return FromStr(name);
}

std::string_view DatabaseOption::GetValue() const {
	duckdb_v2_str value = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_setting, handle(), &value);
	return FromStr(value);
}

std::string_view DatabaseOption::GetDefaultValue() const {
	duckdb_v2_str default_value = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_default_setting, handle(), &default_value);
	return FromStr(default_value);
}

std::string_view DatabaseOption::GetDescription() const {
	duckdb_v2_str description = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_description, handle(), &description);
	return FromStr(description);
}

size_t DatabaseOption::GetAliasCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_option_get_alias_count, handle(), &count);
	return static_cast<size_t>(count);
}

std::string_view DatabaseOption::GetAliasByIndex(size_t index) const {
	duckdb_v2_str alias = {nullptr, 0};
	CheckedAPICall(duckdb_v2_option_get_alias, handle(), static_cast<idx_t>(index), &alias);
	return FromStr(alias);
}

DatabaseOption::~DatabaseOption() {
	auto _h = handle();
	duckdb_v2_option_destroy(&_h);
}

//---------------------------------------------------------------------------
// Database
//---------------------------------------------------------------------------

Database::Database(void *impl) : detail::Handle<Database>(impl) {
}

Database::~Database() {
	auto _h = handle();
	duckdb_v2_close(&_h);
}

size_t Database::GetOptionCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_database_option_get_count, handle(), &count);
	return static_cast<size_t>(count);
}

DatabaseOption Database::GetOptionByIndex(size_t index) const {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_database_option_get_by_index, handle(), static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

void Database::SetOption(const DatabaseOption &option) {
	CheckedAPICall(duckdb_v2_database_option_set, handle(), option.handle());
}

Connection Database::Connect() {
	duckdb_v2_connection_handle conn = nullptr;
	CheckedAPICall(duckdb_v2_connect, handle(), &conn);
	return detail::Factory::Make<Connection>(conn, true);
}

//---------------------------------------------------------------------------
// Connection
//---------------------------------------------------------------------------

Connection::Connection(void *impl, bool owned) : detail::Handle<Connection>(impl), owned(owned) {
}

Connection::~Connection() {
	if (owned) {
		auto _h = handle();
		duckdb_v2_disconnect(&_h);
	}
}

size_t Connection::GetOptionCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_connection_option_get_count, handle(), &count);
	return static_cast<size_t>(count);
}

DatabaseOption Connection::GetOptionByIndex(size_t index) const {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_connection_option_get_by_index, handle(), static_cast<idx_t>(index), &option);
	return detail::Factory::Make<DatabaseOption>(option);
}

void Connection::SetOption(const DatabaseOption &option) {
	// TODO: Pass scope
	CheckedAPICall(duckdb_v2_connection_option_set, handle(), option.handle(), DUCKDB_V2_SETTING_SCOPE_AUTOMATIC);
}

void Connection::WithTransaction(std::function<void(const Context &ctx)> callback) {
	static auto trampoline = [](duckdb_v2_context_handle ctx, void *user_data, duckdb_v2_error_info_handle *err) {
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

	CheckedAPICall(duckdb_v2_connection_execute_with_context, handle(), trampoline, &callback);
}

//----------------------------------------------------------------------------------------------------------------------
// SQL statements
//----------------------------------------------------------------------------------------------------------------------

SqlStatement::SqlStatement(void *impl) : detail::Handle<SqlStatement>(impl) {
}

SqlStatement::~SqlStatement() {
	auto _h = handle();
	duckdb_v2_sql_statement_destroy(&_h);
}

StatementIterator::StatementIterator(void *impl) : detail::Handle<StatementIterator>(impl) {
}

StatementIterator::~StatementIterator() {
	auto _h = handle();
	duckdb_v2_statement_iterator_destroy(&_h);
}

auto StatementIterator::Next() -> SqlStatement {
	duckdb_v2_sql_statement_handle statement = nullptr;
	CheckedAPICall(duckdb_v2_statement_iterator_next, handle(), &statement);
	// An empty handle marks exhaustion.
	return detail::Factory::Make<SqlStatement>(statement);
}

StatementIterator Connection::ParseSQL(const char *sql) {
	duckdb_v2_statement_iterator_handle iterator = nullptr;
	CheckedAPICall(duckdb_v2_parse_sql, handle(), sql, &iterator);
	return detail::Factory::Make<StatementIterator>(iterator);
}

QueryResult Connection::Query(SqlStatement statement) {
	auto stmt = static_cast<duckdb_v2_sql_statement_handle>(statement.release());
	duckdb_v2_result_handle result = nullptr;
	try {
		CheckedAPICall(duckdb_v2_connection_query, handle(), &stmt, &result);
	} catch (...) {
		// The busy and null refusals do not consume the statement; the
		// by-value parameter is gone either way, so free the handle.
		duckdb_v2_sql_statement_destroy(&stmt);
		throw;
	}
	return detail::Factory::Make<QueryResult>(result);
}

QueryResult Connection::Query(const std::string &sql) {
	auto statements = ParseSQL(sql);
	auto statement = statements.Next();
	if (!statement || statements.Next()) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "Query expects exactly one statement; use ParseSQL for multi-statement input");
	}
	return Query(std::move(statement));
}

void Connection::Interrupt() {
	CheckedAPICall(duckdb_v2_connection_interrupt, handle());
}

Connection::QueryProgress Connection::GetQueryProgress() const {
	// Flatten the C snapshot object into the POD struct: capture, read the
	// accessors, destroy.
	duckdb_v2_query_progress_handle snapshot = nullptr;
	CheckedAPICall(duckdb_v2_connection_query_progress, handle(), &snapshot);
	QueryProgress progress {};
	try {
		CheckedAPICall(duckdb_v2_query_progress_get_percentage, snapshot, &progress.percentage);
		CheckedAPICall(duckdb_v2_query_progress_get_rows_processed, snapshot, &progress.rows_processed);
		CheckedAPICall(duckdb_v2_query_progress_get_total_rows_to_process, snapshot, &progress.total_rows_to_process);
	} catch (...) {
		duckdb_v2_query_progress_destroy(&snapshot);
		throw;
	}
	duckdb_v2_query_progress_destroy(&snapshot);
	return progress;
}

void Connection::Log(LogLevel level, const std::string &message) const noexcept {
	CheckedAPICall(duckdb_v2_connection_log, handle(), static_cast<DUCKDB_V2_LOG_LEVEL>(level), ToStr(message));
}

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

Context::Context(void *impl) : detail::Handle<Context>(impl) {
}

Context::~Context() {
	// Context lifetime is managed by DuckDB, so we don't destroy the handle here
}

FileSystem Context::GetFileSystem() const {
	duckdb_v2_file_system_handle fs = nullptr;
	CheckedAPICall(duckdb_v2_file_system_get_from_context, handle(), &fs);
	return detail::Factory::Make<FileSystem>(fs);
}

void Context::Log(LogLevel level, const std::string &message) const noexcept {
	CheckedAPICall(duckdb_v2_context_log, handle(), static_cast<DUCKDB_V2_LOG_LEVEL>(level), ToStr(message));
}

//----------------------------------------------------------------------------------------------------------------------
// File System
//----------------------------------------------------------------------------------------------------------------------

FileSystem::FileSystem(void *impl) : detail::Handle<FileSystem>(impl) {
}

FileSystem::~FileSystem() {
	// FileSystem lifetime is managed by DuckDB, so we don't destroy the handle here
}

FileHandle FileSystem::OpenFile(const std::string &path, FileFlags flags) const {
	duckdb_v2_file_handle_handle result = nullptr;

	// TODO: Verify file flags
	CheckedAPICall(duckdb_v2_file_system_open, handle(), ToStr(path), static_cast<uint64_t>(flags), &result);
	return detail::Factory::Make<FileHandle>(result);
}

//----------------------------------------------------------------------------------------------------------------------
// File Handle
//----------------------------------------------------------------------------------------------------------------------

FileHandle::FileHandle(void *impl) : detail::Handle<FileHandle>(impl) {
}

FileHandle::~FileHandle() {
	auto _h = handle();
	duckdb_v2_file_handle_destroy(&_h);
}

void FileHandle::Close() {
	CheckedAPICall(duckdb_v2_file_handle_close, handle());
}

void FileHandle::Sync() {
	CheckedAPICall(duckdb_v2_file_handle_sync, handle());
}

void FileHandle::Seek(int64_t position) {
	CheckedAPICall(duckdb_v2_file_handle_seek, handle(), position);
}

auto FileHandle::Tell() const -> int64_t {
	int64_t position = 0;
	CheckedAPICall(duckdb_v2_file_handle_tell, handle(), &position);
	return position;
}

auto FileHandle::Size() const -> int64_t {
	int64_t size = 0;
	CheckedAPICall(duckdb_v2_file_handle_size, handle(), &size);
	return size;
}

auto FileHandle::Read(void *buffer, int64_t size) -> int64_t {
	int64_t bytes_read = 0;
	CheckedAPICall(duckdb_v2_file_handle_read, handle(), buffer, size, &bytes_read);
	return bytes_read;
}

auto FileHandle::Write(const void *buffer, int64_t size) -> int64_t {
	int64_t bytes_written = 0;
	CheckedAPICall(duckdb_v2_file_handle_write, handle(), buffer, size, &bytes_written);
	return bytes_written;
}

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------

LogicalType::LogicalType(void *impl) : detail::Handle<LogicalType>(impl) {
}

bool LogicalType::operator==(const LogicalType &other) const {
	if (handle() == other.handle()) {
		return true; // same handle means same logical type
	}
	bool result = false;
	CheckedAPICall(duckdb_v2_logical_type_is_equal, handle(), other.handle(), &result);
	return result;
}

std::string_view LogicalType::GetAlias() const {
	duckdb_v2_str alias = {nullptr, 0};
	CheckedAPICall(duckdb_v2_logical_type_get_alias, handle(), &alias);
	return FromStr(alias);
}

LogicalType LogicalType::INTEGER() {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, &type);
	return detail::Factory::Make<LogicalType>(type);
}

LogicalType LogicalType::VARCHAR() {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, &type);
	return detail::Factory::Make<LogicalType>(type);
}

LogicalType LogicalType::BIGINT() {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &type);
	return detail::Factory::Make<LogicalType>(type);
}

LogicalType LogicalType::WithAlias(std::string_view alias) const {
	duckdb_v2_logical_type_handle new_type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_with_alias, handle(), ToStr(std::string(alias)), &new_type);
	return detail::Factory::Make<LogicalType>(new_type);
}

LogicalType::~LogicalType() {
	auto _h = handle();
	duckdb_v2_logical_type_destroy(&_h);
}

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------

Value::Value(void *impl) : detail::Handle<Value>(impl) {
}

Value::~Value() {
	auto _h = handle();
	duckdb_v2_value_destroy(&_h);
}

auto Value::IsNull() const -> bool {
	bool is_null = false;
	CheckedAPICall(duckdb_v2_value_is_null, handle(), &is_null);
	return is_null;
}

auto Value::GetLogicalType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_logical_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Value::ToString() const -> std::string {
	char *str = nullptr;
	CheckedAPICall(duckdb_v2_value_to_string, handle(), &str);
	auto result = std::string(str);
	free(str); // TODO: This should use something like duckdb_v2_free to avoid potential cross-allocator issues.
	return result;
}

auto Value::AsBool() const -> bool {
	bool result = false;
	CheckedAPICall(duckdb_v2_value_get_bool, handle(), &result);
	return result;
}

auto Value::AsI8() const -> int8_t {
	int8_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int8, handle(), &result);
	return result;
}

auto Value::AsI16() const -> int16_t {
	int16_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int16, handle(), &result);
	return result;
}

auto Value::AsU8() const -> uint8_t {
	uint8_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint8, handle(), &result);
	return result;
}

auto Value::AsU16() const -> uint16_t {
	uint16_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint16, handle(), &result);
	return result;
}

auto Value::AsI32() const -> int32_t {
	int32_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int32, handle(), &result);
	return result;
}

auto Value::AsU32() const -> uint32_t {
	uint32_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint32, handle(), &result);
	return result;
}

auto Value::AsI64() const -> int64_t {
	int64_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_int64, handle(), &result);
	return result;
}

auto Value::AsU64() const -> uint64_t {
	uint64_t result = 0;
	CheckedAPICall(duckdb_v2_value_get_uint64, handle(), &result);
	return result;
}

auto Value::AsF32() const -> float {
	float result = 0;
	CheckedAPICall(duckdb_v2_value_get_float, handle(), &result);
	return result;
}

auto Value::AsF64() const -> double {
	double result = 0;
	CheckedAPICall(duckdb_v2_value_get_double, handle(), &result);
	return result;
}

auto Value::AsVarchar() const -> std::string_view {
	duckdb_v2_str str = {nullptr, 0};
	CheckedAPICall(duckdb_v2_value_get_varchar, handle(), &str);
	return FromStr(str);
}

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------
Vector::Vector(void *impl) : detail::Handle<Vector>(impl) {
}

Vector::~Vector() {
	/* Vectors are always borrowed, so we don't destroy the handle here */
}

auto Vector::GetDataMutable() -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_data_mutable, handle(), &data);
	return data;
}

auto Vector::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_vector_get_child_count, handle(), &count);
	return count;
}

auto Vector::GetChild(idx_t index) const -> Vector {
	duckdb_v2_vector_handle child = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_child, handle(), index, &child);
	return detail::Factory::Make<Vector>(child);
}

auto Vector::GetLogicalType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_logical_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Vector::Flatten() const -> void {
	CheckedAPICall(duckdb_v2_vector_flatten, handle());
}

auto Vector::GetSize() const -> idx_t {
	idx_t size = 0;
	CheckedAPICall(duckdb_v2_vector_get_size, handle(), &size);
	return size;
}

auto Vector::SetSize(idx_t size) -> void {
	CheckedAPICall(duckdb_v2_vector_set_size, handle(), size);
}

//----------------------------------------------------------------------------------------------------------------------
// Data Chunk
//----------------------------------------------------------------------------------------------------------------------
DataChunk::DataChunk(const Context &context, const std::vector<LogicalType> &types) {
	// LogicalType is a Handle (with a vtable), so its storage is not layout-compatible with a raw
	// duckdb_v2_logical_type_handle array. Extract the underlying handles into a contiguous buffer.
	std::vector<duckdb_v2_logical_type_handle> type_pointers;
	type_pointers.reserve(types.size());
	for (const auto &type : types) {
		type_pointers.push_back(type.handle());
	}

	// TODO: Pass context to create buffer-managed data chunks.
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_create, type_pointers.data(), type_pointers.size(), &chunk);

	impl = chunk;
	owned = true;
}

DataChunk::DataChunk(void *impl, bool owned) : detail::Handle<DataChunk>(impl), owned(owned) {
}

DataChunk::~DataChunk() {
	if (owned) {
		auto _h = handle();
		duckdb_v2_data_chunk_destroy(&_h);
	}
}

auto DataChunk::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_size, handle(), &count);
	return count;
}

auto DataChunk::GetVectorCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector_count, handle(), &count);
	return count;
}

auto DataChunk::GetVector(idx_t index) const -> Vector {
	duckdb_v2_vector_handle vector = nullptr;
	CheckedAPICall(duckdb_v2_data_chunk_get_vector, handle(), index, &vector);
	return detail::Factory::Make<Vector>(vector);
}

//----------------------------------------------------------------------------------------------------------------------
// Query Result
//----------------------------------------------------------------------------------------------------------------------

QueryResult::QueryResult(void *impl) : detail::Handle<QueryResult>(impl) {
}

QueryResult::~QueryResult() {
	auto _h = handle();
	duckdb_v2_result_destroy(&_h);
}

auto QueryResult::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_result_column_count, handle(), &count);
	return count;
}

auto QueryResult::GetColumnName(idx_t index) const -> std::string_view {
	duckdb_v2_str name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_result_column_name, handle(), index, &name);
	return FromStr(name);
}

auto QueryResult::GetColumnType(idx_t index) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_result_column_logical_type, handle(), index, &type);
	return detail::Factory::Make<LogicalType>(type);
}

// StepStatus mirrors DUCKDB_V2_RESULT_STEP_STATUS numerically; trip here if
// either side is renumbered.
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::WAITING) == DUCKDB_V2_RESULT_STEP_STATUS_WAITING,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::CHUNK) == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::FINISHED) == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");
static_assert(static_cast<uint8_t>(QueryResult::StepStatus::CANCELLED) == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED,
              "StepStatus must mirror DUCKDB_V2_RESULT_STEP_STATUS");

auto QueryResult::Step() -> StepResult {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	CheckedAPICall(duckdb_v2_result_step, handle(), &chunk, &status);
	return StepResult {static_cast<StepStatus>(status), detail::Factory::Make<DataChunk>(chunk, chunk != nullptr)};
}

auto QueryResult::Wait() -> void {
	CheckedAPICall(duckdb_v2_result_wait, handle());
}

auto QueryResult::FetchChunk() -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_result_fetch_chunk, handle(), &chunk);
	// An empty handle marks end-of-stream.
	return detail::Factory::Make<DataChunk>(chunk, chunk != nullptr);
}

auto QueryResult::Drain() -> idx_t {
	idx_t rows_changed = 0;
	CheckedAPICall(duckdb_v2_result_drain, handle(), &rows_changed);
	return rows_changed;
}

//----------------------------------------------------------------------------------------------------------------------
// Column Data Collection
//----------------------------------------------------------------------------------------------------------------------
ColumnDataCollection::ColumnDataCollection(void *impl) : detail::Handle<ColumnDataCollection>(impl) {
}

ColumnDataCollection::ColumnDataCollection(const Context &context, const std::vector<LogicalType> &types) {
	// LogicalType is a Handle (with a vtable), so its storage is not layout-compatible with a raw
	// duckdb_v2_logical_type_handle array. Extract the underlying handles into a contiguous buffer.
	std::vector<duckdb_v2_logical_type_handle> type_pointers;
	type_pointers.reserve(types.size());
	for (const auto &type : types) {
		type_pointers.push_back(type.handle());
	}

	auto _h = handle();
	CheckedAPICall(duckdb_v2_column_data_collection_create, context.handle(), type_pointers.data(),
	               type_pointers.size(), &_h);
	impl = _h;
}

ColumnDataCollection::~ColumnDataCollection() {
	auto _h = handle();
	duckdb_v2_column_data_collection_destroy(&_h);
}

auto ColumnDataCollection::GetRowCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_column_data_collection_row_count, handle(), &count);
	return count;
}

auto ColumnDataCollection::Combine(ColumnDataCollection &&other) -> void {
	auto _other = other.handle();
	CheckedAPICall(duckdb_v2_column_data_collection_combine, handle(), &_other);
	other.impl = nullptr;
}

ColumnDataCollection::ScanState::ScanState(void *impl) : detail::Handle<ScanState>(impl) {
}
ColumnDataCollection::ScanState::~ScanState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_scan_state_destroy(&_h);
}

auto ColumnDataCollection::GetSingleScanState() -> ScanState {
	duckdb_v2_column_data_collection_scan_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_scan_state_create, handle(), &state);
	return detail::Factory::Make<ScanState>(state);
}

auto ColumnDataCollection::Scan(ScanState &state, DataChunk &chunk) -> bool {
	auto did_produce_chunk = false;
	CheckedAPICall(duckdb_v2_column_data_collection_scan, handle(), state.handle(), chunk.handle(), &did_produce_chunk);
	return did_produce_chunk;
}

ColumnDataCollection::SharedScanState::SharedScanState(void *impl) : detail::Handle<SharedScanState>(impl) {
}
ColumnDataCollection::SharedScanState::~SharedScanState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_shared_scan_state_destroy(&_h);
}

auto ColumnDataCollection::GetSharedScanState() -> SharedScanState {
	duckdb_v2_column_data_collection_shared_scan_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_shared_scan_state_create, handle(), &state);
	return detail::Factory::Make<SharedScanState>(state);
}

ColumnDataCollection::WorkerScanState::WorkerScanState(void *impl) : detail::Handle<WorkerScanState>(impl) {
}
ColumnDataCollection::WorkerScanState::~WorkerScanState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_worker_scan_state_destroy(&_h);
}

auto ColumnDataCollection::GetWorkerScanState() -> WorkerScanState {
	duckdb_v2_column_data_collection_worker_scan_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_worker_scan_state_create, handle(), &state);
	return detail::Factory::Make<WorkerScanState>(state);
}

auto ColumnDataCollection::Scan(SharedScanState &shared_state, WorkerScanState &worker_state, DataChunk &chunk)
    -> bool {
	auto did_produce_chunk = false;
	CheckedAPICall(duckdb_v2_column_data_collection_parallel_scan, handle(), shared_state.handle(),
	               worker_state.handle(), chunk.handle(), &did_produce_chunk);
	return did_produce_chunk;
}

ColumnDataCollection::AppendState::AppendState(void *impl) : detail::Handle<AppendState>(impl) {
}
ColumnDataCollection::AppendState::~AppendState() {
	auto _h = handle();
	duckdb_v2_column_data_collection_append_state_destroy(&_h);
}

auto ColumnDataCollection::GetAppendState() -> AppendState {
	duckdb_v2_column_data_collection_append_state_handle state = nullptr;
	CheckedAPICall(duckdb_v2_column_data_collection_append_state_create, handle(), &state);
	return detail::Factory::Make<AppendState>(state);
}

auto ColumnDataCollection::Append(AppendState &state, const DataChunk &chunk) -> void {
	CheckedAPICall(duckdb_v2_column_data_collection_append, handle(), state.handle(), chunk.handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Log Storage
//----------------------------------------------------------------------------------------------------------------------
class LogStorageInfo {
public:
	LogStorage::LogCallback log_callback = nullptr;
	detail::UserData user_data;
};

LogStorage::LogStorage(const Context &ctx) {
	duckdb_v2_log_storage_builder_handle handle = nullptr;
	CheckedAPICall(duckdb_v2_log_storage_builder_create, ctx.handle(), &handle);
	impl = handle;
}

LogStorage::~LogStorage() {
	auto _h = handle();
	duckdb_v2_log_storage_builder_destroy(&_h);
}

auto LogStorage::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto LogStorage::Register(const Context &ctx) -> void {
	auto info = std::make_unique<LogStorageInfo>();
	info->log_callback = callback;
	info->user_data = std::move(user_data);

	CheckedAPICall(duckdb_v2_log_storage_builder_set_user_data, handle(), info.release(),
	               detail::TypedDelete<LogStorageInfo>);
	CheckedAPICall(duckdb_v2_log_storage_builder_register, ctx.handle(), handle());
}

auto LogStorage::SetName(const std::string &name) & -> LogStorage & {
	CheckedAPICall(duckdb_v2_log_storage_builder_set_name, handle(), ToStr(name));
	return *this;
}

class LogStorage::LogEntry::Inner {
public:
	void *user_data = nullptr;
	int64_t timestamp = 0;
	DUCKDB_V2_LOG_LEVEL level = DUCKDB_V2_LOG_LEVEL_DEBUG;
	const char *log_type = nullptr;
	const char *log_message = nullptr;
};

auto LogStorage::LogEntry::GetLogLevel() const -> LogLevel {
	return static_cast<LogLevel>(inner.level);
}

auto LogStorage::LogEntry::GetLogMessage() const -> const char * {
	return inner.log_message;
}

auto LogStorage::LogEntry::GetLogType() const -> const char * {
	return inner.log_type;
}

auto LogStorage::LogEntry::GetLogTimestamp() const -> int64_t {
	return inner.timestamp;
}

auto LogStorage::LogEntry::GetUserData() const -> void * {
	return inner.user_data;
}

auto LogStorage::SetLogCallback(LogCallback cb) & -> LogStorage & {
	if (cb == nullptr) {
		CheckedAPICall(duckdb_v2_log_storage_builder_set_log_callback, handle(), nullptr);
		callback = nullptr;
		return *this;
	}

	auto trampoline = [](void *user_data, int64_t timestamp, DUCKDB_V2_LOG_LEVEL level, duckdb_v2_str log_type,
	                     duckdb_v2_str log_message, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &info = *static_cast<LogStorageInfo *>(user_data);

			if (!info.log_callback) {
				return;
			}

			// The engine backs these views with null-terminated std::string storage,
			// so .ptr is a valid C string for the duration of the callback.
			LogEntry::Inner inner {user_data = info.user_data.get(), timestamp, level, log_type.ptr, log_message.ptr};
			LogEntry entry(inner);

			info.log_callback(entry);
		});
	};

	CheckedAPICall(duckdb_v2_log_storage_builder_set_log_callback, handle(), trampoline);
	callback = cb;
	return *this;
}

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

ScalarFunction::ScalarFunction(const Context &context) {
	duckdb_v2_scalar_function_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_scalar_function_builder_create, context.handle(), &_h);
	impl = _h;
}

ScalarFunction::~ScalarFunction() {
	auto _h = handle();
	duckdb_v2_scalar_function_builder_destroy(&_h);
}

auto ScalarFunction::SetName(const std::string &name) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_name, handle(), ToStr(name));
	return *this;
}

auto ScalarFunction::AddParameter(const std::string &name, const LogicalType &type) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_add_parameter, handle(), ToStr(name), type.handle());
	return *this;
}

auto ScalarFunction::SetReturnType(const LogicalType &type) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_return_type, handle(), type.handle());
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

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, handle(), nullptr);

		bind_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_bind_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<BindInput>(args);

			// Now call the user callback
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, handle(), trampoline);

	// And set the bind callback. This will be set in the user_data once the function is registered.
	bind_callback = callback;

	return *this;
}
auto ScalarFunction::SetInitCallback(InitCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_init_callback, handle(), nullptr);

		init_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_init_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<InitInput>(args);

			// Now call the user callback
			function.init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_init_callback, handle(), trampoline);

	// And set the init callback. This will be set in the user_data once the function is registered.
	init_callback = callback;

	return *this;
}
auto ScalarFunction::SetExecCallback(ExecCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_exec_callback, handle(), nullptr);

		exec_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_exec_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<ExecInput>(args);

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_exec_callback, handle(), trampoline);

	// And set the exec callback. This will be set in the user_data once the function is registered.
	exec_callback = callback;

	return *this;
}

void ScalarFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_user_data, handle(),
	               new ScalarFunctionInfo {bind_callback, init_callback, exec_callback},
	               detail::TypedDelete<ScalarFunctionInfo>);

	CheckedAPICall(duckdb_v2_scalar_function_builder_register, ctx.handle(), handle());
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
	duckdb_v2_aggregate_function_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_create, ctx.handle(), &_h);
	impl = _h;
}

AggregateFunction::~AggregateFunction() {
	auto _h = handle();
	duckdb_v2_aggregate_function_builder_destroy(&_h);
}

auto AggregateFunction::SetName(const std::string &name) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_name, handle(), ToStr(name));
	return *this;
}

auto AggregateFunction::AddParameter(const std::string &name, const LogicalType &type) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_add_parameter, handle(), ToStr(name), type.handle());
	return *this;
}

auto AggregateFunction::SetReturnType(const LogicalType &type) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_return_type, handle(), type.handle());
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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_size_callback, handle(), nullptr);
		size_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_size_args *args, duckdb_v2_error_info_handle *err) {
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

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_size_callback, handle(), trampoline);

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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_init_callback, handle(), nullptr);
		initialize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_init_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			InitializeInput::Inner inner;
			inner.state = args->state;

			InitializeInput input {inner};

			// Now call the user callback
			function.initialize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_init_callback, handle(), trampoline);

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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_update_callback, handle(), nullptr);
		update_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_update_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			UpdateInput::Inner inner {args->count, args->states, detail::Factory::Make<DataChunk>(args->input, false)};

			UpdateInput input {inner};

			// Now call the user callback
			function.update_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_update_callback, handle(), trampoline);

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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_combine_callback, handle(), nullptr);
		combine_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_combine_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			CombineInput::Inner inner {args->count, args->sources, args->targets};

			CombineInput input {inner};

			// Now call the user callback
			function.combine_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_combine_callback, handle(), trampoline);

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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_finalize_callback, handle(), nullptr);
		finalize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_finalize_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			FinalizeInput::Inner inner {args->count, args->states, detail::Factory::Make<Vector>(args->result),
			                            args->result_offset};

			FinalizeInput input {inner};

			// Now call the user callback
			function.finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_finalize_callback, handle(), trampoline);

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
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_destroy_callback, handle(), nullptr);
		destroy_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_destroy_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			DestroyInput::Inner inner {args->count, args->states};
			DestroyInput input {inner};

			// Now call the user callback
			function.destroy_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_destroy_callback, handle(), trampoline);

	destroy_callback = callback;

	return *this;
}

void AggregateFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_user_data, handle(),
	               new AggregateFunctionInfo {size_callback, initialize_callback, update_callback, combine_callback,
	                                          finalize_callback, destroy_callback},
	               detail::TypedDelete<AggregateFunctionInfo>);

	CheckedAPICall(duckdb_v2_aggregate_function_builder_register, ctx.handle(), handle());
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
	duckdb_v2_table_function_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_table_function_builder_create, ctx.handle(), &_h);
	impl = _h;
}

TableFunction::~TableFunction() {
	auto _h = handle();
	duckdb_v2_table_function_builder_destroy(&_h);
}

auto TableFunction::SetName(const std::string &name) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_set_name, handle(), ToStr(name));
	return *this;
}

auto TableFunction::AddParameter(const LogicalType &type) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_add_parameter, handle(), type.handle());
	return *this;
}

auto TableFunction::AddNamedParameter(const std::string &name, const LogicalType &type) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_add_named_parameter, handle(), ToStr(name), type.handle());
	return *this;
}

class TableFunction::BindInput::Inner {
public:
	duckdb_v2_table_function_bind_info_handle info = nullptr;
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
	CheckedAPICall(duckdb_v2_table_function_bind_add_result_column, inner.info, ToStr(name), type.handle());
}

auto TableFunction::BindInput::GetParameter(idx_t index) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_parameter, inner.info, index, &value);
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::GetNamedParameter(const std::string &name) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_named_parameter, inner.info, ToStr(name), &value);
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::TryGetParameter(idx_t index) const -> std::optional<Value> {
	duckdb_v2_value_handle value = nullptr;
	const auto res = duckdb_v2_table_function_bind_get_parameter(inner.info, index, &value, nullptr);
	if (res != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::BindInput::TryGetNamedParameter(const std::string &name) const -> std::optional<Value> {
	duckdb_v2_value_handle value = nullptr;

	const auto res = duckdb_v2_table_function_bind_get_named_parameter(inner.info, ToStr(name), &value, nullptr);
	if (res != DUCKDB_V2_ERROR_NONE) {
		return std::nullopt;
	}
	return detail::Factory::Make<Value>(value);
}

auto TableFunction::SetBindCallback(BindCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle ctx,
	                            duckdb_v2_error_info_handle *err) {
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

	CheckedAPICall(duckdb_v2_table_function_builder_set_bind_callback, handle(), trampoline);

	bind_callback = callback;

	return *this;
}

class TableFunction::InitGlobalInput::Inner {
public:
	duckdb_v2_table_function_init_info_handle info = nullptr;
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
		CheckedAPICall(duckdb_v2_table_function_builder_set_init_global_callback, handle(), nullptr);
		init_global_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
	                            duckdb_v2_error_info_handle *err) {
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

	CheckedAPICall(duckdb_v2_table_function_builder_set_init_global_callback, handle(), trampoline);

	init_global_callback = callback;

	return *this;
}

class TableFunction::InitLocalInput::Inner {
public:
	duckdb_v2_table_function_init_info_handle info = nullptr;
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
		CheckedAPICall(duckdb_v2_table_function_builder_set_init_local_callback, handle(), nullptr);
		init_local_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_init_info_handle info, duckdb_v2_context_handle ctx,
	                            duckdb_v2_error_info_handle *err) {
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

	CheckedAPICall(duckdb_v2_table_function_builder_set_init_local_callback, handle(), trampoline);

	init_local_callback = callback;

	return *this;
}

class TableFunction::ExecInput::Inner {
public:
	duckdb_v2_table_function_exec_info_handle info = nullptr;
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
		CheckedAPICall(duckdb_v2_table_function_builder_set_exec_callback, handle(), nullptr);
		exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle ctx,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_exec_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			duckdb_v2_data_chunk_handle output_chunk = nullptr;
			CheckedAPICall(duckdb_v2_table_function_exec_get_output_chunk, info, &output_chunk);

			ExecInput::Inner inner {info, detail::Factory::Make<DataChunk>(output_chunk, false)};
			ExecInput input {inner};

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_exec_callback, handle(), trampoline);

	exec_callback = callback;

	return *this;
}

void TableFunction::Register(const Context &ctx) {
	// Set the user data to the callbacks so they can be retrieved in the trampoline(s)
	CheckedAPICall(duckdb_v2_table_function_builder_set_user_data, handle(),
	               new TableFunctionInfo {bind_callback, init_global_callback, init_local_callback, exec_callback},
	               detail::TypedDelete<TableFunctionInfo>);

	CheckedAPICall(duckdb_v2_table_function_builder_register, ctx.handle(), handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Copy Function
//----------------------------------------------------------------------------------------------------------------------

// Holds the user callbacks. A pointer to this is stashed as the function's user data and recovered (via the args
// struct) in every trampoline, mirroring the scalar/aggregate/table function wrappers.
struct CopyFunctionInfo {
	CopyFunction::BindCallback bind_callback = nullptr;
	CopyFunction::InitCallback init_callback = nullptr;
	CopyFunction::BatchCallback batch_callback = nullptr;
	CopyFunction::FlushCallback flush_callback = nullptr;
	CopyFunction::FinalizeCallback finalize_callback = nullptr;
};

CopyFunction::CopyFunction(const Context &ctx) {
	duckdb_v2_copy_function_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_copy_function_builder_create, ctx.handle(), &_h);
	impl = _h;
}

CopyFunction::~CopyFunction() {
	auto _h = handle();
	duckdb_v2_copy_function_builder_destroy(&_h);
}

auto CopyFunction::SetName(const std::string &name) & -> CopyFunction & {
	CheckedAPICall(duckdb_v2_copy_function_builder_set_name, handle(), ToStr(name));
	return *this;
}

// --- Bind input ---

auto CopyFunction::BindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_copy_function_bind_args *>(args)->context);
}

auto CopyFunction::BindInput::GetColumnCount() const -> idx_t {
	return static_cast<duckdb_v2_copy_function_bind_args *>(args)->column_count;
}

auto CopyFunction::BindInput::GetColumnName(idx_t index) const -> std::string_view {
	return FromStr(static_cast<duckdb_v2_copy_function_bind_args *>(args)->column_names[index]);
}

auto CopyFunction::BindInput::GetColumnType(idx_t index) const -> LogicalType {
	auto type_handle = static_cast<duckdb_v2_copy_function_bind_args *>(args)->column_types[index];
	duckdb_v2_logical_type_handle copy_handle = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_copy, type_handle, &copy_handle);
	return detail::Factory::Make<LogicalType>(copy_handle);
}

void CopyFunction::BindInput::SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
                                                  void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_bind_args *>(args);
	args_struct->out_bind_data = data;
	args_struct->out_bind_data_copy = copy;
	args_struct->out_bind_data_equality = equals;
	args_struct->out_bind_data_destructor = destructor;
}

// --- Init input ---

auto CopyFunction::InitInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_copy_function_init_args *>(args)->context);
}

auto CopyFunction::InitInput::GetFilePath() const -> std::string_view {
	return FromStr(static_cast<duckdb_v2_copy_function_init_args *>(args)->file_path);
}

const void *CopyFunction::InitInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_copy_function_init_args *>(args)->bind_data;
}

void CopyFunction::InitInput::SetInitDataInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_init_args *>(args);
	args_struct->out_init_data = data;
	args_struct->out_init_data_destructor = destructor;
}

// --- Batch input ---

auto CopyFunction::BatchInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_copy_function_batch_args *>(args)->context);
}

const void *CopyFunction::BatchInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_copy_function_batch_args *>(args)->bind_data;
}

void *CopyFunction::BatchInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_batch_args *>(args)->init_data;
}

void CopyFunction::BatchInput::SetBatchDataInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_batch_args *>(args);
	args_struct->out_batch = data;
	args_struct->out_batch_destructor = destructor;
}

// --- Flush input ---

auto CopyFunction::FlushInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_copy_function_flush_args *>(args)->context);
}

const void *CopyFunction::FlushInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_copy_function_flush_args *>(args)->bind_data;
}

void *CopyFunction::FlushInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_flush_args *>(args)->init_data;
}

void *CopyFunction::FlushInput::GetBatchDataInternal() const {
	return static_cast<duckdb_v2_copy_function_flush_args *>(args)->in_batch;
}

// --- Finalize input ---

auto CopyFunction::FinalizeInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_copy_function_finalize_args *>(args)->context);
}

const void *CopyFunction::FinalizeInput::GetBindDataInternal() const {
	return static_cast<duckdb_v2_copy_function_finalize_args *>(args)->bind_data;
}

void *CopyFunction::FinalizeInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_finalize_args *>(args)->init_data;
}

// --- Callback registration ---

auto CopyFunction::SetBindCallback(BindCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_bind_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<BindInput>(args);
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_bind_callback, handle(), trampoline);
	bind_callback = callback;
	return *this;
}

auto CopyFunction::SetInitCallback(InitCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_init_callback, handle(), nullptr);
		init_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_init_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<InitInput>(args);
			function.init_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_init_callback, handle(), trampoline);
	init_callback = callback;
	return *this;
}

auto CopyFunction::SetBatchCallback(BatchCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_batch_callback, handle(), nullptr);
		batch_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_batch_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			// Ownership of the collection is handed to us; wrap it so it is destroyed when the callback returns.
			auto collection = detail::Factory::Make<ColumnDataCollection>(static_cast<void *>(args->in_batch));
			auto input = detail::Factory::Make<BatchInput>(args, std::move(collection));
			function.batch_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_batch_callback, handle(), trampoline);
	batch_callback = callback;
	return *this;
}

auto CopyFunction::SetFlushCallback(FlushCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_flush_callback, handle(), nullptr);
		flush_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_flush_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<FlushInput>(args);
			function.flush_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_flush_callback, handle(), trampoline);
	flush_callback = callback;
	return *this;
}

auto CopyFunction::SetFinalizeCallback(FinalizeCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_finalize_callback, handle(), nullptr);
		finalize_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_finalize_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<FinalizeInput>(args);
			function.finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_finalize_callback, handle(), trampoline);
	finalize_callback = callback;
	return *this;
}

auto CopyFunction::Register(const Context &ctx) -> void {
	// Stash the callbacks as the function's user data so the trampolines can recover them via args->user_data.
	CheckedAPICall(
	    duckdb_v2_copy_function_builder_set_user_data, handle(),
	    new CopyFunctionInfo {bind_callback, init_callback, batch_callback, flush_callback, finalize_callback},
	    detail::TypedDelete<CopyFunctionInfo>);

	CheckedAPICall(duckdb_v2_copy_function_builder_register, ctx.handle(), handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Cast Function
//----------------------------------------------------------------------------------------------------------------------

struct CastFunctionInfo {
	CastFunction::ExecCallback exec_callback = nullptr;
};

CastFunction::CastFunction(const Context &ctx) {
	duckdb_v2_cast_function_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_cast_function_builder_create, ctx.handle(), &_h);
	impl = _h;
}

CastFunction::~CastFunction() {
	auto _h = handle();
	duckdb_v2_cast_function_builder_destroy(&_h);
}

auto CastFunction::SetSourceType(const LogicalType &type) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_builder_set_source_type, handle(), type.handle());
	return *this;
}

auto CastFunction::SetTargetType(const LogicalType &type) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_builder_set_target_type, handle(), type.handle());
	return *this;
}

auto CastFunction::SetImplicitCastCost(int64_t cost) & -> CastFunction & {
	CheckedAPICall(duckdb_v2_cast_function_builder_set_implicit_cast_cost, handle(), cost);
	return *this;
}

auto CastFunction::ExecInput::GetInput() const -> Vector {
	auto vec = static_cast<duckdb_v2_cast_function_exec_args *>(args)->input;
	return detail::Factory::Make<Vector>(vec);
}

auto CastFunction::ExecInput::GetOutput() const -> Vector {
	auto vec = static_cast<duckdb_v2_cast_function_exec_args *>(args)->output;
	return detail::Factory::Make<Vector>(vec);
}

auto CastFunction::ExecInput::GetCount() const -> idx_t {
	return static_cast<duckdb_v2_cast_function_exec_args *>(args)->count;
}

auto CastFunction::ExecInput::GetCastMode() const -> CastMode {
	auto mode = static_cast<duckdb_v2_cast_function_exec_args *>(args)->mode;
	return mode == DUCKDB_V2_CAST_MODE_TRY ? CastMode::TRY : CastMode::NORMAL;
}

auto CastFunction::SetExecCallback(ExecCallback callback) & -> CastFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_cast_function_builder_set_exec_callback, handle(), nullptr);
		exec_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_cast_function_exec_args *args, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CastFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<ExecInput>(args);
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_cast_function_builder_set_exec_callback, handle(), trampoline);
	exec_callback = callback;
	return *this;
}

void CastFunction::Register(const Context &ctx) {
	// Stash the callback as the function's user data so the trampoline can recover it via args->user_data.
	CheckedAPICall(duckdb_v2_cast_function_builder_set_user_data, handle(), new CastFunctionInfo {exec_callback},
	               detail::TypedDelete<CastFunctionInfo>);

	CheckedAPICall(duckdb_v2_cast_function_builder_register, ctx.handle(), handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Custom Type
//----------------------------------------------------------------------------------------------------------------------

CustomType::CustomType(const Context &ctx) {
	duckdb_v2_custom_type_builder_handle _h = nullptr;
	CheckedAPICall(duckdb_v2_custom_type_builder_create, ctx.handle(), &_h);
	impl = _h;
}

CustomType::~CustomType() {
	auto _h = handle();
	duckdb_v2_custom_type_builder_destroy(&_h);
}

auto CustomType::SetName(const std::string &name) & -> CustomType & {
	CheckedAPICall(duckdb_v2_custom_type_builder_set_name, handle(), ToStr(name));
	return *this;
}

auto CustomType::SetBaseType(const LogicalType &type) & -> CustomType & {
	CheckedAPICall(duckdb_v2_custom_type_builder_set_base_type, handle(), type.handle());
	return *this;
}

void CustomType::Register(const Context &ctx) {
	CheckedAPICall(duckdb_v2_custom_type_builder_register, ctx.handle(), handle());
}

} // namespace duckdb_api
