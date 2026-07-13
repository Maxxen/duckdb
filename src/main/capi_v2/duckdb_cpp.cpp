// Include the C++ API header (which includes the C API header)
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"

#include <type_traits>

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
struct HandleTraits<PreparedStatement> {
	using handle = duckdb_v2_prepared_statement_handle;
};
template <>
struct HandleTraits<Schema> {
	using handle = duckdb_v2_schema_handle;
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
struct HandleTraits<Expression> {
	using handle = duckdb_v2_expression_handle;
};
template <>
struct HandleTraits<Vector> {
	using handle = duckdb_v2_vector_handle;
};
template <>
struct HandleTraits<ArrowConversionPlan> {
	using handle = duckdb_v2_arrow_conversion_plan_handle;
};
template <>
struct HandleTraits<StringHeap> {
	using handle = duckdb_v2_string_heap_handle;
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

// ---- Function property enum conversions (C++ enum <-> generic C value) ----
// Each function property maps onto a (key, value) pair in the underlying C API.
// The wrapper exposes one explicit enum per property; these translate to and
// from the corresponding DUCKDB_V2_FUNCTION_PROPERTY_VALUE.

[[noreturn]] void ThrowUnexpectedPropertyValue(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
	                "Unexpected function property value " + std::to_string(static_cast<uint32_t>(value)));
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(FunctionStability value) {
	switch (value) {
	case FunctionStability::Consistent:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;
	case FunctionStability::Volatile:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE;
	case FunctionStability::ConsistentWithinQuery:
		return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;
}
FunctionStability FromCStability(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT:
		return FunctionStability::Consistent;
	case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE:
		return FunctionStability::Volatile;
	case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY:
		return FunctionStability::ConsistentWithinQuery;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(FunctionNullHandling value) {
	switch (value) {
	case FunctionNullHandling::Default:
		return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT;
	case FunctionNullHandling::Special:
		return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT;
}
FunctionNullHandling FromCNullHandling(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT:
		return FunctionNullHandling::Default;
	case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL:
		return FunctionNullHandling::Special;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(FunctionFallibility value) {
	switch (value) {
	case FunctionFallibility::Infallible:
		return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE;
	case FunctionFallibility::Fallible:
		return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE;
}
FunctionFallibility FromCFallibility(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE:
		return FunctionFallibility::Infallible;
	case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE:
		return FunctionFallibility::Fallible;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(FunctionCollationHandling value) {
	switch (value) {
	case FunctionCollationHandling::Propagate:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE;
	case FunctionCollationHandling::PushCombinable:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE;
	case FunctionCollationHandling::Ignore:
		return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE;
}
FunctionCollationHandling FromCCollationHandling(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE:
		return FunctionCollationHandling::Propagate;
	case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE:
		return FunctionCollationHandling::PushCombinable;
	case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE:
		return FunctionCollationHandling::Ignore;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(AggregateFunction::OrderDependence value) {
	switch (value) {
	case AggregateFunction::OrderDependence::Dependent:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;
	case AggregateFunction::OrderDependence::Independent:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;
}
AggregateFunction::OrderDependence FromCOrderDependence(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES:
		return AggregateFunction::OrderDependence::Dependent;
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO:
		return AggregateFunction::OrderDependence::Independent;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE ToCValue(AggregateFunction::DistinctDependence value) {
	switch (value) {
	case AggregateFunction::DistinctDependence::Dependent:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES;
	case AggregateFunction::DistinctDependence::Independent:
		return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO;
	}
	return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES;
}
AggregateFunction::DistinctDependence FromCDistinctDependence(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (value) {
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES:
		return AggregateFunction::DistinctDependence::Dependent;
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO:
		return AggregateFunction::DistinctDependence::Independent;
	default:
		ThrowUnexpectedPropertyValue(value);
	}
}

} // namespace

namespace detail {

template <class T, class... ARGS>
auto MakeUserData(ARGS &&... args) -> duckdb_v2_opaque {
	return duckdb_v2_opaque {new T(std::forward<ARGS>(args)...), TypedDelete<T>, TypedEquals<T>};
}

} // namespace detail

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

auto LibraryVersion() -> std::string {
	char *version = nullptr;
	CheckedAPICall(duckdb_v2_library_version, &version);
	auto result = std::string(version);
	free(version);
	return result;
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

// OptionTargetScope mirrors DUCKDB_V2_OPTION_TARGET_SCOPE numerically; every member is pinned.
static_assert(static_cast<uint8_t>(OptionTargetScope::Unknown) == DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::GlobalOnly) == DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::LocalOnly) == DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::GlobalDefault) == DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");
static_assert(static_cast<uint8_t>(OptionTargetScope::LocalDefault) == DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT,
              "OptionTargetScope must mirror DUCKDB_V2_OPTION_TARGET_SCOPE");

OptionTargetScope DatabaseOption::GetTargetScope() const {
	DUCKDB_V2_OPTION_TARGET_SCOPE scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	CheckedAPICall(duckdb_v2_option_get_target_scope, handle(), &scope);
	return static_cast<OptionTargetScope>(scope);
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

DatabaseOption Database::GetOption(std::string_view name) const {
	duckdb_v2_option_handle option = nullptr;
	CheckedAPICall(duckdb_v2_database_option_get, handle(), duckdb_v2_str {name.data(), name.size()}, &option);
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

// Bundles the C++ callback and the caller's user data into the C API's single opaque slot.
struct ReplacementScanInfo {
	Database::ReplacementScanCallback callback = nullptr;
	detail::UserData user_data;
};

void Database::AddReplacementScanInternal(ReplacementScanCallback callback, void *user_data,
                                          void (*destructor)(void *)) {
	auto *payload = new ReplacementScanInfo(); // NOLINT
	payload->callback = callback;
	payload->user_data = detail::UserData(user_data, destructor);

	static auto trampoline = [](duckdb_v2_replacement_scan_info_handle c_info, duckdb_v2_context_handle ctx,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *raw = nullptr;
			CheckedAPICall(duckdb_v2_replacement_scan_get_user_data, c_info, &raw);
			auto &recovered = *static_cast<ReplacementScanInfo *>(raw);
			auto input = detail::Factory::Make<ReplacementScanInput>(
			    static_cast<void *>(c_info), static_cast<void *>(ctx), recovered.user_data.get());
			recovered.callback(input);
		});
	};

	// The engine owns the payload on success (freed at db close); on failure we still own it.
	duckdb_v2_opaque opaque {payload, detail::TypedDelete<ReplacementScanInfo>, nullptr};
	try {
		CheckedAPICall(duckdb_v2_replacement_scan_register, handle(), trampoline, opaque);
	} catch (...) {
		detail::TypedDelete<ReplacementScanInfo>(payload);
		throw;
	}
}

void Database::AddReplacementScan(ReplacementScanCallback callback) {
	AddReplacementScanInternal(callback, nullptr, nullptr);
}

auto Database::ReplacementScanInput::GetCatalogName() const -> std::string_view {
	duckdb_v2_str name {nullptr, 0};
	CheckedAPICall(duckdb_v2_replacement_scan_get_catalog_name,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(info), &name);
	return FromStr(name);
}

auto Database::ReplacementScanInput::GetSchemaName() const -> std::string_view {
	duckdb_v2_str name {nullptr, 0};
	CheckedAPICall(duckdb_v2_replacement_scan_get_schema_name,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(info), &name);
	return FromStr(name);
}

auto Database::ReplacementScanInput::GetTableName() const -> std::string_view {
	duckdb_v2_str name {nullptr, 0};
	CheckedAPICall(duckdb_v2_replacement_scan_get_table_name, static_cast<duckdb_v2_replacement_scan_info_handle>(info),
	               &name);
	return FromStr(name);
}

auto Database::ReplacementScanInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

void Database::ReplacementScanInput::SetFunctionName(const std::string &name) {
	CheckedAPICall(duckdb_v2_replacement_scan_set_function_name,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(info), ToStr(name));
}

void Database::ReplacementScanInput::AddParameter(const Value &value) {
	CheckedAPICall(duckdb_v2_replacement_scan_add_parameter, static_cast<duckdb_v2_replacement_scan_info_handle>(info),
	               value.handle());
}

void Database::ReplacementScanInput::AddNamedParameter(const std::string &name, const Value &value) {
	CheckedAPICall(duckdb_v2_replacement_scan_add_named_parameter,
	               static_cast<duckdb_v2_replacement_scan_info_handle>(info), ToStr(name), value.handle());
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

auto Connection::ParseType(std::string_view text) -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	WithTransaction([&](const Context &ctx) { //
		type = static_cast<duckdb_v2_logical_type_handle>(ctx.ParseType(text).release());
	});
	return detail::Factory::Make<LogicalType>(type);
}

auto Connection::CreateType(const std::string &name, const std::vector<TypeParam> &params) -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	WithTransaction([&](const Context &ctx) { //
		type = static_cast<duckdb_v2_logical_type_handle>(ctx.CreateType(name, params).release());
	});
	return detail::Factory::Make<LogicalType>(type);
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

QueryResult Connection::Execute(const SqlStatement &statement, const Value *parameters, idx_t parameter_count) {
	// Borrowed, not consumed: pass the handle without releasing it, so the
	// caller's SqlStatement keeps ownership and can be executed again.
	std::vector<duckdb_v2_value_handle> values;
	values.reserve(parameter_count);
	for (idx_t i = 0; i < parameter_count; i++) {
		values.push_back(parameters[i].handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_statement_execute, handle(), statement.handle(), nullptr,
	               parameter_count ? values.data() : nullptr, parameter_count, &result);
	return detail::Factory::Make<QueryResult>(result);
}

QueryResult Connection::Execute(const SqlStatement &statement, const std::vector<NamedParam> &parameters) {
	// Split into the C API's parallel arrays; an empty name crosses as the positional
	// {NULL, 0} view (mirrors Context::CreateType).
	std::vector<duckdb_v2_str> names;
	std::vector<duckdb_v2_value_handle> values;
	names.reserve(parameters.size());
	values.reserve(parameters.size());
	for (const auto &param : parameters) {
		names.push_back(param.name.empty() ? duckdb_v2_str {nullptr, 0} : ToStr(param.name));
		values.push_back(param.value.handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_statement_execute, handle(), statement.handle(), names.empty() ? nullptr : names.data(),
	               values.empty() ? nullptr : values.data(), static_cast<idx_t>(parameters.size()), &result);
	return detail::Factory::Make<QueryResult>(result);
}

QueryResult Connection::Execute(const SqlStatement &statement) {
	return Execute(statement, nullptr, 0);
}

QueryResult Connection::Execute(const std::string &sql) {
	auto statements = ParseSQL(sql);
	auto statement = statements.Next();
	if (!statement || statements.Next()) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "Execute expects exactly one statement; use ParseSQL for multi-statement input");
	}
	return Execute(statement);
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

auto Context::ParseType(std::string_view text) const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_text, handle(), duckdb_v2_str {text.data(), text.size()}, &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Context::CreateType(const std::string &name, const std::vector<TypeParam> &params) const -> LogicalType {
	// Split into the C API's parallel arrays; an empty name crosses as the
	// positional {NULL, 0} view.
	std::vector<duckdb_v2_str> names;
	std::vector<duckdb_v2_value_handle> values;
	names.reserve(params.size());
	values.reserve(params.size());
	for (const auto &param : params) {
		names.push_back(param.name.empty() ? duckdb_v2_str {nullptr, 0} : ToStr(param.name));
		values.push_back(param.value.handle());
	}
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create, handle(), ToStr(name), names.empty() ? nullptr : names.data(),
	               values.empty() ? nullptr : values.data(), static_cast<idx_t>(params.size()), &type);
	return detail::Factory::Make<LogicalType>(type);
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

std::string_view LogicalType::GetName() const {
	duckdb_v2_str name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_logical_type_get_name, handle(), &name);
	return FromStr(name);
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

// TypeId mirrors LOGICAL_TYPE_ID numerically; every member is pinned.
#define DUCKDB_CPP_ASSERT_TYPE_ID(member)                                                                              \
	static_assert(static_cast<uint32_t>(TypeId::member) == DUCKDB_V2_LOGICAL_TYPE_ID_##member,                         \
	              "TypeId::" #member " must mirror DUCKDB_V2_LOGICAL_TYPE_ID_" #member)
DUCKDB_CPP_ASSERT_TYPE_ID(INVALID);
DUCKDB_CPP_ASSERT_TYPE_ID(SQLNULL);
DUCKDB_CPP_ASSERT_TYPE_ID(UNKNOWN);
DUCKDB_CPP_ASSERT_TYPE_ID(ANY);
DUCKDB_CPP_ASSERT_TYPE_ID(TYPE);
DUCKDB_CPP_ASSERT_TYPE_ID(BOOLEAN);
DUCKDB_CPP_ASSERT_TYPE_ID(TINYINT);
DUCKDB_CPP_ASSERT_TYPE_ID(SMALLINT);
DUCKDB_CPP_ASSERT_TYPE_ID(INTEGER);
DUCKDB_CPP_ASSERT_TYPE_ID(BIGINT);
DUCKDB_CPP_ASSERT_TYPE_ID(DATE);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_SEC);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_MS);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(DECIMAL);
DUCKDB_CPP_ASSERT_TYPE_ID(FLOAT);
DUCKDB_CPP_ASSERT_TYPE_ID(DOUBLE);
DUCKDB_CPP_ASSERT_TYPE_ID(VARCHAR);
DUCKDB_CPP_ASSERT_TYPE_ID(BLOB);
DUCKDB_CPP_ASSERT_TYPE_ID(INTERVAL);
DUCKDB_CPP_ASSERT_TYPE_ID(UTINYINT);
DUCKDB_CPP_ASSERT_TYPE_ID(USMALLINT);
DUCKDB_CPP_ASSERT_TYPE_ID(UINTEGER);
DUCKDB_CPP_ASSERT_TYPE_ID(UBIGINT);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_TZ);
DUCKDB_CPP_ASSERT_TYPE_ID(TIMESTAMP_TZ_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME_TZ);
DUCKDB_CPP_ASSERT_TYPE_ID(TIME_NS);
DUCKDB_CPP_ASSERT_TYPE_ID(BIT);
DUCKDB_CPP_ASSERT_TYPE_ID(BIGNUM);
DUCKDB_CPP_ASSERT_TYPE_ID(UHUGEINT);
DUCKDB_CPP_ASSERT_TYPE_ID(HUGEINT);
DUCKDB_CPP_ASSERT_TYPE_ID(UUID);
DUCKDB_CPP_ASSERT_TYPE_ID(GEOMETRY);
DUCKDB_CPP_ASSERT_TYPE_ID(STRUCT);
DUCKDB_CPP_ASSERT_TYPE_ID(LIST);
DUCKDB_CPP_ASSERT_TYPE_ID(MAP);
DUCKDB_CPP_ASSERT_TYPE_ID(ENUM);
DUCKDB_CPP_ASSERT_TYPE_ID(UNION);
DUCKDB_CPP_ASSERT_TYPE_ID(ARRAY);
DUCKDB_CPP_ASSERT_TYPE_ID(VARIANT);
#undef DUCKDB_CPP_ASSERT_TYPE_ID

auto LogicalType::GetId() const -> TypeId {
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	CheckedAPICall(duckdb_v2_logical_type_get_id, handle(), &id);
	return static_cast<TypeId>(id);
}

auto LogicalType::ToText() const -> std::string {
	char *text = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_to_text, handle(), &text);
	auto result = std::string(text);
	free(text);
	return result;
}

auto LogicalType::GetParamCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_logical_type_get_param_count, handle(), &count);
	return count;
}

auto LogicalType::GetParam(idx_t index) const -> TypeParam {
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_get_param, handle(), index, &name, &value);
	return TypeParam {std::string(FromStr(name)), detail::Factory::Make<Value>(value)};
}

auto LogicalType::RequireKind(TypeId expected, const char *what) const -> void {
	if (GetId() != expected) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                std::string("Invalid Input Error: ") + what + " requires the matching type kind");
	}
}

auto LogicalType::GetDecimalWidth() const -> uint8_t {
	RequireKind(TypeId::DECIMAL, "GetDecimalWidth");
	return GetParam(0).value.AsU8();
}

auto LogicalType::GetDecimalScale() const -> uint8_t {
	RequireKind(TypeId::DECIMAL, "GetDecimalScale");
	return GetParam(1).value.AsU8();
}

auto LogicalType::GetEnumSize() const -> idx_t {
	RequireKind(TypeId::ENUM, "GetEnumSize");
	return GetParamCount();
}

auto LogicalType::GetEnumValue(idx_t index) const -> std::string {
	RequireKind(TypeId::ENUM, "GetEnumValue");
	// Owned string: the backing Value is owned per call, a view would dangle.
	return std::string(GetParam(index).value.AsVarchar());
}

auto LogicalType::GetListChildType() const -> LogicalType {
	RequireKind(TypeId::LIST, "GetListChildType");
	return GetParam(0).value.AsType();
}

auto LogicalType::GetArrayChildType() const -> LogicalType {
	RequireKind(TypeId::ARRAY, "GetArrayChildType");
	return GetParam(0).value.AsType();
}

auto LogicalType::GetArraySize() const -> idx_t {
	RequireKind(TypeId::ARRAY, "GetArraySize");
	return static_cast<idx_t>(GetParam(1).value.AsI64());
}

auto LogicalType::GetMapKeyType() const -> LogicalType {
	RequireKind(TypeId::MAP, "GetMapKeyType");
	return GetParam(0).value.AsType();
}

auto LogicalType::GetMapValueType() const -> LogicalType {
	RequireKind(TypeId::MAP, "GetMapValueType");
	return GetParam(1).value.AsType();
}

auto LogicalType::GetStructChildCount() const -> idx_t {
	RequireKind(TypeId::STRUCT, "GetStructChildCount");
	return GetParamCount();
}

auto LogicalType::GetStructChildName(idx_t index) const -> std::string {
	RequireKind(TypeId::STRUCT, "GetStructChildName");
	return GetParam(index).name;
}

auto LogicalType::GetStructChildType(idx_t index) const -> LogicalType {
	RequireKind(TypeId::STRUCT, "GetStructChildType");
	return GetParam(index).value.AsType();
}

auto LogicalType::GetUnionMemberCount() const -> idx_t {
	RequireKind(TypeId::UNION, "GetUnionMemberCount");
	return GetParamCount();
}

auto LogicalType::GetUnionMemberName(idx_t index) const -> std::string {
	RequireKind(TypeId::UNION, "GetUnionMemberName");
	return GetParam(index).name;
}

auto LogicalType::GetUnionMemberType(idx_t index) const -> LogicalType {
	RequireKind(TypeId::UNION, "GetUnionMemberType");
	return GetParam(index).value.AsType();
}

auto LogicalType::GetDecimalInternalTypeId() const -> TypeId {
	// The committed DECIMAL storage tiers: width <= 4 int16, <= 9 int32,
	// <= 18 int64, <= 38 int128. Pinned against the engine in [capi_v2].
	auto width = GetDecimalWidth();
	if (width <= 4) {
		return TypeId::SMALLINT;
	}
	if (width <= 9) {
		return TypeId::INTEGER;
	}
	if (width <= 18) {
		return TypeId::BIGINT;
	}
	return TypeId::HUGEINT;
}

auto LogicalType::GetEnumInternalTypeId() const -> TypeId {
	// The committed ENUM storage tiers: size <= 255 uint8, <= 65535 uint16,
	// else uint32. Pinned against the engine in [capi_v2].
	auto size = GetEnumSize();
	if (size <= 255) {
		return TypeId::UTINYINT;
	}
	if (size <= 65535) {
		return TypeId::USMALLINT;
	}
	return TypeId::UINTEGER;
}

//----------------------------------------------------------------------------------------------------------------------
// Schema
//----------------------------------------------------------------------------------------------------------------------
Schema::Schema(void *impl) : detail::Handle<Schema>(impl) {
}
Schema::~Schema() {
	auto _h = handle();
	duckdb_v2_schema_destroy(&_h);
}
idx_t Schema::GetFieldCount() const {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_schema_get_count, handle(), &count);
	return count;
}
std::string_view Schema::GetFieldName(idx_t index) const {
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr; // borrowed; unused here
	CheckedAPICall(duckdb_v2_schema_get_field, handle(), index, &name, &type);
	return FromStr(name);
}
LogicalType Schema::GetFieldType(idx_t index) const {
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle borrowed = nullptr;
	CheckedAPICall(duckdb_v2_schema_get_field, handle(), index, &name, &borrowed);
	// get_field borrows the type; copy it into an owned handle the wrapper manages.
	duckdb_v2_logical_type_handle owned = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_copy, borrowed, &owned);
	return detail::Factory::Make<LogicalType>(owned);
}

auto Schema::ToArrowSchema(const Context &context, ArrowSchema &out) const -> void {
	// The C converter takes parallel name/type arrays; get_field borrows both,
	// so no copies are needed for the duration of this call.
	const auto count = GetFieldCount();
	std::vector<duckdb_v2_logical_type_handle> types(count, nullptr);
	std::vector<duckdb_v2_str> names(count, duckdb_v2_str {nullptr, 0});
	for (idx_t i = 0; i < count; i++) {
		CheckedAPICall(duckdb_v2_schema_get_field, handle(), i, &names[i], &types[i]);
	}
	CheckedAPICall(duckdb_v2_logical_types_to_arrow_schema, context.handle(), types.data(), names.data(), count, &out);
}

Signature Connection::Bind(const SqlStatement &statement) const {
	duckdb_v2_schema_handle out_schema = nullptr;
	duckdb_v2_schema_handle out_parameters = nullptr;
	CheckedAPICall(duckdb_v2_statement_bind, handle(), statement.handle(), &out_schema, &out_parameters);
	return Signature {detail::Factory::Make<Schema>(out_schema), detail::Factory::Make<Schema>(out_parameters)};
}

PreparedStatement Connection::Prepare(const SqlStatement &statement, bool require_cacheable) const {
	// Borrowed, not consumed: pass the handle without releasing it.
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	CheckedAPICall(duckdb_v2_statement_prepare, handle(), statement.handle(), require_cacheable, &prepared);
	return detail::Factory::Make<PreparedStatement>(prepared);
}

//----------------------------------------------------------------------------------------------------------------------
// Prepared Statement
//----------------------------------------------------------------------------------------------------------------------

PreparedStatement::PreparedStatement(void *impl) : detail::Handle<PreparedStatement>(impl) {
}

PreparedStatement::~PreparedStatement() {
	auto _h = handle();
	duckdb_v2_prepared_statement_destroy(&_h);
}

QueryResult PreparedStatement::Execute(const Value *parameters, idx_t parameter_count) {
	std::vector<duckdb_v2_value_handle> values;
	values.reserve(parameter_count);
	for (idx_t i = 0; i < parameter_count; i++) {
		values.push_back(parameters[i].handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_prepared_execute, handle(), nullptr, parameter_count ? values.data() : nullptr,
	               parameter_count, &result);
	return detail::Factory::Make<QueryResult>(result);
}

QueryResult PreparedStatement::Execute(const std::vector<NamedParam> &parameters) {
	// Split into the C API's parallel arrays; an empty name crosses as the positional
	// {NULL, 0} view (mirrors Context::CreateType).
	std::vector<duckdb_v2_str> names;
	std::vector<duckdb_v2_value_handle> values;
	names.reserve(parameters.size());
	values.reserve(parameters.size());
	for (const auto &param : parameters) {
		names.push_back(param.name.empty() ? duckdb_v2_str {nullptr, 0} : ToStr(param.name));
		values.push_back(param.value.handle());
	}
	duckdb_v2_result_handle result = nullptr;
	CheckedAPICall(duckdb_v2_prepared_execute, handle(), names.empty() ? nullptr : names.data(),
	               values.empty() ? nullptr : values.data(), static_cast<idx_t>(parameters.size()), &result);
	return detail::Factory::Make<QueryResult>(result);
}

QueryResult PreparedStatement::Execute() {
	return Execute(nullptr, 0);
}

bool PreparedStatement::ReusesPlan() const {
	bool reuses = false;
	CheckedAPICall(duckdb_v2_prepared_reuses_plan, handle(), &reuses);
	return reuses;
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

// Leaf codec plumbing: the per-kind C value functions are gone; primitive
// payloads cross through value_create_from_data / value_get_data in each
// kind's committed physical layout.
namespace {

duckdb_v2_value_handle MakeLeafValue(DUCKDB_V2_LOGICAL_TYPE_ID id, const void *data, idx_t len) {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_logical_type_create_from_id, id, &type);
	duckdb_v2_value_handle value = nullptr;
	try {
		CheckedAPICall(duckdb_v2_value_create_from_data, type, data, len, &value);
	} catch (...) {
		duckdb_v2_logical_type_destroy(&type);
		throw;
	}
	duckdb_v2_logical_type_destroy(&type);
	return value;
}

DUCKDB_V2_LOGICAL_TYPE_ID LeafTypeId(duckdb_v2_value_handle value) {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_logical_type, value, &type);
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	auto code = duckdb_v2_logical_type_get_id(type, &id, nullptr);
	duckdb_v2_logical_type_destroy(&type);
	if (code != DUCKDB_V2_ERROR_NONE) {
		throw Exception(code, "Invalid Input Error: failed to read the value's type id");
	}
	return id;
}

// Reads a borrowed leaf payload. Gates on the type id first so a wrongly
// typed read fails clearly instead of reinterpreting same-width bytes.
std::pair<const void *, idx_t> LeafPayload(duckdb_v2_value_handle value, DUCKDB_V2_LOGICAL_TYPE_ID expected,
                                           const char *what) {
	if (LeafTypeId(value) != expected) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                std::string("Invalid Input Error: ") + what + ": value is not of the expected type");
	}
	const void *data = nullptr;
	idx_t len = 0;
	CheckedAPICall(duckdb_v2_value_get_data, value, &data, &len);
	return {data, len};
}

template <class T>
T LeafPayloadAs(duckdb_v2_value_handle value, DUCKDB_V2_LOGICAL_TYPE_ID expected, const char *what) {
	auto payload = LeafPayload(value, expected, what);
	if (payload.second != sizeof(T)) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                std::string("Invalid Input Error: ") + what + ": unexpected payload size");
	}
	T out;
	std::memcpy(&out, payload.first, sizeof(T));
	return out;
}

} // namespace

Value Value::FromI64(int64_t value) {
	return detail::Factory::Make<Value>(MakeLeafValue(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, &value, sizeof(value)));
}

Value Value::FromVarchar(const std::string &value) {
	return detail::Factory::Make<Value>(MakeLeafValue(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, value.data(), value.size()));
}

Value Value::Null(const LogicalType &type) {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create_null, type.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

Value Value::FromBignum(const uint8_t *data, idx_t length, bool is_negative) {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create_bignum, data, length, is_negative, &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::AsBignum() const -> Bignum {
	uint8_t *data = nullptr;
	idx_t length = 0;
	bool is_negative = false;
	CheckedAPICall(duckdb_v2_value_get_bignum, handle(), &data, &length, &is_negative);
	Bignum result {std::vector<uint8_t>(data, data + length), is_negative};
	free(data);
	return result;
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
	return LeafPayloadAs<bool>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, "AsBool");
}

auto Value::AsI8() const -> int8_t {
	return LeafPayloadAs<int8_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT, "AsI8");
}

auto Value::AsI16() const -> int16_t {
	return LeafPayloadAs<int16_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT, "AsI16");
}

auto Value::AsU8() const -> uint8_t {
	return LeafPayloadAs<uint8_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT, "AsU8");
}

auto Value::AsU16() const -> uint16_t {
	return LeafPayloadAs<uint16_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT, "AsU16");
}

auto Value::AsI32() const -> int32_t {
	return LeafPayloadAs<int32_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, "AsI32");
}

auto Value::AsU32() const -> uint32_t {
	return LeafPayloadAs<uint32_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER, "AsU32");
}

auto Value::AsI64() const -> int64_t {
	return LeafPayloadAs<int64_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, "AsI64");
}

auto Value::AsU64() const -> uint64_t {
	return LeafPayloadAs<uint64_t>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT, "AsU64");
}

auto Value::AsF32() const -> float {
	return LeafPayloadAs<float>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT, "AsF32");
}

auto Value::AsF64() const -> double {
	return LeafPayloadAs<double>(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, "AsF64");
}

auto Value::AsVarchar() const -> std::string_view {
	auto payload = LeafPayload(handle(), DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, "AsVarchar");
	return payload.second ? std::string_view(static_cast<const char *>(payload.first), payload.second)
	                      : std::string_view();
}

Value Value::Type(const LogicalType &type) {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create_type, type.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::AsType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_value_get_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Value::UnwrapVariant() const -> Value {
	duckdb_v2_value_handle unwrapped = nullptr;
	CheckedAPICall(duckdb_v2_value_get_variant, handle(), &unwrapped);
	return detail::Factory::Make<Value>(unwrapped);
}

Value Value::Create(const LogicalType &type, const std::vector<Value> &children) {
	std::vector<duckdb_v2_value_handle> handles;
	handles.reserve(children.size());
	for (const auto &child : children) {
		handles.push_back(child.handle());
	}
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_create, type.handle(), handles.empty() ? nullptr : handles.data(),
	               static_cast<idx_t>(children.size()), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::Cast(const Context &ctx, const LogicalType &target) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_value_cast, ctx.handle(), handle(), target.handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Value::Cast(Connection &conn, const LogicalType &target) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	conn.WithTransaction([&](const Context &ctx) { //
		value = static_cast<duckdb_v2_value_handle>(Cast(ctx, target).release());
	});
	return detail::Factory::Make<Value>(value);
}

auto Value::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_value_get_child_count, handle(), &count);
	return count;
}

auto Value::GetChild(idx_t index) const -> Value {
	duckdb_v2_value_handle child = nullptr;
	CheckedAPICall(duckdb_v2_value_get_child, handle(), index, &child);
	return detail::Factory::Make<Value>(child);
}

//----------------------------------------------------------------------------------------------------------------------
// Expression
//----------------------------------------------------------------------------------------------------------------------
// ExpressionClass and ExpressionType mirror the C enums numerically. The
// sentinels below pin the start and end of every numbering run, so a renumber
// on either side trips here.
static_assert(
    static_cast<uint32_t>(ExpressionClass::Invalid) == DUCKDB_V2_EXPRESSION_CLASS_INVALID &&
        static_cast<uint32_t>(ExpressionClass::Aggregate) == DUCKDB_V2_EXPRESSION_CLASS_AGGREGATE &&
        static_cast<uint32_t>(ExpressionClass::Star) == DUCKDB_V2_EXPRESSION_CLASS_STAR &&
        static_cast<uint32_t>(ExpressionClass::Subquery) == DUCKDB_V2_EXPRESSION_CLASS_SUBQUERY &&
        static_cast<uint32_t>(ExpressionClass::Type) == DUCKDB_V2_EXPRESSION_CLASS_TYPE &&
        static_cast<uint32_t>(ExpressionClass::BoundAggregate) == DUCKDB_V2_EXPRESSION_CLASS_BOUND_AGGREGATE &&
        static_cast<uint32_t>(ExpressionClass::BoundLambdaRef) == DUCKDB_V2_EXPRESSION_CLASS_BOUND_LAMBDA_REF &&
        static_cast<uint32_t>(ExpressionClass::BoundExpression) == DUCKDB_V2_EXPRESSION_CLASS_BOUND_EXPRESSION &&
        static_cast<uint32_t>(ExpressionClass::BoundExpanded) == DUCKDB_V2_EXPRESSION_CLASS_BOUND_EXPANDED,
    "ExpressionClass must mirror DUCKDB_V2_EXPRESSION_CLASS");
static_assert(static_cast<uint32_t>(ExpressionType::Invalid) == DUCKDB_V2_EXPRESSION_TYPE_INVALID &&
                  static_cast<uint32_t>(ExpressionType::OperatorCast) == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_CAST &&
                  static_cast<uint32_t>(ExpressionType::OperatorUnpack) == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_UNPACK &&
                  static_cast<uint32_t>(ExpressionType::CompareEqual) == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL &&
                  static_cast<uint32_t>(ExpressionType::CompareNotDistinctFrom) ==
                      DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_DISTINCT_FROM &&
                  static_cast<uint32_t>(ExpressionType::ConjunctionAnd) == DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND &&
                  static_cast<uint32_t>(ExpressionType::ConjunctionOr) == DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR &&
                  static_cast<uint32_t>(ExpressionType::ValueConstant) == DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT &&
                  static_cast<uint32_t>(ExpressionType::ValueDefault) == DUCKDB_V2_EXPRESSION_TYPE_VALUE_DEFAULT,
              "ExpressionType must mirror DUCKDB_V2_EXPRESSION_TYPE");
static_assert(
    static_cast<uint32_t>(ExpressionType::Aggregate) == DUCKDB_V2_EXPRESSION_TYPE_AGGREGATE &&
        static_cast<uint32_t>(ExpressionType::GroupingFunction) == DUCKDB_V2_EXPRESSION_TYPE_GROUPING_FUNCTION &&
        static_cast<uint32_t>(ExpressionType::WindowAggregate) == DUCKDB_V2_EXPRESSION_TYPE_WINDOW_AGGREGATE &&
        static_cast<uint32_t>(ExpressionType::WindowRank) == DUCKDB_V2_EXPRESSION_TYPE_WINDOW_RANK &&
        static_cast<uint32_t>(ExpressionType::WindowRowNumber) == DUCKDB_V2_EXPRESSION_TYPE_WINDOW_ROW_NUMBER &&
        static_cast<uint32_t>(ExpressionType::WindowFirstValue) == DUCKDB_V2_EXPRESSION_TYPE_WINDOW_FIRST_VALUE &&
        static_cast<uint32_t>(ExpressionType::WindowFill) == DUCKDB_V2_EXPRESSION_TYPE_WINDOW_FILL &&
        static_cast<uint32_t>(ExpressionType::Function) == DUCKDB_V2_EXPRESSION_TYPE_FUNCTION &&
        static_cast<uint32_t>(ExpressionType::CaseExpr) == DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR &&
        static_cast<uint32_t>(ExpressionType::OperatorTry) == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_TRY &&
        static_cast<uint32_t>(ExpressionType::Subquery) == DUCKDB_V2_EXPRESSION_TYPE_SUBQUERY &&
        static_cast<uint32_t>(ExpressionType::Star) == DUCKDB_V2_EXPRESSION_TYPE_STAR &&
        static_cast<uint32_t>(ExpressionType::Type) == DUCKDB_V2_EXPRESSION_TYPE_TYPE &&
        static_cast<uint32_t>(ExpressionType::Cast) == DUCKDB_V2_EXPRESSION_TYPE_CAST &&
        static_cast<uint32_t>(ExpressionType::BoundRef) == DUCKDB_V2_EXPRESSION_TYPE_BOUND_REF &&
        static_cast<uint32_t>(ExpressionType::BoundExpanded) == DUCKDB_V2_EXPRESSION_TYPE_BOUND_EXPANDED,
    "ExpressionType must mirror DUCKDB_V2_EXPRESSION_TYPE");

Expression::Expression(void *impl) : detail::Handle<Expression>(impl) {
}

Expression::~Expression() {
	/* Expressions are always borrowed, so we don't destroy the handle here */
}

auto Expression::GetClass() const -> ExpressionClass {
	DUCKDB_V2_EXPRESSION_CLASS expr_class = DUCKDB_V2_EXPRESSION_CLASS_INVALID;
	CheckedAPICall(duckdb_v2_expression_get_class, handle(), &expr_class);
	return static_cast<ExpressionClass>(expr_class);
}

auto Expression::GetType() const -> ExpressionType {
	DUCKDB_V2_EXPRESSION_TYPE type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
	CheckedAPICall(duckdb_v2_expression_get_type, handle(), &type);
	return static_cast<ExpressionType>(type);
}

auto Expression::GetReturnType() const -> LogicalType {
	duckdb_v2_logical_type_handle type = nullptr;
	CheckedAPICall(duckdb_v2_expression_get_return_type, handle(), &type);
	return detail::Factory::Make<LogicalType>(type);
}

auto Expression::GetChildCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_expression_get_child_count, handle(), &count);
	return count;
}

auto Expression::GetChild(idx_t index) const -> Expression {
	duckdb_v2_expression_handle child = nullptr;
	CheckedAPICall(duckdb_v2_expression_get_child, handle(), index, &child);
	return detail::Factory::Make<Expression>(child);
}

auto Expression::GetFunctionName() const -> std::string_view {
	duckdb_v2_str name = {nullptr, 0};
	CheckedAPICall(duckdb_v2_expression_get_function_name, handle(), &name);
	return FromStr(name);
}

auto Expression::GetConstantValue() const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_expression_get_constant_value, handle(), &value);
	return detail::Factory::Make<Value>(value);
}

auto Expression::GetColumnBinding() const -> ColumnBinding {
	ColumnBinding binding {0, 0};
	CheckedAPICall(duckdb_v2_expression_get_column_binding, handle(), &binding.table_index, &binding.column_index);
	return binding;
}

auto Expression::GetReferenceIndex() const -> idx_t {
	idx_t index = 0;
	CheckedAPICall(duckdb_v2_expression_get_reference_index, handle(), &index);
	return index;
}

//----------------------------------------------------------------------------------------------------------------------
// String Heap
//----------------------------------------------------------------------------------------------------------------------
// StringStorage mirrors duckdb_v2_string; pin it here (both types visible) so any
// layout drift breaks the build rather than the ABI.
static_assert(sizeof(StringStorage) == sizeof(duckdb_v2_string) && alignof(StringStorage) == alignof(duckdb_v2_string),
              "StringStorage must mirror the C ABI's duckdb_v2_string");
static_assert(offsetof(StringStorage, value.pointer.length) == offsetof(duckdb_v2_string, value.pointer.length) &&
                  offsetof(StringStorage, value.pointer.prefix) == offsetof(duckdb_v2_string, value.pointer.prefix) &&
                  offsetof(StringStorage, value.pointer.ptr) == offsetof(duckdb_v2_string, value.pointer.ptr) &&
                  offsetof(StringStorage, value.inlined.inlined) == offsetof(duckdb_v2_string, value.inlined.inlined),
              "StringStorage field offsets must match duckdb_v2_string");
static_assert(StringStorage::INLINE_LENGTH == DUCKDB_V2_STRING_INLINE_LENGTH,
              "StringStorage::INLINE_LENGTH must match DUCKDB_V2_STRING_INLINE_LENGTH");

StringHeap::StringHeap(void *impl) : detail::Handle<StringHeap>(impl) {
}

StringHeap::~StringHeap() {
	/* String heaps are always borrowed, so we don't destroy the handle here */
}

auto StringHeap::Allocate(idx_t byte_len) -> uint8_t * {
	uint8_t *ptr = nullptr;
	CheckedAPICall(duckdb_v2_string_heap_allocate, handle(), byte_len, &ptr);
	return ptr;
}

void StringHeap::ThrowStringTooLong(idx_t size) {
	throw Exception(DUCKDB_V2_ERROR_OUT_OF_RANGE, "Out of Range Error: string length " + std::to_string(size) +
	                                                  " exceeds the maximum a duckdb_v2_string can hold");
}

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------
// VectorType mirrors DUCKDB_V2_VECTOR_TYPE numerically; trip here if either
// side is renumbered.
static_assert(static_cast<uint8_t>(VectorType::Other) == DUCKDB_V2_VECTOR_TYPE_OTHER,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::Flat) == DUCKDB_V2_VECTOR_TYPE_FLAT,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::Constant) == DUCKDB_V2_VECTOR_TYPE_CONSTANT,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");
static_assert(static_cast<uint8_t>(VectorType::Dictionary) == DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
              "VectorType must mirror DUCKDB_V2_VECTOR_TYPE");

// VectorView mirrors duckdb_v2_vector_view. GetView copies it field-for-field,
// so the compiler checks the pointer types; this pins the one typedef the
// header cannot see.
static_assert(std::is_same<duckdb_v2_sel_t, uint32_t>::value, "VectorView::sel must mirror duckdb_v2_sel_t");

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

auto Vector::GetView() const -> VectorView {
	duckdb_v2_vector_view view {};
	CheckedAPICall(duckdb_v2_vector_get_view, handle(), &view);
	return VectorView {view.data, view.validity, view.sel, view.count};
}

auto Vector::GetVectorType() const -> VectorType {
	DUCKDB_V2_VECTOR_TYPE type = DUCKDB_V2_VECTOR_TYPE_OTHER;
	CheckedAPICall(duckdb_v2_vector_get_vector_type, handle(), &type);
	return static_cast<VectorType>(type);
}

auto Vector::GetValidityMutable() -> ValidityMask {
	uint64_t *words = nullptr;
	CheckedAPICall(duckdb_v2_vector_flat_get_validity_mutable, handle(), &words);
	return ValidityMask {words};
}

auto Vector::SetConstantValid(bool valid) -> void {
	CheckedAPICall(duckdb_v2_vector_constant_set_valid, handle(), valid);
}

auto Vector::MakeConstant(const Value &value, idx_t count) -> void {
	CheckedAPICall(duckdb_v2_vector_make_constant, handle(), value.handle(), count);
}

auto Vector::MakeSequence(int64_t start, int64_t increment, idx_t count) -> void {
	CheckedAPICall(duckdb_v2_vector_make_sequence, handle(), start, increment, count);
}

// The StringStorage <-> duckdb_v2_string casts below are sanctioned by the
// layout static_asserts above.

auto Vector::DecodeBit(const StringStorage &value) -> BitView {
	const uint8_t *data = nullptr;
	idx_t length = 0;
	uint8_t padding_bits = 0;
	CheckedAPICall(duckdb_v2_bit_decode, reinterpret_cast<const duckdb_v2_bit_t *>(&value), &data, &length,
	               &padding_bits);
	return BitView {data, length, padding_bits};
}

auto Vector::DecodeBignum(const StringStorage &value) -> Bignum {
	uint8_t *data = nullptr;
	idx_t length = 0;
	bool is_negative = false;
	CheckedAPICall(duckdb_v2_bignum_decode, reinterpret_cast<const duckdb_v2_bignum_t *>(&value), &data, &length,
	               &is_negative);
	Bignum result {std::vector<uint8_t>(data, data + length), is_negative};
	free(data);
	return result;
}

// --- Single-cell value bridge (owned by the types-values worktree) ---

auto Vector::GetValue(idx_t row) const -> Value {
	duckdb_v2_value_handle value = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_value, handle(), row, &value);
	return detail::Factory::Make<Value>(value);
}

auto Vector::SetValue(idx_t row, const Value &value) -> void {
	CheckedAPICall(duckdb_v2_vector_set_value, handle(), row, value.handle());
}

// --- end single-cell value bridge ---

auto Vector::CheckWriteRange(idx_t start, idx_t count) const -> void {
	if (count == 0) {
		return;
	}
	// A CONSTANT vector's data array holds a single slot; only index 0 is writable.
	if (GetVectorType() == VectorType::Constant && (start != 0 || count > 1)) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "Invalid Input Error: cannot assign a string to a CONSTANT vector at index != 0");
	}
}

auto Vector::AssignString(idx_t index, std::string_view data) -> void {
	CheckWriteRange(index, 1);
	auto heap = GetStringHeap();
	GetDataMutable<StringStorage>()[index] = heap.Add(data);
}

auto Vector::AssignStrings(idx_t start, const std::vector<std::string_view> &data) -> void {
	if (data.empty()) {
		return;
	}
	CheckWriteRange(start, data.size());
	auto heap = GetStringHeap();
	// Intern and place into the data array in one pass. On throw, slots
	// [start, start+i) are already written; the vector is left partially filled.
	auto *slots = GetDataMutable<StringStorage>();
	for (idx_t i = 0; i < data.size(); i++) {
		slots[start + i] = heap.Add(data[i]);
	}
}

auto Vector::GetStringHeap() -> StringHeap {
	duckdb_v2_string_heap_handle heap = nullptr;
	CheckedAPICall(duckdb_v2_vector_get_string_heap, handle(), &heap);
	return detail::Factory::Make<StringHeap>(heap);
}

auto Vector::SetString(idx_t index, StringStorage value) -> void {
	GetDataMutable<StringStorage>()[index] = value;
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

auto DataChunk::ToArrowArray(const Context &context, ArrowArray &out) const -> void {
	CheckedAPICall(duckdb_v2_data_chunk_to_arrow_array, context.handle(), handle(), &out);
}

//----------------------------------------------------------------------------------------------------------------------
// Arrow Conversion Plan
//----------------------------------------------------------------------------------------------------------------------

ArrowConversionPlan::ArrowConversionPlan(const Context &context, ArrowSchema &schema) {
	duckdb_v2_arrow_conversion_plan_handle plan = nullptr;
	CheckedAPICall(duckdb_v2_arrow_conversion_plan_create, context.handle(), &schema, &plan);
	impl = plan;
}

ArrowConversionPlan::~ArrowConversionPlan() {
	auto _h = handle();
	duckdb_v2_arrow_conversion_plan_destroy(&_h);
}

auto ArrowConversionPlan::Convert(const Context &context, ArrowArray &array) const -> DataChunk {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CheckedAPICall(duckdb_v2_arrow_array_to_data_chunk, context.handle(), &array, handle(), &chunk);
	return detail::Factory::Make<DataChunk>(chunk, true);
}

auto ArrowConversionPlan::GetSchema() const -> Schema {
	duckdb_v2_schema_handle schema = nullptr;
	CheckedAPICall(duckdb_v2_arrow_conversion_plan_get_schema, handle(), &schema);
	return detail::Factory::Make<Schema>(schema);
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

auto QueryResult::GetSchema() const -> Schema {
	duckdb_v2_schema_handle schema = nullptr;
	CheckedAPICall(duckdb_v2_result_get_schema, handle(), &schema);
	return detail::Factory::Make<Schema>(schema);
}

// ResultType mirrors DUCKDB_V2_RESULT_TYPE numerically; every member is pinned.
static_assert(static_cast<uint8_t>(QueryResult::ResultType::QUERY_RESULT) == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");
static_assert(static_cast<uint8_t>(QueryResult::ResultType::CHANGED_ROWS) == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");
static_assert(static_cast<uint8_t>(QueryResult::ResultType::NOTHING) == DUCKDB_V2_RESULT_TYPE_NOTHING,
              "ResultType must mirror DUCKDB_V2_RESULT_TYPE");

// StatementType mirrors DUCKDB_V2_STATEMENT_TYPE numerically; every member is pinned.
#define DUCKDB_CPP_ASSERT_STATEMENT_TYPE(member)                                                                       \
	static_assert(static_cast<uint8_t>(QueryResult::StatementType::member) == DUCKDB_V2_STATEMENT_TYPE_##member,       \
	              "StatementType::" #member " must mirror DUCKDB_V2_STATEMENT_TYPE_" #member)
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(INVALID);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(SELECT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(INSERT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(UPDATE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CREATE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DELETE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(PREPARE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXECUTE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ALTER);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(TRANSACTION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(COPY);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ANALYZE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(VARIABLE_SET);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CREATE_FUNC);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXPLAIN);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DROP);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXPORT);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(PRAGMA);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(VACUUM);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(CALL);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(SET);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(LOAD);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(RELATION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(EXTENSION);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(LOGICAL_PLAN);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(ATTACH);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(DETACH);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(MULTI);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(COPY_DATABASE);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(UPDATE_EXTENSIONS);
DUCKDB_CPP_ASSERT_STATEMENT_TYPE(MERGE_INTO);
#undef DUCKDB_CPP_ASSERT_STATEMENT_TYPE

auto QueryResult::GetResultType() const -> ResultType {
	DUCKDB_V2_RESULT_TYPE type = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	CheckedAPICall(duckdb_v2_result_get_result_type, handle(), &type);
	return static_cast<ResultType>(type);
}

auto QueryResult::GetStatementType() const -> StatementType {
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CheckedAPICall(duckdb_v2_result_get_statement_type, handle(), &type);
	return static_cast<StatementType>(type);
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

auto QueryResult::RenderBox(idx_t max_rows, idx_t max_width, idx_t max_col_width, const std::string &null_value,
                            idx_t render_mode, idx_t limit) -> std::string {
	auto raw = handle();
	this->release();
	char *text = nullptr;
	CheckedAPICall(duckdb_v2_result_render_box, &raw, max_rows, max_width, max_col_width, ToStr(null_value),
	               render_mode, limit, &text);
	auto out = std::string(text);
	free(text);
	return out;
}

auto QueryResult::ToArrowStream(idx_t batch_size) -> ArrowStream {
	// Allocate before detaching the result: if this throws, the result wrapper
	// is still owned by *this and ~QueryResult frees it (no leak).
	auto *stream = new ArrowArrayStream {};
	auto raw = handle();
	// result_to_arrow_stream consumes the result by transfer (a valid handle is
	// consumed on success and failure alike). Detach our wrapper now so
	// ~QueryResult never double-frees the transferred result.
	this->release();
	try {
		CheckedAPICall(duckdb_v2_result_to_arrow_stream, &raw, batch_size, stream);
	} catch (...) {
		delete stream;
		throw;
	}
	return detail::Factory::Make<ArrowStream>(stream);
}

//----------------------------------------------------------------------------------------------------------------------
// Arrow Stream
//----------------------------------------------------------------------------------------------------------------------

ArrowStream::~ArrowStream() {
	if (stream) {
		if (stream->release) {
			stream->release(stream);
		}
		delete stream;
	}
}

// The Arrow C stream interface reports failure only as an errno-style int, with
// no error code, so GetSchema/Next surface a generic INVALID_INPUT code; the
// real detail comes through get_last_error and is carried in the message.
void ArrowStream::GetSchema(ArrowSchema &out) const {
	if (!stream || !stream->release) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT, "ArrowStream::GetSchema on an empty stream");
	}
	if (stream->get_schema(stream, &out) != 0) {
		const char *msg = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT, msg ? msg : "Arrow stream get_schema failed");
	}
}

bool ArrowStream::Next(ArrowArray &out) const {
	out.release = nullptr;
	if (!stream || !stream->release) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT, "ArrowStream::Next on an empty stream");
	}
	if (stream->get_next(stream, &out) != 0) {
		const char *msg = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT, msg ? msg : "Arrow stream get_next failed");
	}
	return out.release != nullptr;
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

auto ColumnDataCollection::Combine(ColumnDataCollection other) -> void {
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

	LogStorageInfo(LogStorage::LogCallback log_callback, detail::UserData user_data)
	    : log_callback(log_callback), user_data(std::move(user_data)) {
	}

	bool operator==(const LogStorageInfo &other) const {
		return log_callback == other.log_callback && user_data.get() == other.user_data.get();
	}
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
	auto info = detail::MakeUserData<LogStorageInfo>(callback, std::move(user_data));
	CheckedAPICall(duckdb_v2_log_storage_builder_set_user_data, handle(), info);
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

auto ScalarFunction::SetStability(FunctionStability value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               ToCValue(value));
	return *this;
}
auto ScalarFunction::GetStability() const -> FunctionStability {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_scalar_function_builder_get_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               &value);
	return FromCStability(value);
}

auto ScalarFunction::SetNullHandling(FunctionNullHandling value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
	               ToCValue(value));
	return *this;
}
auto ScalarFunction::GetNullHandling() const -> FunctionNullHandling {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_scalar_function_builder_get_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
	               &value);
	return FromCNullHandling(value);
}

auto ScalarFunction::SetFallibility(FunctionFallibility value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               ToCValue(value));
	return *this;
}
auto ScalarFunction::GetFallibility() const -> FunctionFallibility {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_scalar_function_builder_get_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               &value);
	return FromCFallibility(value);
}

auto ScalarFunction::SetCollationHandling(FunctionCollationHandling value) & -> ScalarFunction & {
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING, ToCValue(value));
	return *this;
}
auto ScalarFunction::GetCollationHandling() const -> FunctionCollationHandling {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_scalar_function_builder_get_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING, &value);
	return FromCCollationHandling(value);
}

namespace {

// Shared guard for the function builders' GetUserData: the builder's info
// table rides the C user_data slot; the user's own slot lives inside it.
// `setter` names the builder's SetUserData for the error message.
void *RequireUserData(const detail::UserData &user_data, const char *setter) {
	auto ptr = user_data.get();
	if (!ptr) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                std::string("no user data was set; call ") + setter + " before Register");
	}
	return ptr;
}

// Shared guard for the function builders' phase GetBindData: a clear error
// instead of a null deref. Templated so const and non-const pointers pass.
template <class PTR>
PTR *RequireBindData(PTR *ptr) {
	if (!ptr) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "no bind data was set; call BindInput::SetBindData in the bind callback");
	}
	return ptr;
}

} // namespace

struct ScalarFunctionInfo {
	ScalarFunction::BindCallback bind_callback = nullptr;
	ScalarFunction::InitCallback init_callback = nullptr;
	ScalarFunction::ExecCallback exec_callback = nullptr;
	// The user's own slot (ScalarFunction::SetUserData), destroyed with this
	// object at engine teardown.
	detail::UserData user_data;

	ScalarFunctionInfo(ScalarFunction::BindCallback bind_callback, ScalarFunction::InitCallback init_callback,
	                   ScalarFunction::ExecCallback exec_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), init_callback(init_callback), exec_callback(exec_callback),
	      user_data(std::move(user_data)) {
	}

	bool operator==(const ScalarFunctionInfo &other) const {
		return bind_callback == other.bind_callback && init_callback == other.init_callback &&
		       exec_callback == other.exec_callback && user_data.get() == other.user_data.get();
	}
};

void *ScalarFunction::BindInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_scalar_function_bind_args *>(args)->out_bind_data.ptr);
}

void *ScalarFunction::BindInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const ScalarFunctionInfo *>(static_cast<duckdb_v2_scalar_function_bind_args *>(args)->user_data);
	return RequireUserData(function.user_data, "ScalarFunction::SetUserData");
}

void ScalarFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                    void (*destructor)(void *)) {
	duckdb_v2_scalar_function_bind_args *args_struct = static_cast<duckdb_v2_scalar_function_bind_args *>(args);
	args_struct->out_bind_data = duckdb_v2_opaque {data, destructor, equals};
}

void *ScalarFunction::InitInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_scalar_function_init_args *>(args)->bind_data);
}

void *ScalarFunction::InitInput::GetWorkerStateInternal() const {
	return static_cast<duckdb_v2_scalar_function_init_args *>(args)->out_init_data.ptr;
}

void ScalarFunction::InitInput::SetWorkerStateInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_scalar_function_init_args *>(args);
	args_struct->out_init_data = duckdb_v2_opaque {data, destructor, nullptr};
}

void *ScalarFunction::InitInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const ScalarFunctionInfo *>(static_cast<duckdb_v2_scalar_function_init_args *>(args)->user_data);
	return RequireUserData(function.user_data, "ScalarFunction::SetUserData");
}

auto ScalarFunction::InitInput::HasContext() const -> bool {
	return context != nullptr;
}

auto ScalarFunction::InitInput::GetContext() const -> Context {
	if (!context) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "Invalid Input Error: this invocation runs without a client context");
	}
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

void *ScalarFunction::ExecInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_scalar_function_exec_args *>(args)->bind_data);
}

void *ScalarFunction::ExecInput::GetWorkerStateInternal() const {
	return static_cast<duckdb_v2_scalar_function_exec_args *>(args)->init_data;
}

void *ScalarFunction::ExecInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const ScalarFunctionInfo *>(static_cast<duckdb_v2_scalar_function_exec_args *>(args)->user_data);
	return RequireUserData(function.user_data, "ScalarFunction::SetUserData");
}

auto ScalarFunction::ExecInput::GetInputChunk() const -> DataChunk {
	auto chunk = static_cast<duckdb_v2_scalar_function_exec_args *>(args)->input;
	return detail::Factory::Make<DataChunk>(chunk, false);
}

auto ScalarFunction::ExecInput::GetResultVector() const -> Vector {
	auto vec = static_cast<duckdb_v2_scalar_function_exec_args *>(args)->result;
	return detail::Factory::Make<Vector>(vec);
}

auto ScalarFunction::ExecInput::HasContext() const -> bool {
	return context != nullptr;
}

auto ScalarFunction::ExecInput::GetContext() const -> Context {
	if (!context) {
		throw Exception(DUCKDB_V2_ERROR_INVALID_INPUT,
		                "Invalid Input Error: this invocation runs without an execution context");
	}
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

auto ScalarFunction::SetBindCallback(BindCallback callback) & -> ScalarFunction & {
	if (!callback) {
		// Reset

		CheckedAPICall(duckdb_v2_scalar_function_builder_set_bind_callback, handle(), nullptr);

		bind_callback = nullptr;

		return *this;
	}

	static auto trampoline = [](duckdb_v2_scalar_function_bind_args *args, duckdb_v2_context_handle /*context*/,
	                            duckdb_v2_error_info_handle *err) {
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

	static auto trampoline = [](duckdb_v2_scalar_function_init_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<InitInput>(args, context);

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

	static auto trampoline = [](duckdb_v2_scalar_function_exec_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<ScalarFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<ExecInput>(args, context);

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_scalar_function_builder_set_exec_callback, handle(), trampoline);

	// And set the exec callback. This will be set in the user_data once the function is registered.
	exec_callback = callback;

	return *this;
}

auto ScalarFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

void ScalarFunction::Register(const Context &ctx) {
	// The callback table rides the C builder user_data slot so the
	// trampolines can find it; the user's own data (SetUserData, moved out
	// here) rides inside it.
	auto info =
	    detail::MakeUserData<ScalarFunctionInfo>(bind_callback, init_callback, exec_callback, std::move(user_data));
	CheckedAPICall(duckdb_v2_scalar_function_builder_set_user_data, handle(), info);

	CheckedAPICall(duckdb_v2_scalar_function_builder_register, ctx.handle(), handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Aggregate Function
//----------------------------------------------------------------------------------------------------------------------

class AggregateFunctionInfo {
public:
	AggregateFunction::BindCallback bind_callback = nullptr;
	AggregateFunction::SizeCallback size_callback = nullptr;
	AggregateFunction::InitializeCallback initialize_callback = nullptr;
	AggregateFunction::UpdateCallback update_callback = nullptr;
	AggregateFunction::CombineCallback combine_callback = nullptr;
	AggregateFunction::FinalizeCallback finalize_callback = nullptr;
	AggregateFunction::DestroyCallback destroy_callback = nullptr;

	// The user's own slot (AggregateFunction::SetUserData), destroyed with
	// this object at engine teardown.
	detail::UserData user_data;

	AggregateFunctionInfo(AggregateFunction::BindCallback bind_callback, AggregateFunction::SizeCallback size_callback,
	                      AggregateFunction::InitializeCallback initialize_callback,
	                      AggregateFunction::UpdateCallback update_callback,
	                      AggregateFunction::CombineCallback combine_callback,
	                      AggregateFunction::FinalizeCallback finalize_callback,
	                      AggregateFunction::DestroyCallback destroy_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), size_callback(size_callback), initialize_callback(initialize_callback),
	      update_callback(update_callback), combine_callback(combine_callback), finalize_callback(finalize_callback),
	      destroy_callback(destroy_callback), user_data(std::move(user_data)) {
	}

	bool operator==(const AggregateFunctionInfo &other) const {
		// We only compare the presence of callbacks, not their actual function pointers, since the latter can be
		// wrapped in different trampoline layers.
		return (bind_callback != nullptr) == (other.bind_callback != nullptr) &&
		       (size_callback != nullptr) == (other.size_callback != nullptr) &&
		       (initialize_callback != nullptr) == (other.initialize_callback != nullptr) &&
		       (update_callback != nullptr) == (other.update_callback != nullptr) &&
		       (combine_callback != nullptr) == (other.combine_callback != nullptr) &&
		       (finalize_callback != nullptr) == (other.finalize_callback != nullptr) &&
		       (destroy_callback != nullptr) == (other.destroy_callback != nullptr) &&
		       user_data.get() == other.user_data.get();
	}
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

auto AggregateFunction::SetStability(FunctionStability value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               ToCValue(value));
	return *this;
}
auto AggregateFunction::GetStability() const -> FunctionStability {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	               &value);
	return FromCStability(value);
}

auto AggregateFunction::SetNullHandling(FunctionNullHandling value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING, ToCValue(value));
	return *this;
}
auto AggregateFunction::GetNullHandling() const -> FunctionNullHandling {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING, &value);
	return FromCNullHandling(value);
}

auto AggregateFunction::SetFallibility(FunctionFallibility value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               ToCValue(value));
	return *this;
}
auto AggregateFunction::GetFallibility() const -> FunctionFallibility {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(), DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
	               &value);
	return FromCFallibility(value);
}

auto AggregateFunction::SetCollationHandling(FunctionCollationHandling value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING, ToCValue(value));
	return *this;
}
auto AggregateFunction::GetCollationHandling() const -> FunctionCollationHandling {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING, &value);
	return FromCCollationHandling(value);
}

auto AggregateFunction::SetOrderDependence(OrderDependence value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT, ToCValue(value));
	return *this;
}
auto AggregateFunction::GetOrderDependence() const -> OrderDependence {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT, &value);
	return FromCOrderDependence(value);
}

auto AggregateFunction::SetDistinctDependence(DistinctDependence value) & -> AggregateFunction & {
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT, ToCValue(value));
	return *this;
}
auto AggregateFunction::GetDistinctDependence() const -> DistinctDependence {
	DUCKDB_V2_FUNCTION_PROPERTY_VALUE value;
	CheckedAPICall(duckdb_v2_aggregate_function_builder_get_property, handle(),
	               DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT, &value);
	return FromCDistinctDependence(value);
}

void AggregateFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                       void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_aggregate_function_bind_args *>(args);
	args_struct->out_bind_data = duckdb_v2_opaque {data, destructor, equals};
}

void *AggregateFunction::BindInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_aggregate_function_bind_args *>(args)->out_bind_data.ptr);
}

void *AggregateFunction::BindInput::GetUserDataInternal() const {
	const auto &function = *static_cast<const AggregateFunctionInfo *>(
	    static_cast<duckdb_v2_aggregate_function_bind_args *>(args)->user_data);
	return RequireUserData(function.user_data, "AggregateFunction::SetUserData");
}

auto AggregateFunction::SetBindCallback(BindCallback callback) & -> AggregateFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_aggregate_function_builder_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_aggregate_function_bind_args *args, duckdb_v2_context_handle /*context*/,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<AggregateFunctionInfo *>(args->user_data);

			auto input = detail::Factory::Make<BindInput>(args);

			// Now call the user callback
			function.bind_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_bind_callback, handle(), trampoline);

	bind_callback = callback;

	return *this;
}

class AggregateFunction::SizeInput::Inner {
public:
	idx_t size_in_bytes = 0;
	const AggregateFunctionInfo *info = nullptr;
};

void AggregateFunction::SizeInput::Reserve(idx_t size_in_bytes) {
	inner.size_in_bytes = size_in_bytes;
}

auto AggregateFunction::SizeInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
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
			inner.info = &function;

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
	const AggregateFunctionInfo *info = nullptr;
};

void *AggregateFunction::InitializeInput::GetStatePointer() const {
	return inner.state;
}

auto AggregateFunction::InitializeInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
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
			inner.info = &function;

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
	const AggregateFunctionInfo *info = nullptr;
	const void *bind_data = nullptr;
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

auto AggregateFunction::UpdateInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
}

auto AggregateFunction::UpdateInput::GetBindDataInternal() const -> const void * {
	return RequireBindData(inner.bind_data);
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

			UpdateInput::Inner inner {args->count, args->states, detail::Factory::Make<DataChunk>(args->input, false),
			                          &function, args->bind_data};

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
	const AggregateFunctionInfo *info = nullptr;
	const void *bind_data = nullptr;
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

auto AggregateFunction::CombineInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
}

auto AggregateFunction::CombineInput::GetBindDataInternal() const -> const void * {
	return RequireBindData(inner.bind_data);
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

			CombineInput::Inner inner {args->count, args->sources, args->targets, &function, args->bind_data};

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
	const AggregateFunctionInfo *info = nullptr;
	const void *bind_data = nullptr;
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

auto AggregateFunction::FinalizeInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
}

auto AggregateFunction::FinalizeInput::GetBindDataInternal() const -> const void * {
	return RequireBindData(inner.bind_data);
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

			FinalizeInput::Inner inner {args->count,         args->states, detail::Factory::Make<Vector>(args->result),
			                            args->result_offset, &function,    args->bind_data};

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
	const AggregateFunctionInfo *info = nullptr;
	const void *bind_data = nullptr;
};

auto AggregateFunction::DestroyInput::GetStateArray() const -> void ** {
	return inner.states;
}

auto AggregateFunction::DestroyInput::GetStateCount() const -> idx_t {
	return inner.count;
}

auto AggregateFunction::DestroyInput::GetUserDataInternal() const -> void * {
	return RequireUserData(inner.info->user_data, "AggregateFunction::SetUserData");
}

auto AggregateFunction::DestroyInput::GetBindDataInternal() const -> const void * {
	return RequireBindData(inner.bind_data);
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

			DestroyInput::Inner inner {args->count, args->states, &function, args->bind_data};
			DestroyInput input {inner};

			// Now call the user callback
			function.destroy_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_destroy_callback, handle(), trampoline);

	destroy_callback = callback;

	return *this;
}

auto AggregateFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

void AggregateFunction::Register(const Context &ctx) {
	// The callback table rides the C builder user_data slot so the
	// trampolines can find it; the user's own data (SetUserData, moved out
	// here) rides inside it.
	auto info = detail::MakeUserData<AggregateFunctionInfo>(bind_callback, size_callback, initialize_callback,
	                                                        update_callback, combine_callback, finalize_callback,
	                                                        destroy_callback, std::move(user_data));
	CheckedAPICall(duckdb_v2_aggregate_function_builder_set_user_data, handle(), info);

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
	TableFunction::PushdownCallback pushdown_callback = nullptr;
	// The user's own slot (TableFunction::SetUserData), destroyed with this
	// object at engine teardown.
	detail::UserData user_data;

	TableFunctionInfo(TableFunction::BindCallback bind_callback, TableFunction::InitGlobalCallback init_global_callback,
	                  TableFunction::InitLocalCallback init_local_callback, TableFunction::ExecCallback exec_callback,
	                  TableFunction::PushdownCallback pushdown_callback, detail::UserData user_data)
	    : bind_callback(bind_callback), init_global_callback(init_global_callback),
	      init_local_callback(init_local_callback), exec_callback(exec_callback), pushdown_callback(pushdown_callback),
	      user_data(std::move(user_data)) {
	}

	bool operator==(const TableFunctionInfo &other) const {
		return bind_callback == other.bind_callback && init_global_callback == other.init_global_callback &&
		       init_local_callback == other.init_local_callback && exec_callback == other.exec_callback &&
		       pushdown_callback == other.pushdown_callback && user_data.get() == other.user_data.get();
	}
};

namespace {

// The C user_data slot carries the wrapper's TableFunctionInfo; the user's
// slot lives inside it.
void *GetTableFunctionUserData(const TableFunctionInfo &function) {
	return RequireUserData(function.user_data, "TableFunction::SetUserData");
}

} // namespace

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

auto TableFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

class TableFunction::BindInput::Inner {
public:
	duckdb_v2_table_function_bind_info_handle info = nullptr;
	duckdb_v2_context_handle ctx = nullptr;
};

void *TableFunction::BindInput::GetUserDataInternal() const {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_bind_get_user_data, inner.info, &data);
	return GetTableFunctionUserData(*static_cast<TableFunctionInfo *>(data));
}

void TableFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                   void (*destructor)(void *)) {
	CheckedAPICall(duckdb_v2_table_function_bind_set_bind_data, inner.info,
	               duckdb_v2_opaque {data, destructor, equals});
}

auto TableFunction::BindInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(inner.ctx);
}

auto TableFunction::BindInput::SetCardinality(idx_t cardinality, bool is_exact) -> void {
	CheckedAPICall(duckdb_v2_table_function_bind_set_cardinality, inner.info, cardinality, is_exact);
}

auto TableFunction::BindInput::AddResultColumn(const std::string &name, const LogicalType &type) -> void {
	CheckedAPICall(duckdb_v2_table_function_bind_add_result_column, inner.info, ToStr(name), type.handle());
}

auto TableFunction::BindInput::AddResultColumns(const Schema &schema) -> void {
	const auto count = schema.GetFieldCount();
	for (idx_t i = 0; i < count; i++) {
		const auto name = schema.GetFieldName(i);
		const auto type = schema.GetFieldType(i);
		CheckedAPICall(duckdb_v2_table_function_bind_add_result_column, inner.info,
		               duckdb_v2_str {name.data(), name.size()}, type.handle());
	}
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

			BindInput::Inner inner {info, ctx};
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
	duckdb_v2_context_handle ctx = nullptr;
};

auto TableFunction::InitGlobalInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_bind_data, inner.info, &data);
	return RequireBindData(data);
}

auto TableFunction::InitGlobalInput::SetGlobalStateInternal(void *data, void (*destructor)(void *)) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_set_global_state, inner.info,
	               duckdb_v2_opaque {data, destructor, nullptr});
}

auto TableFunction::InitGlobalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(inner.ctx);
}

auto TableFunction::InitGlobalInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_init_get_column_count, inner.info, &count);
	return count;
}

auto TableFunction::InitGlobalInput::GetColumnIndex(idx_t projected_index) const -> idx_t {
	idx_t original = 0;
	CheckedAPICall(duckdb_v2_table_function_init_get_column_index, inner.info, projected_index, &original);
	return original;
}

auto TableFunction::InitGlobalInput::SetMaxThreads(idx_t max_threads) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_set_max_threads, inner.info, max_threads);
}

auto TableFunction::InitGlobalInput::GetUserDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_user_data, inner.info, &data);
	return GetTableFunctionUserData(*static_cast<TableFunctionInfo *>(data));
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

			InitGlobalInput::Inner inner {info, ctx};
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
	duckdb_v2_context_handle ctx = nullptr;
};

auto TableFunction::InitLocalInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_bind_data, inner.info, &data);
	return RequireBindData(data);
}

auto TableFunction::InitLocalInput::GetGlobalStateInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_global_state, inner.info, &data);
	return data;
}

auto TableFunction::InitLocalInput::SetLocalStateInternal(void *data, void (*destructor)(void *)) -> void {
	CheckedAPICall(duckdb_v2_table_function_init_set_local_state, inner.info,
	               duckdb_v2_opaque {data, destructor, nullptr});
}

auto TableFunction::InitLocalInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(inner.ctx);
}

auto TableFunction::InitLocalInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_init_get_column_count, inner.info, &count);
	return count;
}

auto TableFunction::InitLocalInput::GetColumnIndex(idx_t projected_index) const -> idx_t {
	idx_t original = 0;
	CheckedAPICall(duckdb_v2_table_function_init_get_column_index, inner.info, projected_index, &original);
	return original;
}

auto TableFunction::InitLocalInput::GetUserDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_init_get_user_data, inner.info, &data);
	return GetTableFunctionUserData(*static_cast<TableFunctionInfo *>(data));
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

			InitLocalInput::Inner inner {info, ctx};
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
	duckdb_v2_context_handle ctx = nullptr;
};

auto TableFunction::ExecInput::GetBindDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_bind_data, inner.info, &data);
	return RequireBindData(data);
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

auto TableFunction::ExecInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(inner.ctx);
}

auto TableFunction::ExecInput::GetUserDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_exec_get_user_data, inner.info, &data);
	return GetTableFunctionUserData(*static_cast<TableFunctionInfo *>(data));
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

			ExecInput::Inner inner {info, detail::Factory::Make<DataChunk>(output_chunk, false), ctx};
			ExecInput input {inner};

			// Now call the user callback
			function.exec_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_exec_callback, handle(), trampoline);

	exec_callback = callback;

	return *this;
}

class TableFunction::PushdownInput::Inner {
public:
	duckdb_v2_table_function_filter_info_handle info = nullptr;
	void *bind_data = nullptr;
	duckdb_v2_context_handle ctx = nullptr;
};

auto TableFunction::PushdownInput::GetBindDataInternal() const -> void * {
	return RequireBindData(inner.bind_data);
}

auto TableFunction::PushdownInput::GetUserDataInternal() const -> void * {
	void *data = nullptr;
	CheckedAPICall(duckdb_v2_table_function_filter_get_user_data, inner.info, &data);
	return GetTableFunctionUserData(*static_cast<TableFunctionInfo *>(data));
}

auto TableFunction::PushdownInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(inner.ctx);
}

auto TableFunction::PushdownInput::GetCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_get_count, inner.info, &count);
	return count;
}

auto TableFunction::PushdownInput::GetColumnCount() const -> idx_t {
	idx_t count = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_get_column_count, inner.info, &count);
	return count;
}

auto TableFunction::PushdownInput::GetColumnIndex(idx_t index) const -> idx_t {
	idx_t column = 0;
	CheckedAPICall(duckdb_v2_table_function_filter_get_column_index, inner.info, index, &column);
	return column;
}

auto TableFunction::PushdownInput::GetExpression(idx_t index) const -> Expression {
	duckdb_v2_expression_handle expression = nullptr;
	CheckedAPICall(duckdb_v2_table_function_filter_get_expression, inner.info, index, &expression);
	return detail::Factory::Make<Expression>(expression);
}

auto TableFunction::PushdownInput::MarkHandled(idx_t index) -> void {
	CheckedAPICall(duckdb_v2_table_function_filter_mark_handled, inner.info, index);
}

auto TableFunction::SetPushdownComplexFilterCallback(PushdownCallback callback) & -> TableFunction & {
	if (!callback) {
		// Reset
		CheckedAPICall(duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback, handle(), nullptr);
		pushdown_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](void *bind_data, duckdb_v2_table_function_filter_info_handle info,
	                            duckdb_v2_context_handle ctx, duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			void *user_data = nullptr;
			CheckedAPICall(duckdb_v2_table_function_filter_get_user_data, info, &user_data);

			const auto &function = *static_cast<TableFunctionInfo *>(user_data);

			PushdownInput::Inner inner {info, bind_data, ctx};
			PushdownInput input {inner};

			// Now call the user callback
			function.pushdown_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback, handle(), trampoline);

	pushdown_callback = callback;

	return *this;
}

auto TableFunction::SetProjectionPushdown(bool enable) & -> TableFunction & {
	CheckedAPICall(duckdb_v2_table_function_builder_set_projection_pushdown, handle(), enable);
	return *this;
}

void TableFunction::Register(const Context &ctx) {
	// The callback table rides the C builder user_data slot so the
	// trampolines can find it; the user's own data (SetUserData, moved out
	// here) rides inside it.
	auto info = detail::MakeUserData<TableFunctionInfo>(bind_callback, init_global_callback, init_local_callback,
	                                                    exec_callback, pushdown_callback, std::move(user_data));
	CheckedAPICall(duckdb_v2_table_function_builder_set_user_data, handle(), info);

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
	// The user's own slot (CopyFunction::SetUserData), destroyed with this
	// object at engine teardown.
	detail::UserData user_data;

	CopyFunctionInfo(CopyFunction::BindCallback bind, CopyFunction::InitCallback init,
	                 CopyFunction::BatchCallback batch, CopyFunction::FlushCallback flush,
	                 CopyFunction::FinalizeCallback finalize, detail::UserData user_data)
	    : bind_callback(bind), init_callback(init), batch_callback(batch), flush_callback(flush),
	      finalize_callback(finalize), user_data(std::move(user_data)) {
	}

	bool operator==(const CopyFunctionInfo &other) const {
		return bind_callback == other.bind_callback && init_callback == other.init_callback &&
		       batch_callback == other.batch_callback && flush_callback == other.flush_callback &&
		       finalize_callback == other.finalize_callback && user_data.get() == other.user_data.get();
	}
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
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
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

void CopyFunction::BindInput::SetBindDataInternal(void *data, bool (*equals)(void *a, void *b),
                                                  void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_bind_args *>(args);
	args_struct->out_bind_data = duckdb_v2_opaque {data, destructor, equals};
}

void *CopyFunction::BindInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const CopyFunctionInfo *>(static_cast<duckdb_v2_copy_function_bind_args *>(args)->user_data);
	return RequireUserData(function.user_data, "CopyFunction::SetUserData");
}

// --- Init input ---

auto CopyFunction::InitInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

auto CopyFunction::InitInput::GetFilePath() const -> std::string_view {
	return FromStr(static_cast<duckdb_v2_copy_function_init_args *>(args)->file_path);
}

const void *CopyFunction::InitInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_copy_function_init_args *>(args)->bind_data);
}

void CopyFunction::InitInput::SetInitDataInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_init_args *>(args);
	args_struct->out_init_data = duckdb_v2_opaque {data, destructor, nullptr};
}

void *CopyFunction::InitInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const CopyFunctionInfo *>(static_cast<duckdb_v2_copy_function_init_args *>(args)->user_data);
	return RequireUserData(function.user_data, "CopyFunction::SetUserData");
}

// --- Batch input ---

auto CopyFunction::BatchInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

const void *CopyFunction::BatchInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_copy_function_batch_args *>(args)->bind_data);
}

void *CopyFunction::BatchInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_batch_args *>(args)->init_data;
}

void CopyFunction::BatchInput::SetBatchDataInternal(void *data, void (*destructor)(void *)) {
	auto args_struct = static_cast<duckdb_v2_copy_function_batch_args *>(args);
	args_struct->out_batch = duckdb_v2_opaque {data, destructor, nullptr};
}

void *CopyFunction::BatchInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const CopyFunctionInfo *>(static_cast<duckdb_v2_copy_function_batch_args *>(args)->user_data);
	return RequireUserData(function.user_data, "CopyFunction::SetUserData");
}

// --- Flush input ---

auto CopyFunction::FlushInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

const void *CopyFunction::FlushInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_copy_function_flush_args *>(args)->bind_data);
}

void *CopyFunction::FlushInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_flush_args *>(args)->init_data;
}

void *CopyFunction::FlushInput::GetBatchDataInternal() const {
	return static_cast<duckdb_v2_copy_function_flush_args *>(args)->in_batch;
}

void *CopyFunction::FlushInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const CopyFunctionInfo *>(static_cast<duckdb_v2_copy_function_flush_args *>(args)->user_data);
	return RequireUserData(function.user_data, "CopyFunction::SetUserData");
}

// --- Finalize input ---

auto CopyFunction::FinalizeInput::GetContext() const -> Context {
	return detail::Factory::Make<Context>(static_cast<duckdb_v2_context_handle>(context));
}

const void *CopyFunction::FinalizeInput::GetBindDataInternal() const {
	return RequireBindData(static_cast<duckdb_v2_copy_function_finalize_args *>(args)->bind_data);
}

void *CopyFunction::FinalizeInput::GetInitDataInternal() const {
	return static_cast<duckdb_v2_copy_function_finalize_args *>(args)->init_data;
}

void *CopyFunction::FinalizeInput::GetUserDataInternal() const {
	const auto &function =
	    *static_cast<const CopyFunctionInfo *>(static_cast<duckdb_v2_copy_function_finalize_args *>(args)->user_data);
	return RequireUserData(function.user_data, "CopyFunction::SetUserData");
}

// --- Callback registration ---

auto CopyFunction::SetBindCallback(BindCallback callback) & -> CopyFunction & {
	if (!callback) {
		CheckedAPICall(duckdb_v2_copy_function_builder_set_bind_callback, handle(), nullptr);
		bind_callback = nullptr;
		return *this;
	}

	static auto trampoline = [](duckdb_v2_copy_function_bind_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<BindInput>(args, context);
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

	static auto trampoline = [](duckdb_v2_copy_function_init_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<InitInput>(args, context);
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

	static auto trampoline = [](duckdb_v2_copy_function_batch_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			// Ownership of the collection is handed to us; wrap it so it is destroyed when the callback returns.
			auto collection = detail::Factory::Make<ColumnDataCollection>(static_cast<void *>(args->in_batch));
			auto input = detail::Factory::Make<BatchInput>(args, context, std::move(collection));
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

	static auto trampoline = [](duckdb_v2_copy_function_flush_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<FlushInput>(args, context);
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

	static auto trampoline = [](duckdb_v2_copy_function_finalize_args *args, duckdb_v2_context_handle context,
	                            duckdb_v2_error_info_handle *err) {
		WithExceptionGuard(err, [&]() {
			const auto &function = *static_cast<const CopyFunctionInfo *>(args->user_data);
			auto input = detail::Factory::Make<FinalizeInput>(args, context);
			function.finalize_callback(input);
		});
	};

	CheckedAPICall(duckdb_v2_copy_function_builder_set_finalize_callback, handle(), trampoline);
	finalize_callback = callback;
	return *this;
}

auto CopyFunction::SetUserDataInternal(void *data, void (*destructor)(void *)) -> void {
	user_data = detail::UserData(data, destructor);
}

auto CopyFunction::Register(const Context &ctx) -> void {
	// The callback table rides the C builder user_data slot so the
	// trampolines can find it; the user's own data (SetUserData, moved out
	// here) rides inside it.
	auto info = detail::MakeUserData<CopyFunctionInfo>(bind_callback, init_callback, batch_callback, flush_callback,
	                                                   finalize_callback, std::move(user_data));
	CheckedAPICall(duckdb_v2_copy_function_builder_set_user_data, handle(), info);

	CheckedAPICall(duckdb_v2_copy_function_builder_register, ctx.handle(), handle());
}

//----------------------------------------------------------------------------------------------------------------------
// Cast Function
//----------------------------------------------------------------------------------------------------------------------

struct CastFunctionInfo {
	CastFunction::ExecCallback exec_callback = nullptr;

	explicit CastFunctionInfo(CastFunction::ExecCallback exec_callback) : exec_callback(exec_callback) {
	}

	bool operator==(const CastFunctionInfo &other) const {
		return exec_callback == other.exec_callback;
	}
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
	auto info = detail::MakeUserData<CastFunctionInfo>(exec_callback);
	CheckedAPICall(duckdb_v2_cast_function_builder_set_user_data, handle(), info);

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
