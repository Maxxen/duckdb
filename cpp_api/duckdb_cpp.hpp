#pragma once

#include <functional>
#include <utility>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <memory>

// (Experimental) Stable C++ API

// Arrow C Data Interface structs (defined by duckdb_v2.h, or by the consumer's
// own Arrow headers under the shared ARROW_C_*_INTERFACE guards). Forward
// declared so this header stays free of any concrete Arrow definition.
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

namespace duckdb_api {

typedef uint64_t idx_t;

//----------------------------------------------------------------------------------------------------------------------
// Internal Implementation Details
//----------------------------------------------------------------------------------------------------------------------

namespace detail {

// Maps a C++ wrapper type to its underlying C-API handle type. The primary template is intentionally left undefined.
// Each wrapper is specialized in the .cpp, so the handle types never need to appear in this header.
template <class T>
struct HandleTraits;

template <class TYPE>
class Handle {
public:
	Handle(const Handle &) = delete;
	Handle &operator=(const Handle &) = delete;

	Handle(Handle &&other) noexcept : impl(other.impl) {
		other.impl = nullptr;
	}
	Handle &operator=(Handle &&other) noexcept {
		std::swap(impl, other.impl);
		return *this;
	}

	// True when this wrapper holds a live handle. Moved-from wrappers and
	// "no value" results (e.g. end-of-stream chunks) are empty.
	explicit operator bool() const noexcept {
		return impl != nullptr;
	}

	// Detaches and returns the underlying handle, leaving the wrapper
	// empty; for transfer-semantics calls where the C API consumes the
	// handle.
	void *release() noexcept {
		auto *detached = impl;
		impl = nullptr;
		return detached;
	}

	virtual ~Handle() noexcept = default;

protected:
	Handle() : impl(nullptr) {
	}
	explicit Handle(void *impl) : impl(impl) {
	}

	void *impl;

public:
	// Returns the underlying C-API handle, indirectly typed via HandleTraits<TYPE>.
	// This is a member template so it is only instantiated where it is called
	// (in the .cpp, after the matching HandleTraits specialization is visible).
	// This keeps handle types out of this header.
	template <class TR = HandleTraits<TYPE>>
	typename TR::handle handle() const {
		return static_cast<typename TR::handle>(impl);
	}
};

struct Factory {
	template <class T, class... ARGS>
	static T Make(ARGS &&... args) {
		return T(std::forward<ARGS>(args)...);
	}
};

template <class T>
void TypedDelete(void *ptr) {
	delete static_cast<T *>(ptr);
}

template <class T>
bool TypedEquals(void *ptr_a, void *ptr_b) {
	auto typed_a = static_cast<T *>(ptr_a);
	auto typed_b = static_cast<T *>(ptr_b);
	return *typed_a == *typed_b;
}

// Helper class to hold "user data" pointers along with their destructors.
struct UserData {
public:
	UserData() : data(nullptr), destructor(nullptr) {
	}
	UserData(void *data, void (*destructor)(void *)) : data(data), destructor(destructor) {
	}

	// Not copyable
	UserData(const UserData &) = delete;
	UserData &operator=(const UserData &) = delete;

	// Movable
	UserData(UserData &&other) noexcept : data(other.data), destructor(other.destructor) {
		other.data = nullptr;
		other.destructor = nullptr;
	}

	UserData &operator=(UserData &&other) noexcept {
		if (this != &other) {
			if (data && destructor) {
				destructor(data);
			}
			data = other.data;
			destructor = other.destructor;
			other.data = nullptr;
			other.destructor = nullptr;
		}
		return *this;
	}

	~UserData() {
		if (data && destructor) {
			destructor(data);
		}
	}

	auto get() const -> void * {
		return data;
	}

private:
	void *data;
	void (*destructor)(void *);
};

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Exceptions
//----------------------------------------------------------------------------------------------------------------------

class Exception : public std::runtime_error {
public:
	// TODO: add more exception types!
	Exception(int code, std::string message, std::string raw_message = {})
	    : std::runtime_error(std::move(message)), code(code), raw_message(std::move(raw_message)) {
	}

	int GetCode() const {
		return code;
	}

	// The message body with what()'s "<Type> Error: " prefix stripped, or empty.
	// Not derivable from what() (no type name here to rebuild the prefix); in the
	// engine's rendered form (location block, or JSON under errors_as_json).
	const std::string &GetRawMessage() const {
		return raw_message;
	}

private:
	int code;
	std::string raw_message;
};

// Typed exceptions for callback implementors (UDF / table-function /
// replacement-scan trampolines). Throwing one names an error class; its code
// lives only in the implementation.
class InvalidInputException : public Exception {
public:
	explicit InvalidInputException(std::string message);
};

class InterruptException : public Exception {
public:
	explicit InterruptException(std::string message);
};

//----------------------------------------------------------------------------------------------------------------------
// Database Option
//----------------------------------------------------------------------------------------------------------------------

// Who may write an option. Mirrors DUCKDB_V2_OPTION_TARGET_SCOPE numerically
// (parity pinned in the .cpp). Unknown covers options whose declaration
// carries no explicit scope target, and options created via the constructor
// that have not yet been resolved through a database get.
enum class OptionTargetScope : uint8_t {
	/* Target scope is not known. */
	Unknown = 0,
	/* May only be written at GLOBAL (database) scope. */
	GlobalOnly = 1,
	/* May only be written at LOCAL (session) scope. */
	LocalOnly = 2,
	/* May be written at either scope; defaults to GLOBAL when unspecified. */
	GlobalDefault = 3,
	/* May be written at either scope; defaults to LOCAL when unspecified. */
	LocalDefault = 4,
};

// Scope of a connection-level option write. Mirrors DUCKDB_V2_SETTING_SCOPE
// numerically (parity pinned in the .cpp). Automatic resolves from the
// option's declared target scope, exactly like SQL `SET name = value`.
enum class SettingScope : uint8_t {
	Automatic = 0,
	Global = 1,
	Local = 2,
};

class DatabaseOption final : public detail::Handle<DatabaseOption> {
	friend detail::Factory;

public:
	DatabaseOption(const std::string &name, const std::string &value);

	DatabaseOption(DatabaseOption &&) noexcept = default;
	DatabaseOption &operator=(DatabaseOption &&) noexcept = default;

	std::string_view GetName() const;
	std::string_view GetValue() const;
	std::string_view GetDefaultValue() const;
	std::string_view GetDescription() const;

	OptionTargetScope GetTargetScope() const;

	size_t GetAliasCount() const;
	std::string_view GetAliasByIndex(size_t index) const;

	~DatabaseOption() override;

private:
	explicit DatabaseOption(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// File Handle
//----------------------------------------------------------------------------------------------------------------------

class FileHandle final : public detail::Handle<FileHandle> {
	friend detail::Factory;

public:
	~FileHandle() override;

	void Sync();
	void Close();

	void Seek(int64_t position);
	auto Tell() const -> int64_t;
	auto Size() const -> int64_t;
	auto Read(void *buffer, int64_t size) -> int64_t;
	auto Write(const void *buffer, int64_t size) -> int64_t;

private:
	explicit FileHandle(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// File System
//----------------------------------------------------------------------------------------------------------------------

enum class FileFlags : uint8_t {
	INVALID = 0,
	/* Open the file with "read" capabilities. */
	READ = 1,
	/* Open the file with "write" capabilities. */
	WRITE = 2,
	/* Create a new file, or open if it already exists. */
	FILE_CREATE = 4,
	/* Create a new file, or fail if it already exists. The FILE_ prefix matches
	   the engine's flag names and keeps CREATE_NEW clear of the <windows.h> macro. */
	FILE_CREATE_NEW = 8,
	/* Open the file in "append" mode. */
	APPEND = 16,
};

constexpr FileFlags operator|(FileFlags a, FileFlags b) {
	return static_cast<FileFlags>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}

constexpr FileFlags &operator|=(FileFlags &a, FileFlags b) {
	a = static_cast<FileFlags>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
	return a;
}

constexpr FileFlags operator&(FileFlags a, FileFlags b) {
	return static_cast<FileFlags>(static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}

constexpr FileFlags &operator&=(FileFlags &a, FileFlags b) {
	a = static_cast<FileFlags>(static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
	return a;
}

class FileSystem final : public detail::Handle<FileSystem> {
	friend detail::Factory;

public:
	~FileSystem() override;
	FileHandle OpenFile(const std::string &path, FileFlags flags) const;

private:
	explicit FileSystem(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Logging
//----------------------------------------------------------------------------------------------------------------------

enum class LogLevel : uint8_t {
	/* Trace-level message, typically very verbose and intended for debugging. */
	LOG_TRACE = 10,
	/* Debug-level message, useful for diagnosing issues but less verbose than trace. */
	LOG_DEBUG = 20,
	/* Informational message, indicating normal operation or significant events. */
	LOG_INFO = 30,
	/* Warning message, indicating a potential issue or something that may require attention. */
	LOG_WARN = 40,
	/* Error message, indicating a failure or problem that occurred. */
	LOG_ERROR = 50,
	/* Fatal message, indicating a critical failure that may cause the process to terminate. */
	LOG_FATAL = 60,
};

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

class LogicalType;
struct TypeParam;
struct NamedParam;

class Context final : public detail::Handle<Context> {
	friend detail::Factory;

public:
	~Context() override;

	FileSystem GetFileSystem() const;

	// Parses a SQL type expression into an owned logical type: primitives,
	// parameterized kinds, and catalog-registered names alike. Usable
	// wherever a Context is live (function bind callbacks). From outside,
	// Connection::ParseType resolves the same directly from the connection.
	auto ParseType(std::string_view text) const -> LogicalType;

	// Builds a logical type from a catalog type name plus value parameters,
	// mirroring how SQL binds a type expression; registered extension types
	// construct through the same call. A TypeParam with an empty name is
	// positional. Connection::CreateType is the sugar.
	auto CreateType(const std::string &name, const std::vector<TypeParam> &params) const -> LogicalType;

	// Log a message from this connection. This is infallible and will not throw exceptions.
	void Log(LogLevel level, const std::string &message) const noexcept;

private:
	explicit Context(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// SQL statements
//----------------------------------------------------------------------------------------------------------------------

class ColumnDataCollection;

// An owned, parsed SQL statement, yielded by StatementIterator::Next and
// consumed by Connection::Query.
class SqlStatement final : public detail::Handle<SqlStatement> {
	friend detail::Factory;

public:
	SqlStatement(SqlStatement &&) noexcept = default;
	SqlStatement &operator=(SqlStatement &&) noexcept = default;

	~SqlStatement() override;

	// Binds `collection` into this statement as a table named `name`, injected
	// as a CTE, so the statement can read it once bound, prepared, or executed;
	// each run scans whatever the collection holds at that moment. Works for
	// SELECT, INSERT, UPDATE, DELETE, and MERGE INTO. `name` must be non-empty
	// and not already used by a CTE on the statement.
	//
	// The collection's columns bind as col1..colN by default. Pass `column_names`
	// to name them instead, so the statement can reference them directly
	// (FROM buf) rather than by the positional convention; when given, it must
	// name every column.
	//
	// The collection is borrowed. Keep it alive while this statement, any
	// prepared statement made from it, and any result executed from it are live,
	// and do not Reset or destroy it while such a result is live: the scan reads
	// its buffers directly.
	void AddCollection(std::string_view name, const ColumnDataCollection &collection,
	                   const std::vector<std::string> &column_names = {});

private:
	explicit SqlStatement(void *impl);
};

// An owned iterator over the statements of a SQL string, produced by
// Connection::ParseSQL. Parsing only: no binding, no catalog access, no
// transaction. Statements already yielded outlive the iterator.
class StatementIterator final : public detail::Handle<StatementIterator> {
	friend detail::Factory;

public:
	StatementIterator(StatementIterator &&) noexcept = default;
	StatementIterator &operator=(StatementIterator &&) noexcept = default;

	~StatementIterator() override;

	// Yields the next owned statement; empty when the iterator is
	// exhausted (idempotently).
	auto Next() -> SqlStatement;

private:
	explicit StatementIterator(void *impl);
};

class QueryResult;
class Value;

// A statement bound and planned once, executable repeatedly. Produced by
// Connection::Prepare. Unlike Connection::Execute (stateless, re-binds each call)
// it MAY reuse its compiled plan across executions (see ReusesPlan). Execution
// returns the same QueryResult, with identical streaming / draining behaviour.
class PreparedStatement final : public detail::Handle<PreparedStatement> {
	friend detail::Factory;

public:
	PreparedStatement(PreparedStatement &&) noexcept = default;
	PreparedStatement &operator=(PreparedStatement &&) noexcept = default;

	~PreparedStatement() override;

	// Executes with positional parameters ($1 = parameters[0]), returning a lazy
	// streaming result. Non-consuming, re-executable. Throws RESOURCE_IN_USE while a
	// live result exists on the connection.
	QueryResult Execute(const Value *parameters, idx_t parameter_count);
	// No-parameter convenience.
	QueryResult Execute();
	// std::vector convenience (defined inline below, once Value and QueryResult are
	// complete, to keep std::vector off the compiled boundary).
	QueryResult Execute(const std::vector<Value> &parameters);
	// Named-parameter convenience: each NamedParam binds its value to the named
	// parameter, or positionally when its name is empty.
	QueryResult Execute(const std::vector<NamedParam> &parameters);

	// True if it reuses its compiled plan across executions, false if it re-binds
	// each time (no faster than Connection::Execute). A static property of the plan.
	bool ReusesPlan() const;

private:
	explicit PreparedStatement(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------------------------------------------------

class QueryResult;
class PreparedStatement;
struct Signature;
class Schema;
class Value;
class QualifiedName;
class TableDescription;

class Connection final : public detail::Handle<Connection> {
	friend detail::Factory;

public:
	Connection(Connection &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
	}

	Connection &operator=(Connection &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
		return *this;
	}

	// Snapshot of the active query's execution progress. The engine
	// publishes progress only when enable_progress_bar is set; a
	// percentage of -1 (with both row counts 0) means no information is
	// available.
	struct QueryProgress {
		double percentage;
		uint64_t rows_processed;
		uint64_t total_rows_to_process;
	};

	~Connection() override;

	size_t GetOptionCount() const;
	DatabaseOption GetOptionByIndex(size_t index) const;
	DatabaseOption GetOption(std::string_view name) const;
	void SetOption(const DatabaseOption &option);
	void SetOption(const DatabaseOption &option, SettingScope scope);

	// Parses a SQL string into an iterator over its statements. Parsing
	// only: no binding, no catalog access, no transaction.
	StatementIterator ParseSQL(const char *sql);
	// Inline forwarder: keeps std::string out of the compiled interface.
	StatementIterator ParseSQL(const std::string &sql) {
		return ParseSQL(sql.c_str());
	}

	// Executes a parsed statement, returning a lazy streaming result; nothing runs
	// until the result is stepped. Borrowed (executed via a copy), not consumed, so
	// re-executable. parameters bind positionally ($1 = parameters[0]). Throws
	// RESOURCE_IN_USE while a live result exists.
	QueryResult Execute(const SqlStatement &statement, const Value *parameters, idx_t parameter_count);
	// No-parameter convenience.
	QueryResult Execute(const SqlStatement &statement);
	// std::vector convenience (defined inline below, once Value is complete, to keep
	// std::vector off the compiled boundary).
	QueryResult Execute(const SqlStatement &statement, const std::vector<Value> &parameters);
	// Named-parameter convenience: each NamedParam binds its value to the named
	// parameter, or positionally when its name is empty.
	QueryResult Execute(const SqlStatement &statement, const std::vector<NamedParam> &parameters);

	// Single-statement SQL convenience over ParseSQL + Execute: throws INVALID_INPUT
	// unless the input contains exactly one statement.
	QueryResult Execute(const std::string &sql);

	// Binds a parsed statement without executing, returning its signature (output
	// schema of result columns, input schema of parameter types). Borrowed, not
	// consumed.
	Signature Bind(const SqlStatement &statement) const;

	// Prepares a parsed statement into a reusable cached-execution handle, executed
	// repeatedly via PreparedStatement::Execute. Borrowed (AST copied), not consumed.
	// When require_cacheable is set, throws INVALID_INPUT unless the plan will be
	// reused across executions (see PreparedStatement::ReusesPlan).
	PreparedStatement Prepare(const SqlStatement &statement, bool require_cacheable = false) const;

	// Resolves a possibly partial table name through this connection's catalogs
	// and search path, exactly as the name resolves in SQL, and snapshots the
	// table's description. Throws the engine's missing-table or not-a-table
	// error when the name resolves to nothing or to a view.
	TableDescription DescribeTable(const QualifiedName &name) const;

	// Connection-level counterpart to Context::ParseType: resolves the type
	// directly from the connection (its own transaction), no context scope needed.
	auto ParseType(std::string_view text) -> LogicalType;

	// Connection-level counterpart to Context::CreateType: resolves directly
	// from the connection (its own transaction), no context scope needed.
	auto CreateType(const std::string &name, const std::vector<TypeParam> &params) -> LogicalType;

	// Requests cancellation of the active query. Safe to call from any
	// thread; a no-op when no query is active.
	void Interrupt();

	// Reads the active query's progress. Safe to call from any thread.
	QueryProgress GetQueryProgress() const;

	// Log a message from this connection. This is infallible and will not throw exceptions.
	void Log(LogLevel level, const std::string &message) const noexcept;

	static Connection FromOpaque(void *opaque) {
		return Connection(opaque, false);
	}

private:
	explicit Connection(void *impl, bool owned);
	bool owned = false; // TODO: This should be fixed C++ side
};

//----------------------------------------------------------------------------------------------------------------------
// Database
//----------------------------------------------------------------------------------------------------------------------

class Value;

class Database final : public detail::Handle<Database> {
	friend detail::Factory;

public:
	~Database() override;
	Database(Database &&) noexcept = default;
	Database &operator=(Database &&) noexcept = default;

	size_t GetOptionCount() const;
	DatabaseOption GetOptionByIndex(size_t index) const;
	// By-name get: an alias resolves to its canonical option. Throws
	// INVALID_INPUT for an unknown name.
	DatabaseOption GetOption(std::string_view name) const;
	void SetOption(const DatabaseOption &option);

	Connection Connect();

	// Passed to the callback (valid only during it): inspect the unresolved name and claim it.
	class ReplacementScanInput;
	using ReplacementScanCallback = void (*)(ReplacementScanInput &input);

	// Registers a scan consulted when a name can't be resolved: claim (SetFunctionName), decline (return), or fail
	// (throw). Registration order, first claim wins; lives until db close. Not synchronized with queries: register
	// first.
	void AddReplacementScan(ReplacementScanCallback callback);

	// As above, constructing user data of type T in place (freed at db close; read via GetUserData<T>).
	template <class T, class... ARGS>
	void AddReplacementScan(ReplacementScanCallback callback, ARGS &&... args) {
		auto *data = new T(std::forward<ARGS>(args)...);
		AddReplacementScanInternal(callback, data, detail::TypedDelete<T>);
	}

private:
	explicit Database(void *impl);
	void AddReplacementScanInternal(ReplacementScanCallback callback, void *user_data, void (*destructor)(void *));

public:
	class ReplacementScanInput {
		friend detail::Factory;

	public:
		// Unresolved reference parts; an absent catalog/schema qualifier is an empty view.
		auto GetCatalogName() const -> std::string_view;
		auto GetSchemaName() const -> std::string_view;
		auto GetTableName() const -> std::string_view;

		// Binding context (callback-duration). For filesystem probes / logging; do not run queries through it.
		auto GetContext() const -> Context;

		template <class T>
		auto GetUserData() const -> T & {
			return *static_cast<T *>(user_data);
		}

		// Claim by naming the target function (last call wins); add parameters in order. Values are copied.
		void SetFunctionName(const std::string &name);
		void AddParameter(const Value &value);
		void AddNamedParameter(const std::string &name, const Value &value);

	private:
		ReplacementScanInput(void *info, void *context, void *user_data)
		    : info(info), context(context), user_data(user_data) {
		}

		void *info;
		void *context;
		void *user_data;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Extension
//----------------------------------------------------------------------------------------------------------------------

// The extension currently being loaded: the identity under which catalog
// entries (functions, types, casts) and database-level hooks (replacement
// scans, log storages) are installed from inside DuckDB. Borrowed for the
// duration of the load and never destroyed here. Registrations through it are
// attributed to the extension. From outside a load, the same objects register
// on a Connection (catalog entries) or a Database (database-level hooks).
class Extension final : public detail::Handle<Extension> {
	friend detail::Factory;

public:
	~Extension() override;

	// Registers a replacement scan on the loading extension's database. Same
	// contract as Database::AddReplacementScan.
	void AddReplacementScan(Database::ReplacementScanCallback callback);

	// As above, constructing user data of type T in place (freed at db close; read via GetUserData<T>).
	template <class T, class... ARGS>
	void AddReplacementScan(Database::ReplacementScanCallback callback, ARGS &&... args) {
		auto *data = new T(std::forward<ARGS>(args)...);
		AddReplacementScanInternal(callback, data, detail::TypedDelete<T>);
	}

private:
	explicit Extension(void *impl);
	void AddReplacementScanInternal(Database::ReplacementScanCallback callback, void *user_data,
	                                void (*destructor)(void *));
};

//----------------------------------------------------------------------------------------------------------------------
// Environment
//----------------------------------------------------------------------------------------------------------------------

class Environment final : public detail::Handle<Environment> {
	friend detail::Factory;

public:
	Environment();
	~Environment() override;
	Environment(Environment &&) noexcept = default;
	Environment &operator=(Environment &&) noexcept = default;

	size_t GetOpenDatabaseCount() const;

	Database Open(const std::string &path);
	// Open with pre-open DBConfig options (access_mode, memory_limit, storage
	// options, ...). Options are borrowed; the engine copies what it needs.
	Database Open(const std::string &path, const std::vector<DatabaseOption> &options);
};

// The version string of the linked DuckDB engine.
auto LibraryVersion() -> std::string;

// Render name as a SQL identifier, quoting and escaping it only when required
// (a keyword, or a name with characters that need quoting). Mirrors the engine's
// own identifier rendering; use it instead of hand-rolling quotes when building
// SQL from a name.
auto RenderQuotedIdentifier(std::string_view name) -> std::string;

//----------------------------------------------------------------------------------------------------------------------
// Qualified Name
//----------------------------------------------------------------------------------------------------------------------

// A qualified name: an ordered path of one to three non-empty identifier parts
// whose last element is the object name. Partial qualification is expressed by
// path length, never by empty placeholder parts; interpreting a two-part name
// (schema versus catalog) belongs to resolution, not to the name.
class QualifiedName final : public detail::Handle<QualifiedName> {
	friend detail::Factory;

public:
	QualifiedName(QualifiedName &&) noexcept = default;
	QualifiedName &operator=(QualifiedName &&) noexcept = default;

	~QualifiedName() override;

	// Parses name text with the engine's rules: dots separate parts, a
	// double-quoted part may contain dots and doubled interior quotes. Prefer
	// FromParts when the parts are already separate.
	static QualifiedName Parse(std::string_view text);
	// Builds a name from parts ordered outermost first; the last part is the
	// object name. One to three parts, each non-empty.
	static QualifiedName FromParts(const std::vector<std::string_view> &parts);

	// Number of parts.
	idx_t GetPartCount() const;
	// Borrowed part at index, outermost first; valid for this QualifiedName's lifetime.
	std::string_view GetPart(idx_t index) const;

	// The dot-separated SQL text, each part quoted only when required.
	std::string Render() const;

	// Case-insensitive per part, the engine's identifier equality.
	bool operator==(const QualifiedName &other) const;
	bool operator!=(const QualifiedName &other) const {
		return !(*this == other);
	}
	// Consistent with equality; in-process use only, never persist the value.
	uint64_t Hash() const;

private:
	explicit QualifiedName(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------

// Logical type identifier. Mirrors the C API's LOGICAL_TYPE_ID (and thereby
// duckdb::LogicalTypeId) numerically; parity pinned by static_asserts in the
// implementation.
enum class TypeId : uint32_t {
	INVALID = 0,
	SQLNULL = 1,
	UNKNOWN = 2,
	ANY = 3,
	// A type carried as a value (type parameters); see Value::Type.
	TYPE = 6,
	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
	TIMESTAMP = 19,
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
	DOUBLE = 23,
	VARCHAR = 25,
	BLOB = 26,
	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	UINTEGER = 30,
	UBIGINT = 31,
	TIMESTAMP_TZ = 32,
	TIMESTAMP_TZ_NS = 33,
	TIME_TZ = 34,
	TIME_NS = 35,
	BIT = 36,
	BIGNUM = 39,
	UHUGEINT = 49,
	HUGEINT = 50,
	UUID = 54,
	GEOMETRY = 60,
	STRUCT = 100,
	LIST = 101,
	MAP = 102,
	ENUM = 104,
	UNION = 107,
	ARRAY = 108,
	VARIANT = 109,
	// Unnamed struct; shares the physical representation of STRUCT.
	TUPLE = 110,
};

class LogicalType final : public detail::Handle<LogicalType> {
	friend detail::Factory;

public:
	LogicalType(LogicalType &&) noexcept = default;
	LogicalType &operator=(LogicalType &&) noexcept = default;

	~LogicalType() override;

	static LogicalType VARCHAR();
	static LogicalType INTEGER();
	static LogicalType BIGINT();
	// The function-signature wildcard. Constructible so it can be passed to a
	// function's AddParameter (a fixed-arity wildcard) or SetVarArgs (a
	// heterogeneous variadic tail); data-creating surfaces reject it.
	static LogicalType ANY();

	LogicalType WithAlias(std::string_view alias) const;

	// The type's name: the alias when set, otherwise the canonical fixed
	// name of the type id. Never empty; exactly the vocabulary CreateType
	// consumes. Borrowed; valid until this LogicalType is destroyed.
	std::string_view GetName() const;
	bool operator==(const LogicalType &other) const;
	bool operator!=(const LogicalType &other) const {
		return !(*this == other);
	}

	auto GetId() const -> TypeId;

	// Renders as SQL text (an aliased type renders as its alias). The inverse
	// of Context::ParseType for every constructible kind.
	auto ToText() const -> std::string;

	// Generic parameter inspection: the exact dual of Context::CreateType.
	// DECIMAL 2 (width, scale); LIST 1 (element type); ARRAY 2 (element
	// type, size); MAP 2 (key, value types); STRUCT one per field; UNION one
	// per member; ENUM one per dictionary entry; VARCHAR 1 when a collation
	// is set; GEOMETRY 1 when a coordinate system is set; else 0.
	auto GetParamCount() const -> idx_t;
	// One parameter: owned name (empty = positional) plus owned value. Child
	// types come back as TYPE values (unwrap via Value::AsType).
	auto GetParam(idx_t index) const -> TypeParam;

	// Per-kind sugar over GetParam / GetParamCount. Each throws
	// INVALID_INPUT when this type is not the matching kind. Names and
	// dictionary entries return owned strings: the backing values are owned
	// per call, so views would dangle.
	auto GetDecimalWidth() const -> uint8_t;
	auto GetDecimalScale() const -> uint8_t;
	auto GetEnumSize() const -> idx_t;
	auto GetEnumValue(idx_t index) const -> std::string;
	auto GetListChildType() const -> LogicalType;
	auto GetArrayChildType() const -> LogicalType;
	auto GetArraySize() const -> idx_t;
	auto GetMapKeyType() const -> LogicalType;
	auto GetMapValueType() const -> LogicalType;
	auto GetStructChildCount() const -> idx_t;
	auto GetStructChildName(idx_t index) const -> std::string;
	auto GetStructChildType(idx_t index) const -> LogicalType;
	auto GetUnionMemberCount() const -> idx_t;
	auto GetUnionMemberName(idx_t index) const -> std::string;
	auto GetUnionMemberType(idx_t index) const -> LogicalType;

	// Storage-tier conveniences, computed locally from the committed
	// width / dictionary-size tables (see the vector module's view
	// docstring); no extra boundary crossings. Gate on the type kind like
	// the other sugars.
	auto GetDecimalInternalTypeId() const -> TypeId;
	auto GetEnumInternalTypeId() const -> TypeId;

private:
	explicit LogicalType(void *impl);

	// Shared gate for the per-kind sugar: throws INVALID_INPUT unless this
	// type's id is `expected`.
	auto RequireKind(TypeId expected, const char *what) const -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// Schema
//----------------------------------------------------------------------------------------------------------------------

// An ordered (name, type) row schema: a statement's input (parameters) or output
// (result columns), or a result's columns. Field names may repeat or be empty,
// and the list may be empty.
class Schema final : public detail::Handle<Schema> {
	friend detail::Factory;

public:
	Schema(Schema &&) noexcept = default;
	Schema &operator=(Schema &&) noexcept = default;

	~Schema() override;

	// Number of fields.
	idx_t GetFieldCount() const;
	// Borrowed field name, valid for this Schema's lifetime; empty for an absent name.
	std::string_view GetFieldName(idx_t index) const;
	// An owned copy of the field type.
	LogicalType GetFieldType(idx_t index) const;

	// ---- Arrow export ----

	// Exports the fields as an Arrow schema into caller-allocated `out`; the
	// caller owns it and frees it via out.release(&out). `context` needs an
	// active transaction (extension types and ENUM dictionaries can touch the
	// catalog).
	auto ToArrowSchema(const Context &context, ArrowSchema &out) const -> void;

private:
	explicit Schema(void *impl);
};

// A bound statement's signature: its output schema (result columns) and input
// schema (parameter types). Returned by Connection::Bind.
struct Signature {
	Schema output;
	Schema parameters;
};

//----------------------------------------------------------------------------------------------------------------------
// Table Description
//----------------------------------------------------------------------------------------------------------------------

// An owned snapshot of one base table taken at creation: where the name
// resolved, the table's columns, and per-column catalog facts. Later DDL does
// not update it. Returned by Connection::DescribeTable.
class TableDescription final : public detail::Handle<TableDescription> {
	friend detail::Factory;

public:
	TableDescription(TableDescription &&) noexcept = default;
	TableDescription &operator=(TableDescription &&) noexcept = default;

	~TableDescription() override;

	// The fully resolved name: catalog, schema, and table the lookup landed
	// on, with DDL time casing, never an echo of the requested name.
	QualifiedName GetQualifiedName() const;
	// An owned schema of every column in declared order, generated columns
	// included; the per-column getters are index-aligned with it.
	Schema GetSchema() const;
	// Whether the column at index is generated (computed, not writable).
	bool ColumnIsGenerated(idx_t index) const;
	// Whether the column at index declares a default; generated columns report false.
	bool ColumnHasDefault(idx_t index) const;
	// Whether the resolved catalog was attached read-only. False clears the
	// catalog-level check only; it does not by itself prove a write succeeds.
	bool IsReadonly() const;

private:
	explicit TableDescription(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------
// A decoded BIGNUM: big-endian magnitude bytes plus a sign flag. The integer
// is (-1)**is_negative * unsigned_big_endian(magnitude). Owned bytes.
struct DecodedBignum {
	std::vector<uint8_t> magnitude;
	bool is_negative;
};

// A decoded TIME_TZ: time-of-day microseconds plus the UTC offset in seconds
// (positive = east of UTC). Unpacks the committed packed-uint64 storage.
struct DecodedTimeTz {
	int64_t micros;
	int32_t offset_seconds;
};

// A decoded UUID: the canonical 16 big-endian bytes. The storage is an int128
// with its high bit flipped for sort order; this is the real value.
struct DecodedUuid {
	uint8_t bytes[16];
};

// Committed fixed-layout mirrors for multi-field leaf types, shared by both
// the Value payload path (Value::GetDataAs<T>) and the vector view path
// (VectorView::Data<T>). Layout pinned by static_assert in the .cpp. HugeintLayout's
// halves also carry UUID in its internal storage form.
struct IntervalLayout {
	int32_t months;
	int32_t days;
	int64_t micros;
};
struct HugeintLayout {
	uint64_t lower;
	int64_t upper;
};
struct UhugeintLayout {
	uint64_t lower;
	uint64_t upper;
};

class Value final : public detail::Handle<Value> {
	friend detail::Factory;

public:
	~Value() override;

	Value(Value &&) noexcept = default;
	Value &operator=(Value &&) noexcept = default;

	// Construct an owned value. The library copies the input.
	static Value Bigint(int64_t value);
	static Value Varchar(const std::string &value);

	// A NULL value of the given logical type (borrowed; copied in).
	static Value Null(const LogicalType &type);

	// A BIGNUM value from big-endian magnitude bytes plus a sign flag. The
	// value zero is a single 0x00 byte with is_negative false; length 0 is
	// invalid. Unwrap via AsBignum.
	static Value Bignum(const uint8_t *data, idx_t length, bool is_negative);

	// Wraps a borrowed logical type as a TYPE value (a type carried as a
	// value, e.g. a type parameter). Unwrap via AsType.
	static Value Type(const LogicalType &type);

	// Builds a composite value from a borrowed type plus borrowed children
	// (copied in, cast to the declared child/field types). LIST: elements,
	// any count; ARRAY: count = declared size; STRUCT: positional fields;
	// MAP: alternating key, value. UNION and ENUM values are built via Cast
	// (member value to union type, VARCHAR to enum type).
	static Value Create(const LogicalType &type, const std::vector<Value> &children);

	auto IsNull() const -> bool;
	auto GetLogicalType() const -> LogicalType;
	auto ToString() const -> std::string;

	// Casts through the engine's cast machinery (non-strict; registered
	// custom casts included). The primary form takes a live Context; the
	// Connection overload is sugar that runs a with-context scope.
	auto Cast(const Context &ctx, const LogicalType &target) const -> Value;
	auto Cast(Connection &conn, const LogicalType &target) const -> Value;

	// Composite descent: LIST/ARRAY/STRUCT children are elements or fields,
	// MAP children alternate key, value, UNION children are [0] the tag as a
	// UTINYINT value and [1] the active member. Primitives and NULL
	// composites report 0. Children are owned copies.
	auto GetChildCount() const -> idx_t;
	auto GetChild(idx_t index) const -> Value;

	auto AsBoolean() const -> bool;

	auto AsTinyint() const -> int8_t;
	auto AsUtinyint() const -> uint8_t;

	auto AsSmallint() const -> int16_t;
	auto AsUsmallint() const -> uint16_t;

	auto AsInteger() const -> int32_t;
	auto AsUinteger() const -> uint32_t;

	auto AsBigint() const -> int64_t;
	auto AsUbigint() const -> uint64_t;

	auto AsFloat() const -> float;
	auto AsDouble() const -> double;

	auto AsVarchar() const -> std::string_view;

	// Unwraps the logical type carried by a TYPE value; throws INVALID_INPUT
	// unless this is a non-NULL TYPE value. Joins the As* getters.
	auto AsType() const -> LogicalType;

	// The decoded magnitude bytes + sign of a BIGNUM value.
	auto AsBignum() const -> DecodedBignum;

	// Unwraps a VARIANT value into the plain value it carries, with its
	// real logical type (the in-surface inner-type discovery for VARIANT
	// cells). Throws INVALID_INPUT unless this is a non-NULL VARIANT value.
	auto UnwrapVariant() const -> Value;

	// Generic committed-layout payload access, the single-cell analog of
	// VectorView. GetData borrows the payload in exactly the committed physical
	// layout (fixed layout for fixed kinds; decoded wire bytes for VARCHAR /
	// BLOB / BIT / BIGNUM); dispatch on GetLogicalType and cast, e.g. a DECIMAL
	// reads its backing integer plus GetDecimalScale. FromData builds a value of
	// any type from that layout. Both throw INVALID_INPUT on a NULL value or a
	// length that does not match the type. This is the complete path; the As* /
	// From* below are gated conveniences over it for the everyday types.
	auto GetData() const -> std::pair<const void *, idx_t>; // borrowed, lifetime = this value
	static Value FromData(const LogicalType &type, const void *data, idx_t length);

	// Size-checked typed copy of GetData for a fixed-layout kind. T must match
	// the value's committed layout width (dispatch on GetLogicalType first).
	template <class T>
	auto GetDataAs() const -> T {
		auto raw = GetData();
		if (raw.second != sizeof(T)) {
			throw InvalidInputException("GetDataAs: payload size does not match the requested layout");
		}
		T out;
		std::memcpy(&out, raw.first, sizeof(T));
		return out;
	}

	// Gated conveniences over GetData/FromData for the everyday types. Temporal
	// payloads use the type's native unit; getters throw on a type mismatch.
	static Value Boolean(bool value);
	static Value Ubigint(uint64_t value);
	static Value Double(double value);
	static Value Blob(const void *data, idx_t length);
	static Value Date(int32_t days);
	static Value Time(int64_t micros);
	static Value Timestamp(int64_t micros);
	static Value TimestampTz(int64_t micros);
	static Value Interval(IntervalLayout value);
	static Value Hugeint(HugeintLayout value);
	static Value Uhugeint(UhugeintLayout value);

	auto AsBlob() const -> std::pair<const void *, idx_t>; // borrowed view
	auto AsDate() const -> int32_t;                        // days since epoch
	auto AsTime() const -> int64_t;                        // micros since midnight
	// raw temporal payload in the type's native unit (TIMESTAMP us, _S s,
	// _MS ms, _NS ns, TIMESTAMPTZ us)
	auto AsTimestampRaw() const -> int64_t;
	auto AsInterval() const -> IntervalLayout;
	auto AsHugeint() const -> HugeintLayout;
	auto AsUhugeint() const -> UhugeintLayout;
	// Canonical UUID bytes (the storage's high-bit flip is undone). The raw
	// internal hugeint form is still reachable via GetDataAs<HugeintLayout>.
	auto AsUuid() const -> DecodedUuid;
	// Time-of-day micros + UTC offset, unpacked from the packed TIME_TZ storage.
	auto AsTimeTz() const -> DecodedTimeTz;

	// TODO: Add more

private:
	explicit Value(void *impl);
};

// One type parameter: the unit of Context::CreateType and
// LogicalType::GetParam. An empty name means positional.
struct TypeParam {
	std::string name;
	Value value;
};

// One statement parameter binding for the named-parameter Execute overloads. An
// empty name binds positionally ($1 = the first NamedParam); a non-empty name binds
// by name (case-insensitive), matching $name.
struct NamedParam {
	std::string name;
	Value value;
};

//----------------------------------------------------------------------------------------------------------------------
// Expression
//----------------------------------------------------------------------------------------------------------------------

// The class of a bound expression node; selects which class-specific
// Expression accessors are valid. Mirrors DUCKDB_V2_EXPRESSION_CLASS /
// duckdb::ExpressionClass numerically (sentinel static_asserts in the .cpp).
// Only Bound* values appear in the trees this API exposes; parsed classes
// exist for numeric fidelity. Switch on Bound* (BoundColumnRef, not ColumnRef).
enum class ExpressionClass : uint32_t {
	Invalid = 0,
	Aggregate = 1,
	Case = 2,
	Cast = 3,
	ColumnRef = 4,
	Comparison = 5,
	Conjunction = 6,
	Constant = 7,
	Default = 8,
	Function = 9,
	Operator = 10,
	Star = 11,
	Subquery = 13,
	Window = 14,
	Parameter = 15,
	Collate = 16,
	Lambda = 17,
	PositionalReference = 18,
	Between = 19,
	LambdaRef = 20,
	Type = 21,
	BoundAggregate = 25,
	BoundCase = 26,
	BoundCast = 27,
	BoundColumnRef = 28,
	/* Legacy; comparisons are now BoundFunction. */
	LegacyBoundComparison = 29,
	BoundConjunction = 30,
	BoundConstant = 31,
	BoundDefault = 32,
	BoundFunction = 33,
	BoundOperator = 34,
	BoundParameter = 35,
	BoundRef = 36,
	BoundSubquery = 37,
	BoundWindow = 38,
	LegacyBoundBetween = 39,
	BoundUnnest = 40,
	BoundLambda = 41,
	BoundLambdaRef = 42,
	BoundExpression = 50,
	BoundExpanded = 51,
};

// The semantic operation of an expression node; distinguishes two nodes of the
// same class (e.g. CompareEqual vs CompareGreaterThan on two BoundFunction
// nodes). Mirrors DUCKDB_V2_EXPRESSION_TYPE / duckdb::ExpressionType
// numerically (sentinel static_asserts in the .cpp); only a subset arises in
// the bound trees this API exposes.
enum class ExpressionType : uint32_t {
	Invalid = 0,
	OperatorCast = 12,
	OperatorNot = 13,
	OperatorIsNull = 14,
	OperatorIsNotNull = 15,
	OperatorUnpack = 16,
	CompareEqual = 25,
	CompareNotEqual = 26,
	CompareLessThan = 27,
	CompareGreaterThan = 28,
	CompareLessThanOrEqualTo = 29,
	CompareGreaterThanOrEqualTo = 30,
	CompareIn = 35,
	CompareNotIn = 36,
	CompareDistinctFrom = 37,
	CompareBetween = 38,
	CompareNotBetween = 39,
	CompareNotDistinctFrom = 40,
	ConjunctionAnd = 50,
	ConjunctionOr = 51,
	ValueConstant = 75,
	ValueParameter = 76,
	ValueTuple = 77,
	ValueTupleAddress = 78,
	ValueNull = 79,
	ValueVector = 80,
	ValueScalar = 81,
	ValueDefault = 82,
	Aggregate = 100,
	BoundAggregate = 101,
	GroupingFunction = 102,
	WindowAggregate = 110,
	WindowFunction = 111,
	WindowRank = 120,
	WindowRankDense = 121,
	WindowNtile = 122,
	WindowPercentRank = 123,
	WindowCumeDist = 124,
	WindowRowNumber = 125,
	WindowFirstValue = 130,
	WindowLastValue = 131,
	WindowLead = 132,
	WindowLag = 133,
	WindowNthValue = 134,
	WindowFill = 135,
	Function = 140,
	BoundFunction = 141,
	CaseExpr = 150,
	OperatorNullif = 151,
	OperatorCoalesce = 152,
	ArrayExtract = 153,
	ArraySlice = 154,
	StructExtract = 155,
	ArrayConstructor = 156,
	Arrow = 157,
	OperatorTry = 158,
	Subquery = 175,
	Star = 200,
	TableStar = 201,
	Placeholder = 202,
	ColumnRef = 203,
	FunctionRef = 204,
	TableRef = 205,
	LambdaRef = 206,
	Type = 207,
	Cast = 225,
	BoundRef = 227,
	BoundColumnRef = 228,
	BoundUnnest = 229,
	Collate = 230,
	Lambda = 231,
	PositionalReference = 232,
	BoundLambdaRef = 233,
	BoundExpanded = 234,
};

// The logical column identity of a BoundColumnRef: table_index is the binding
// namespace id, column_index is relative to the producing operator's output.
struct ColumnBinding {
	idx_t table_index;
	idx_t column_index;
};

// A borrowed, read-only handle to a bound expression node in the engine's
// plan. Valid only for the duration of the callback that hands it out (e.g.
// TableFunction::PushdownInput); do not store it past the callback's return.
// Children share the parent's lifetime. Class-specific accessors throw
// INVALID_INPUT on a class mismatch.
class Expression final : public detail::Handle<Expression> {
	friend detail::Factory;

public:
	Expression(Expression &&) noexcept = default;
	Expression &operator=(Expression &&) noexcept = default;

	~Expression() override;

	// Universal: valid for every bound class.
	auto GetClass() const -> ExpressionClass;
	auto GetType() const -> ExpressionType;
	// An owned copy of the expression's result type.
	auto GetReturnType() const -> LogicalType;

	// Traversal is total over every bound class (the engine's
	// ExpressionIterator): nodes this API does not model specially still
	// expose their children. Child order follows the iterator.
	auto GetChildCount() const -> idx_t;
	// Borrowed child; throws INVALID_INPUT when index is out of range.
	auto GetChild(idx_t index) const -> Expression;

	// BoundFunction only. Borrowed, valid for this handle's lifetime. For
	// comparison operators the registered name is an internal symbol (e.g.
	// "__comparison"): dispatch on GetType(), not the name.
	auto GetFunctionName() const -> std::string_view;
	// BoundConstant only; owned.
	auto GetConstantValue() const -> Value;
	// BoundColumnRef only: the logical binding seen during binding and
	// optimization, including filter pushdown.
	auto GetColumnBinding() const -> ColumnBinding;
	// BoundRef only: the physical chunk slot assigned after physical
	// planning; pushdown-stage trees carry BoundColumnRef instead.
	auto GetReferenceIndex() const -> idx_t;

private:
	explicit Expression(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// String Heap
//----------------------------------------------------------------------------------------------------------------------

// Transparent mirror of the C ABI's duckdb_v2_string (same layout as
// duckdb::string_t): 16-byte storage for VARCHAR / BLOB / BIT / BIGNUM, inlined
// when length <= INLINE_LENGTH. A non-inlined value is valid only in a slot of
// the vector whose heap produced it. Aggregate, so it writes straight into a
// slot; layout pinned by static_assert in the .cpp.
struct StringLayout {
	static constexpr uint32_t INLINE_LENGTH = 12;
	static constexpr uint32_t PREFIX_LENGTH = 4;

	union {
		struct {
			uint32_t length;
			char prefix[PREFIX_LENGTH];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[INLINE_LENGTH];
		} inlined;
	} value;

	// Inlined token; the bytes live in the value. `len` must fit INLINE_LENGTH.
	static auto Inlined(const char *data, uint32_t len) -> StringLayout {
		assert(len <= INLINE_LENGTH);
		StringLayout storage {};
		storage.value.inlined.length = len;
		if (len > 0) {
			std::memcpy(storage.value.inlined.inlined, data, len);
		}
		return storage;
	}

	// Non-inlined token over `len` bytes at `heap_data` (from Allocate); sets the
	// prefix. `len` must exceed INLINE_LENGTH, else it would read as inlined.
	static auto FromHeapData(char *heap_data, uint32_t len) -> StringLayout {
		assert(len > INLINE_LENGTH);
		StringLayout storage {};
		storage.value.pointer.length = len;
		storage.value.pointer.ptr = heap_data;
		std::memcpy(storage.value.pointer.prefix, heap_data, PREFIX_LENGTH);
		return storage;
	}

	// length shares offset 0 across both arms, so these read either representation.
	auto IsInlined() const -> bool {
		return value.inlined.length <= INLINE_LENGTH;
	}
	auto Length() const -> uint32_t {
		return value.inlined.length;
	}
	auto Data() const -> const char * {
		return IsInlined() ? value.inlined.inlined : value.pointer.ptr;
	}
	// The bytes as one view: {Data(), Length()}.
	auto AsStringView() const -> std::string_view {
		return std::string_view(Data(), Length());
	}
	auto GetDataWritable() -> char * {
		return IsInlined() ? value.inlined.inlined : value.pointer.ptr;
	}
	// Seal a non-inlined value's prefix from its bytes (cf. string_t::Finalize).
	auto Finalize() -> void {
		if (!IsInlined()) {
			std::memcpy(value.pointer.prefix, value.pointer.ptr, PREFIX_LENGTH);
		}
	}
};

// Borrowed handle to a vector's string heap. Reserves vector-lifetime bytes and
// returns StringLayout tokens to place in any order (dedup, scatter). Borrowed;
// invalid across a flatten or reallocation of the owning vector.
class StringHeap final : public detail::Handle<StringHeap> {
	friend detail::Factory;

public:
	StringHeap(StringHeap &&) noexcept = default;
	StringHeap &operator=(StringHeap &&) noexcept = default;

	~StringHeap() override;

	// Reserves `byte_len` vector-lifetime bytes; raw arena allocation, no gating.
	// Write-in-place: Allocate -> write -> FromHeapData -> Vector::SetString.
	auto Allocate(idx_t byte_len) -> uint8_t *;

	// Interns `data`, returning the token. <= INLINE_LENGTH builds inline (no
	// allocation, no boundary crossing); larger allocates and copies. Throws if
	// `data` exceeds the uint32 length a duckdb_v2_string can hold.
	auto Add(std::string_view data) -> StringLayout {
		if (data.size() > UINT32_MAX) {
			ThrowStringTooLong(data.size());
		}
		if (data.size() <= StringLayout::INLINE_LENGTH) {
			return StringLayout::Inlined(data.data(), static_cast<uint32_t>(data.size()));
		}
		auto len = static_cast<uint32_t>(data.size());
		auto *bytes = Allocate(len);
		std::memcpy(bytes, data.data(), len);
		return StringLayout::FromHeapData(reinterpret_cast<char *>(bytes), len);
	}

	// Bulk Add: interns every view, returning the tokens in order.
	auto AddMany(const std::vector<std::string_view> &data) -> std::vector<StringLayout> {
		std::vector<StringLayout> out;
		out.reserve(data.size());
		for (const auto &view : data) {
			out.push_back(Add(view));
		}
		return out;
	}

private:
	explicit StringHeap(void *impl);

	// Throws OUT_OF_RANGE when an interned value exceeds the uint32 length bound.
	[[noreturn]] static void ThrowStringTooLong(idx_t size);
};

//----------------------------------------------------------------------------------------------------------------------
// Vector
//----------------------------------------------------------------------------------------------------------------------

// A vector's internal representation. Mirrors DUCKDB_V2_VECTOR_TYPE
// (static_assert in the .cpp). Other is the zero value and covers FSST /
// SEQUENCE / SHREDDED; Flatten() before reading such a vector via GetView().
enum class VectorType : uint8_t {
	Other = 0,
	Flat = 1,
	Constant = 2,
	Dictionary = 3,
};

// Read view over a vector, mirroring the C ABI's duckdb_v2_vector_view: data +
// validity + sel + count from one Vector::GetView() crossing; all per-row work
// is inline. Pointers are borrowed and valid until the owning chunk is
// destroyed. sel == nullptr means identity (FLAT); CONSTANT carries the zero
// singleton, DICTIONARY its own sel. Validity follows sel, not the loop
// counter: IsValid takes the logical index and resolves sel internally.
struct VectorView {
	const void *data;
	const uint64_t *validity;
	const uint32_t *sel; // mirrors duckdb_v2_sel_t (static_assert in the .cpp)
	idx_t count;

	template <class T>
	auto Data() const -> const T * {
		return static_cast<const T *>(data);
	}
	// Physical row index for logical index `i`: sel ? sel[i] : i.
	auto SelAt(idx_t i) const -> idx_t {
		return sel ? static_cast<idx_t>(sel[i]) : i;
	}
	// Validity at a physical (post-sel) row index; nullptr means all valid.
	auto RowIsValid(idx_t row) const -> bool {
		return !validity || (validity[row >> 6] & (uint64_t(1) << (row & 63))) != 0;
	}
	// Validity at a logical index: RowIsValid(SelAt(i)).
	auto IsValid(idx_t i) const -> bool {
		return RowIsValid(SelAt(i));
	}
};

// A decoded BIT: borrowed data bytes (lifetime = the owning vector's chunk)
// plus the count of leading bits of the first byte that are not part of the
// bit string. Bit n (0-indexed, leftmost first) is read as
// (data[(n + padding_bits) / 8] >> (7 - ((n + padding_bits) % 8))) & 1.
struct DecodedBit {
	const uint8_t *data;
	idx_t length; // data bytes
	uint8_t padding_bits;
};

// Writer over a FLAT vector's validity mask (from Vector::GetValidityMutable).
// Word W bit N covers row W*64+N; a set bit means valid (not NULL).
// Writes mark only this vector's rows. The engine requires every descendant
// slot of a NULL STRUCT / ARRAY row to be NULL as well, and SetInvalid on a
// parent row does not touch descendants: set nested rows NULL via
// Vector::SetNull. When writing nested masks raw, SetAllInvalid the child
// masks up front and SetValid slots as values are written.
struct ValidityMask {
	uint64_t *words; // public: the raw mask remains reachable

	auto SetValid(idx_t row) -> void {
		words[row >> 6] |= uint64_t(1) << (row & 63);
	}
	auto SetInvalid(idx_t row) -> void {
		words[row >> 6] &= ~(uint64_t(1) << (row & 63));
	}
	auto RowIsValid(idx_t row) const -> bool {
		return (words[row >> 6] & (uint64_t(1) << (row & 63))) != 0;
	}
	// Marks rows [0, count) invalid. Clears whole words, so trailing bits of
	// the last word beyond count read invalid too (matches the engine).
	auto SetAllInvalid(idx_t count) -> void {
		for (idx_t i = 0; i < (count + 63) / 64; i++) {
			words[i] = 0;
		}
	}
	// Marks rows [0, count) valid: the reset half of SetAllInvalid. After a fill
	// that wrote some nulls, restore all-valid to refill without the born-invalid
	// per-write SetValid. Sets whole words, so trailing bits beyond count read
	// valid too; harmless since reads stop at count.
	auto SetAllValid(idx_t count) -> void {
		for (idx_t i = 0; i < (count + 63) / 64; i++) {
			words[i] = ~uint64_t(0);
		}
	}
};

class Vector final : public detail::Handle<Vector> {
	friend detail::Factory;

public:
	Vector(Vector &&) noexcept = default;
	Vector &operator=(Vector &&) noexcept = default;

	~Vector() override;

	template <class T>
	auto GetDataMutable() -> T * {
		return static_cast<T *>(GetDataMutable());
	}
	auto GetDataMutable() -> void *;

	auto GetChildCount() const -> idx_t;
	auto GetChild(idx_t index) const -> Vector;

	auto GetLogicalType() const -> LogicalType;
	auto Flatten() const -> void;

	auto GetSize() const -> idx_t;
	auto SetSize(idx_t size) -> void;

	// ---- Vector read surface ----

	// Reads the vector as a VectorView in one boundary crossing. Throws
	// INVALID_INPUT on VectorType::Other; Flatten() first. On DICTIONARY
	// vectors the underlying child may be flattened in place, invalidating
	// pointers previously borrowed into it.
	auto GetView() const -> VectorView;

	// The internal representation kind.
	auto GetVectorType() const -> VectorType;

	// Mutable validity of a FLAT vector, lazily allocating the mask.
	// Throws INVALID_INPUT on non-FLAT vectors.
	auto GetValidityMutable() -> ValidityMask;

	// Sets a row NULL, recursively nulling descendant slots of STRUCT and
	// ARRAY rows (LIST children are exempt; consumers gate on the list's own
	// validity). The write path that maintains the engine's nested NULL
	// invariant; prefer it over raw mask writes for any nested type. Throws
	// INVALID_INPUT on non-FLAT vectors and out-of-range rows.
	auto SetNull(idx_t row) -> void;

	// Sets a CONSTANT vector's single validity bit. Throws INVALID_INPUT on
	// non-CONSTANT vectors. Setting valid true does not write a value; slot 0
	// holds whatever was last written.
	auto SetConstantValid(bool valid) -> void;

	// Turns the vector into a CONSTANT vector holding `value` for `count`
	// logical rows. The value's type must match the vector's.
	auto MakeConstant(const Value &value, idx_t count) -> void;

	// Turns the vector into the arithmetic sequence start, start+increment,
	// ... for `count` rows. Reads as VectorType::Other; Flatten() before
	// reading via GetView().
	auto MakeSequence(int64_t start, int64_t increment, idx_t count) -> void;

	// Decodes one BIT storage value (a slot of a BIT vector's data array, read
	// as StringLayout): byte 0 is the padding-bit count, bytes 1.. are the data.
	// Borrowed pointers; lifetime = the owning chunk. Inline pointer arithmetic,
	// no allocation or ABI crossing, so it inlines into a per-row vector loop.
	static auto DecodeBit(const StringLayout &value) -> DecodedBit {
		const auto len = value.Length();
		const auto *bytes = reinterpret_cast<const uint8_t *>(value.Data());
		return DecodedBit {len > 0 ? bytes + 1 : bytes, len > 0 ? len - 1 : 0, len > 0 ? bytes[0] : uint8_t(0)};
	}

	// Decodes one BIGNUM storage value into an owned magnitude + sign.
	static auto DecodeBignum(const StringLayout &value) -> DecodedBignum;

	// Unpacks one TIME_TZ slot (read as uint64) into micros + UTC offset.
	// Defined inline: a pure-arithmetic decode of the committed layout with no
	// allocation and no ABI crossing, so it inlines into a per-row vector loop.
	static auto DecodeTimeTz(uint64_t packed) -> DecodedTimeTz {
		constexpr uint64_t OFFSET_MASK = ~uint64_t(0) >> 40;
		constexpr int32_t MAX_OFFSET = 16 * 60 * 60 - 1;
		return DecodedTimeTz {static_cast<int64_t>(packed >> 24),
		                      MAX_OFFSET - static_cast<int32_t>(packed & OFFSET_MASK)};
	}

	// Undoes the sort-order high-bit flip on one UUID slot (read as
	// HugeintLayout) and returns the canonical 16 big-endian bytes. Inline for
	// the same reason as DecodeTimeTz.
	static auto DecodeUuid(HugeintLayout internal) -> DecodedUuid {
		const uint64_t upper = static_cast<uint64_t>(internal.upper) ^ (uint64_t(1) << 63);
		DecodedUuid out {};
		for (int i = 0; i < 8; i++) {
			out.bytes[i] = static_cast<uint8_t>((upper >> (56 - 8 * i)) & 0xFF);
			out.bytes[8 + i] = static_cast<uint8_t>((internal.lower >> (56 - 8 * i)) & 0xFF);
		}
		return out;
	}

	// ---- End vector read surface ----

	// --- Single-cell value bridge (owned by the types-values worktree) ---
	// Total fallback reader: any representation (constant, dictionary,
	// compressed) without flattening, any type kind including those without
	// a committed view layout (VARIANT, GEOMETRY). One owned Value per call;
	// not for per-row loops.
	auto GetValue(idx_t row) const -> Value;
	// Total fallback writer over every type kind; FLAT vectors only
	// (flatten first). The value is copied in and cast to the vector's
	// type. One engine value write per call; not for per-row loops.
	auto SetValue(idx_t row, const Value &value) -> void;
	// --- end single-cell value bridge ---

	// Copies `data` into the vector's string heap and places the resulting
	// storage value into slot `index`. The vector must be a string-backed kind
	// (VARCHAR / BLOB / BIT / BIGNUM); FLAT accepts any in-bounds index, CONSTANT
	// only index 0. Resolves the heap per call, so flattening between calls is safe.
	auto AssignString(idx_t index, std::string_view data) -> void;

	// Bulk form of AssignString: copies each view in `data` into the heap and
	// places the results into consecutive slots starting at `start`. Resolves the
	// heap once and writes straight into the data array in one pass, amortizing
	// the per-value boundary crossing. CONSTANT requires start 0 and one value.
	auto AssignStrings(idx_t start, const std::vector<std::string_view> &data) -> void;

	// Borrows this vector's string heap to intern strings whose placement is
	// decided separately (dedup, scatter, reorder). For simple in-order fills
	// prefer AssignString / AssignStrings. The vector must be a string-backed
	// kind (VARCHAR / BLOB / BIT / BIGNUM).
	auto GetStringHeap() -> StringHeap;

	// Places an interned storage token into slot `index`. The token must come
	// from this vector's heap (a non-inlined token from another vector dangles).
	auto SetString(idx_t index, StringLayout value) -> void;

private:
	explicit Vector(void *impl);

	// Throws INVALID_INPUT if [start, start+count) is not writable: a CONSTANT
	// vector's data array holds a single slot, so only index 0 may be written.
	auto CheckWriteRange(idx_t start, idx_t count) const -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// DataChunk
//----------------------------------------------------------------------------------------------------------------------
class DataChunk final : public detail::Handle<DataChunk> {
	friend detail::Factory;

public:
	explicit DataChunk(const std::vector<LogicalType> &types);

	DataChunk(DataChunk &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
	}
	DataChunk &operator=(DataChunk &&other) noexcept {
		std::swap(impl, other.impl);
		std::swap(owned, other.owned);
		return *this;
	}

	~DataChunk() override;

	auto GetVectorCount() const -> idx_t;
	auto GetRowCount() const -> idx_t;
	auto GetVector(idx_t index) const -> Vector;

	// ---- Arrow export ----

	// Exports this chunk as an Arrow array into caller-allocated `out`; the
	// caller owns it and frees it via out.release(&out). The chunk is not
	// consumed.
	auto ToArrowArray(const Context &context, ArrowArray &out) const -> void;

private:
	explicit DataChunk(void *impl, bool owned);
	bool owned = false; // UGLY, this should probably be done c++-side.
};

//----------------------------------------------------------------------------------------------------------------------
// Arrow Conversion Plan
//----------------------------------------------------------------------------------------------------------------------

// An owned handle to the bound DuckDB-side interpretation of an ArrowSchema:
// the column logical types plus the per-column Arrow type info needed to
// import arrays. Built once (e.g. at table-function bind time) and reused
// across many arrays via Convert. The context is a per-call parameter, not
// captured at construction: a plan built under a bind-time context is used
// at exec time under a different one.
class ArrowConversionPlan final : public detail::Handle<ArrowConversionPlan> {
public:
	// Resolves `schema` against `context` (extension types included).
	// `schema` is read, not consumed; the caller retains ownership of it.
	ArrowConversionPlan(const Context &context, ArrowSchema &schema);

	ArrowConversionPlan(ArrowConversionPlan &&) noexcept = default;
	ArrowConversionPlan &operator=(ArrowConversionPlan &&) noexcept = default;

	~ArrowConversionPlan() override;

	// Converts `array` into an owned DataChunk sized to the array length
	// (which may exceed the pipeline chunk size). The chunk adopts the
	// array's buffers zero-copy and `array.release` is set to NULL, so the
	// caller must not release `array` afterward.
	auto Convert(const Context &context, ArrowArray &array) const -> DataChunk;

	// The resolved fields as an owned Schema: the DuckDB-side (name, type)
	// pairs of the source ArrowSchema (e.g. to declare a table function's
	// result columns).
	auto GetSchema() const -> Schema;
};

class ColumnDataCollection final : public detail::Handle<ColumnDataCollection> {
	friend detail::Factory;

public:
	ColumnDataCollection(const Context &context, const std::vector<LogicalType> &types);
	ColumnDataCollection(const Connection &conn, const std::vector<LogicalType> &types);

	ColumnDataCollection(ColumnDataCollection &&other) noexcept = default;
	ColumnDataCollection &operator=(ColumnDataCollection &&other) noexcept = default;

	~ColumnDataCollection() override;

	// Get the number of rows currently stored in the collection.
	auto GetRowCount() const -> idx_t;

	// Merge the other collection into this one, destroying it in the process.
	auto Combine(ColumnDataCollection other) -> void;

	// Drops all buffered rows and frees their memory, keeping the column types so
	// the collection can be filled again from scratch. Any append or scan state
	// you are already holding refers to the freed memory and must be recreated
	// before reuse. Do not call this while a result is executing over the
	// collection.
	auto Reset() -> void;

	// Perform a single-threaded scan
	class ScanState;
	auto GetSingleScanState() -> ScanState;

	// Returns true if the scan produced a chunk of data, false if the scan is finished and no more data is available.
	auto Scan(ScanState &state, DataChunk &chunk) -> bool;

	// Perform a multi-threaded scan
	class SharedScanState;
	auto GetSharedScanState() -> SharedScanState;
	class WorkerScanState;
	auto GetWorkerScanState() -> WorkerScanState;

	// Returns true if the scan produced a chunk of data, false if the scan is finished and no more data is available.
	auto Scan(SharedScanState &shared_state, WorkerScanState &worker_state, DataChunk &chunk) -> bool;

	// Append data
	class AppendState;
	auto GetAppendState() -> AppendState;

	auto Append(AppendState &state, const DataChunk &chunk) -> void;

private:
	explicit ColumnDataCollection(void *impl);

public:
	class AppendState final : public detail::Handle<AppendState> {
		friend detail::Factory;

	public:
		AppendState(AppendState &&other) noexcept = default;
		AppendState &operator=(AppendState &&other) noexcept = default;
		~AppendState() override;

	private:
		explicit AppendState(void *impl);
	};

	class ScanState final : public detail::Handle<ScanState> {
		friend detail::Factory;

	public:
		ScanState(ScanState &&other) noexcept = default;
		ScanState &operator=(ScanState &&other) noexcept = default;
		~ScanState() override;

	private:
		explicit ScanState(void *impl);
	};

	class SharedScanState final : public detail::Handle<SharedScanState> {
		friend detail::Factory;

	public:
		SharedScanState(SharedScanState &&other) noexcept = default;
		SharedScanState &operator=(SharedScanState &&other) noexcept = default;
		~SharedScanState() override;

	private:
		explicit SharedScanState(void *impl);
	};

	class WorkerScanState final : public detail::Handle<WorkerScanState> {
		friend detail::Factory;

	public:
		WorkerScanState(WorkerScanState &&other) noexcept = default;
		WorkerScanState &operator=(WorkerScanState &&other) noexcept = default;
		~WorkerScanState() override;

	private:
		explicit WorkerScanState(void *impl);
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Appender
//----------------------------------------------------------------------------------------------------------------------

// A batch appender: it buffers data chunks in a fixed set of column types and
// pushes them to their destination when you Flush, by running one statement that
// reads the buffer as a table (via SqlStatement::AddCollection). It is not a
// handle, just a convenience built from the wrappers above. It holds no
// connection between calls: it borrows one only while constructing (to set the
// buffer up) and while flushing (you pass one to Flush, and it can be a
// different connection to the same database). Destroying the appender frees the
// buffer but does not flush, so Flush before you drop it to keep the rows.
//
// Data goes in a chunk at a time: build a DataChunk over ColumnTypes(), fill its
// vectors, and AppendChunk it. Producing a chunk from rows is the caller's job;
// this surface has no row-at-a-time protocol.
//
// There are two ways to make one. The query constructor is the general form:
// you bring the statement and the buffer's column types. The table-name
// constructor is the common case: it looks the table up, skips generated
// columns, and writes the INSERT for you, then hands off to the same code.
class Appender {
public:
	// The general form: buffer rows in `column_types`, then Flush by running
	// `query`, which reads the buffer as a table named `data_name` ("appended_data"
	// by default). `query` is one SELECT, INSERT, UPDATE, DELETE, or MERGE INTO.
	// The buffer's columns are col1..colN unless you name them with
	// `column_names`. `column_types` is moved in. No catalog is consulted here:
	// casts, constraints, and defaults are left to the statement at flush time.
	Appender(Connection &con, std::string_view query, std::vector<LogicalType> column_types,
	         std::string_view data_name = "appended_data", const std::vector<std::string> &column_names = {});

	// The common case: look `name` up the way SQL would (search path included for
	// a partial name) and buffer the table's non-generated columns; the engine
	// fills generated columns at flush. The INSERT is written against the
	// resolved name, so it keeps targeting that table even if the search path
	// changes later. A read-only catalog is refused here, a view by the lookup.
	Appender(Connection &con, const QualifiedName &name);

	Appender(const Appender &) = delete;
	Appender &operator=(const Appender &) = delete;
	Appender(Appender &&other) noexcept : state(other.state) {
		other.state = nullptr;
	}
	// Move assignment swaps the two buffers; the one being overwritten goes to
	// `other` and is discarded (not flushed) when `other` is destroyed.
	Appender &operator=(Appender &&other) noexcept {
		std::swap(state, other.state);
		return *this;
	}

	// Frees the buffer. Does not flush: any rows not yet flushed are dropped.
	~Appender();

	// The buffer's column types, in order. Build a chunk over these to fill and
	// AppendChunk, e.g. DataChunk chunk(ctx, appender.ColumnTypes()). Valid for
	// the appender's lifetime.
	const std::vector<LogicalType> &ColumnTypes() const;

	// Buffers a whole chunk. Its column types must equal ColumnTypes() exactly;
	// a mismatch is refused before anything is copied. Complex-typed vectors may
	// be flattened in place by the buffering.
	void AppendChunk(DataChunk &chunk);

	// Runs the statement over the buffered chunks on `con`, drains it, and empties
	// the buffer so it can be filled again. `con` need not be the connection the
	// appender was built with, but it must reach the same database. Does nothing
	// if the buffer is empty. If `con` is busy with another live result, or the
	// run is interrupted, the rows are kept so you can retry; any other failure
	// drops them.
	void Flush(Connection &con);
	// Empties the buffer without running anything. Also clears the broken state
	// left by a failed buffer operation.
	void Clear();

private:
	// All state lives behind this pointer in the implementation file.
	void *state = nullptr;
};

//----------------------------------------------------------------------------------------------------------------------
// Arrow Stream
//----------------------------------------------------------------------------------------------------------------------

// An owning, move-only handle to an Arrow C Data Interface stream
// (ArrowArrayStream). Produced by QueryResult::ToArrowStream. On destruction it
// releases the underlying stream (its transaction and the connection's query
// slot). The arrays produced by Next() are owned by the caller and released
// independently of this wrapper.
//
// Unlike the other wrappers this does not derive from detail::Handle<T>: it
// owns a raw Arrow C struct (ArrowArrayStream) rather than an opaque DuckDB C
// handle, so the Handle storage/release machinery does not apply.
class ArrowStream final {
	friend detail::Factory;

public:
	ArrowStream(ArrowStream &&other) noexcept : stream(other.stream) {
		other.stream = nullptr;
	}
	ArrowStream &operator=(ArrowStream &&other) noexcept {
		std::swap(stream, other.stream);
		return *this;
	}
	ArrowStream(const ArrowStream &) = delete;
	ArrowStream &operator=(const ArrowStream &) = delete;

	~ArrowStream();

	// True while this wrapper holds a live stream (not moved-from / released).
	explicit operator bool() const noexcept {
		return stream != nullptr;
	}

	// Borrows the underlying Arrow C stream; the wrapper retains ownership.
	// Hand its address to an Arrow consumer that does not take ownership.
	auto get() const noexcept -> ArrowArrayStream * {
		return stream;
	}

	// Detaches the underlying Arrow C stream, transferring ownership (and the
	// duty to call its release) to the caller. The wrapper is left empty.
	auto Detach() noexcept -> ArrowArrayStream * {
		auto detached = stream;
		stream = nullptr;
		return detached;
	}

	// Reads the stream schema into `out`; the caller owns it (release via
	// out.release). Throws on failure.
	void GetSchema(ArrowSchema &out) const;

	// Fetches the next array into `out`. Returns false at end of stream (where
	// `out` is left released). Each produced array is owned by the caller.
	// Throws on failure.
	bool Next(ArrowArray &out) const;

private:
	explicit ArrowStream(ArrowArrayStream *stream) : stream(stream) {
	}
	ArrowArrayStream *stream = nullptr;
};

//----------------------------------------------------------------------------------------------------------------------
// Result
//----------------------------------------------------------------------------------------------------------------------
class QueryResult final : public detail::Handle<QueryResult> {
	friend detail::Factory;

public:
	// Mirrors DUCKDB_V2_RESULT_STEP_STATUS, including WAITING as the
	// zero value.
	enum class StepStatus : uint8_t {
		WAITING = 0,
		CHUNK = 1,
		FINISHED = 2,
		CANCELLED = 3,
	};

	// Outcome of one step: the status, plus the produced chunk. The chunk
	// is non-empty iff the status is CHUNK.
	struct StepResult {
		StepStatus status;
		DataChunk chunk;
	};

	// Shape of the result. Mirrors DUCKDB_V2_RESULT_TYPE numerically
	// (parity pinned in the .cpp).
	enum class ResultType : uint8_t {
		/* Produces rows (SELECT, RETURNING, EXPLAIN). */
		QUERY_RESULT = 0,
		/* Carries an affected-row count (INSERT/UPDATE/DELETE without RETURNING). */
		CHANGED_ROWS = 1,
		/* No row output (DDL and other statements). */
		NOTHING = 2,
	};

	// The SQL statement type that produced the result. Mirrors
	// DUCKDB_V2_STATEMENT_TYPE / duckdb::StatementType numerically (parity
	// pinned in the .cpp).
	enum class StatementType : uint8_t {
		INVALID = 0,
		SELECT = 1,
		INSERT = 2,
		UPDATE = 3,
		CREATE = 4,
		DELETE = 5,
		PREPARE = 6,
		EXECUTE = 7,
		ALTER = 8,
		TRANSACTION = 9,
		COPY = 10,
		ANALYZE = 11,
		VARIABLE_SET = 12,
		CREATE_FUNC = 13,
		EXPLAIN = 14,
		DROP = 15,
		EXPORT = 16,
		PRAGMA = 17,
		VACUUM = 18,
		CALL = 19,
		SET = 20,
		LOAD = 21,
		RELATION = 22,
		EXTENSION = 23,
		LOGICAL_PLAN = 24,
		ATTACH = 25,
		DETACH = 26,
		MULTI = 27,
		COPY_DATABASE = 28,
		UPDATE_EXTENSIONS = 29,
		MERGE_INTO = 30,
	};

	QueryResult(QueryResult &&) noexcept = default;
	QueryResult &operator=(QueryResult &&) noexcept = default;

	~QueryResult() override;

	// The result's output schema (its column names and types) as one owned Schema.
	auto GetSchema() const -> Schema;

	// The shape of the result: prepare-time metadata, so callers decide
	// between consuming rows and draining without inspecting the SQL.
	auto GetResultType() const -> ResultType;

	// The SQL statement type that produced the result (prepare-time metadata).
	auto GetStatementType() const -> StatementType;

	// The streaming primitive, built for async runtimes: runs a bounded
	// unit of execution work and returns without blocking. Execution
	// errors throw; FINISHED / CANCELLED are sticky statuses.
	auto Step() -> StepResult;

	// Blocks until Step can make progress; a no-op on a terminal
	// result.
	auto Wait() -> void;

	// Fetches the next chunk of the streaming result, blocking until it is
	// available. Empty at end-of-stream (idempotently). Cancellation
	// throws RUNTIME_INTERRUPT.
	auto FetchChunk() -> DataChunk;

	// Runs the result to completion, applying all side effects and
	// discarding rows; returns the affected row count for CHANGED_ROWS
	// results, 0 otherwise. Cancellation throws RUNTIME_INTERRUPT.
	auto Drain() -> idx_t;

	// Exports this result as an owning Arrow stream, consuming the result:
	// the wrapper is left empty afterward. batch_size is the target rows per
	// Arrow array (0 selects the engine default). The stream is lazy; its
	// schema is fixed at this call.
	auto ToArrowStream(idx_t batch_size = 0) -> ArrowStream;

	// Renders the result as the engine's box table (the CLI renderer),
	// consuming this result; the remainder materializes in memory first.
	// Zero selects the renderer default per sizing knob; an empty null_value
	// renders NULL cells as "NULL". render_mode: 0 rows, 1 columns. limit is
	// the query-side LIMIT the caller applied (0 for none): when the result
	// fills it the footer renders "? rows" rather than an exact count.
	auto RenderBox(idx_t max_rows = 0, idx_t max_width = 0, idx_t max_col_width = 0, const std::string &null_value = "",
	               idx_t render_mode = 0, idx_t limit = 0) -> std::string;

private:
	explicit QueryResult(void *impl);
};

// Defined here (not in-class) now that Value and QueryResult are complete: the
// inline std::vector forwarder keeps std::vector off the compiled boundary.
inline QueryResult Connection::Execute(const SqlStatement &statement, const std::vector<Value> &parameters) {
	return Execute(statement, parameters.data(), parameters.size());
}

inline QueryResult PreparedStatement::Execute(const std::vector<Value> &parameters) {
	return Execute(parameters.data(), parameters.size());
}

//----------------------------------------------------------------------------------------------------------------------
// Log Storage
//----------------------------------------------------------------------------------------------------------------------

class LogStorage final : public detail::Handle<LogStorage> {
public:
	LogStorage();

	~LogStorage() override;

	class LogEntry;
	using LogCallback = void (*)(LogEntry &entry);

	template <class T, class... ARGS>
	void SetUserData(ARGS &&... args) {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
	}

	auto SetLogCallback(LogCallback cb) & -> LogStorage &;
	auto SetName(const std::string &name) & -> LogStorage &;
	auto Register(const Extension &extension) -> void;
	auto Register(const Database &db) -> void;

public:
	class LogEntry {
	public:
		class Inner;

		auto GetLogTimestamp() const -> int64_t;
		auto GetLogLevel() const -> LogLevel;
		auto GetLogMessage() const -> const char *; // TODO: return some sort of string view or something here
		auto GetLogType() const -> const char *;    // TODO: return some sort of string view or something here

		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserData();
			return *static_cast<T *>(ptr);
		}
		auto GetUserData() const -> void *;

		explicit LogEntry(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

private:
	LogCallback callback = nullptr;
	detail::UserData user_data;

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// Function Properties
//----------------------------------------------------------------------------------------------------------------------

// Properties shared by every function category, mirroring the engine's base
// FunctionProperties. Category-specific properties (e.g. aggregate order/distinct
// dependence) are declared as nested enums on the relevant function class.

// How stable/deterministic a function's result is, used by the optimizer.
enum class FunctionStability : uint8_t {
	/* Always returns the same result for the same input. */
	Consistent = 0,
	/* The result may differ per row (e.g. random()). */
	Volatile = 1,
	/* Stable within a single query/transaction but may change across queries (e.g. now()). */
	ConsistentWithinQuery = 2,
};

// Whether a function handles NULL inputs itself.
enum class FunctionNullHandling : uint8_t {
	/* If any argument is NULL the result is NULL and the function is not invoked for that row. */
	Default = 0,
	/* The function is invoked even when arguments are NULL and decides the result itself. */
	Special = 1,
};

// Whether a function can raise a runtime error.
enum class FunctionFallibility : uint8_t {
	/* The function never raises a runtime error. */
	Infallible = 0,
	/* The function may raise a runtime error for some inputs. */
	Fallible = 1,
};

// How a function interacts with collations on its arguments.
enum class FunctionCollationHandling : uint8_t {
	/* Combines collations from its inputs and propagates them to its result (default). */
	Propagate = 0,
	/* Combinable collations are executed on the input arguments before the function runs. */
	PushCombinable = 1,
	/* Collations are ignored by the function. */
	Ignore = 2,
};

//----------------------------------------------------------------------------------------------------------------------
// Function Signature
//----------------------------------------------------------------------------------------------------------------------

// A callable function's shared shape: its fixed parameters (name plus type, each
// optionally with a default value), an optional variadic tail type, and an
// optional return type. Build one, then hand it to a function builder via
// SetSignature. The builder copies it in, so the signature can be destroyed
// afterwards. Structural validity (unique names, trailing defaults) is checked
// when the signature is registered with a builder, not while building it.
class FunctionSignature final : public detail::Handle<FunctionSignature> {
	friend detail::Factory;

public:
	FunctionSignature(FunctionSignature &&) noexcept = default;
	FunctionSignature &operator=(FunctionSignature &&) noexcept = default;

	~FunctionSignature() override;

	// Creates an empty signature.
	static FunctionSignature Create();

	// Appends a parameter (no default). ANY is accepted. Returns *this so the
	// setters chain, on a named signature or an inline temporary alike.
	auto AddParameter(const std::string &name, const LogicalType &type) -> FunctionSignature &;
	// Appends a parameter with a default value: the caller may omit it, the callee
	// still gets the default. The value is cast to type when type is concrete
	// (throws INVALID_INPUT if not castable) and stored as-is when type is ANY.
	auto AddParameterDefault(const std::string &name, const LogicalType &type, const Value &value)
	    -> FunctionSignature &;
	// Sets the variadic tail type (pass LogicalType::ANY to leave the tail
	// un-cast). Overwrites any prior variadic tail.
	auto SetVarArgs(const LogicalType &type) -> FunctionSignature &;
	// Sets the return type. Overwrites any prior return type.
	auto SetReturnType(const LogicalType &type) -> FunctionSignature &;

	// The number of fixed parameters, with and without defaults.
	auto GetParameterCount() const -> idx_t;
	// An owned copy of the parameter name at index.
	auto GetParameterName(idx_t index) const -> std::string;
	// An owned copy of the parameter type at index.
	auto GetParameterType(idx_t index) const -> LogicalType;
	// Whether the parameter at index carries a default value.
	auto ParameterHasDefault(idx_t index) const -> bool;
	// An owned copy of the parameter's default value; throws INVALID_INPUT when it
	// has none (test with ParameterHasDefault first).
	auto GetParameterDefault(idx_t index) const -> Value;
	auto HasVarArgs() const -> bool;
	// An owned copy of the variadic tail type; throws INVALID_INPUT when there is
	// none (test with HasVarArgs first).
	auto GetVarArgs() const -> LogicalType;
	auto HasReturnType() const -> bool;
	// An owned copy of the return type; throws INVALID_INPUT when there is none
	// (test with HasReturnType first).
	auto GetReturnType() const -> LogicalType;

private:
	explicit FunctionSignature(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

class ScalarFunction final : public detail::Handle<ScalarFunction> {
	friend detail::Factory;

public:
	class BindInput;
	class InitInput;
	class ExecInput;

	using BindCallback = void (*)(BindInput &input);
	using InitCallback = void (*)(InitInput &input);
	using ExecCallback = void (*)(ExecInput &input);

	ScalarFunction();

	~ScalarFunction() override;

	auto SetName(const std::string &name) & -> ScalarFunction &;
	// Copies a signature (parameter names, types, defaults, variadic tail, return
	// type) into the builder. Registration requires the return type to be present
	// and a fully defined concrete type. Overwrites any prior signature.
	auto SetSignature(const FunctionSignature &sig) & -> ScalarFunction &;

	// Constructs user data of type T, carried by the registered function and
	// freed at engine teardown; read from any callback via the inputs'
	// GetUserData<T>. Consumed by Register: set it again before
	// re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> ScalarFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> ScalarFunction &;
	auto SetInitCallback(InitCallback callback) & -> ScalarFunction &;
	auto SetExecCallback(ExecCallback callback) & -> ScalarFunction &;

	auto SetStability(FunctionStability value) & -> ScalarFunction &;
	auto GetStability() const -> FunctionStability;
	auto SetNullHandling(FunctionNullHandling value) & -> ScalarFunction &;
	auto GetNullHandling() const -> FunctionNullHandling;
	auto SetFallibility(FunctionFallibility value) & -> ScalarFunction &;
	auto GetFallibility() const -> FunctionFallibility;
	auto SetCollationHandling(FunctionCollationHandling value) & -> ScalarFunction &;
	auto GetCollationHandling() const -> FunctionCollationHandling;

	void Register(const Extension &extension);
	void Register(const Connection &conn);

private:
	BindCallback bind_callback = nullptr;
	InitCallback init_callback = nullptr;
	ExecCallback exec_callback = nullptr;
	detail::UserData user_data;

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

public:
	class BindInput {
		friend detail::Factory;

	public:
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		// The user data set via ScalarFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// The bound argument list in signature-slot order (fixed parameters plus
		// any varargs extras, already expanded). Arguments surface as types,
		// folded values, and slot names, never as expressions; the same accessor
		// family serves the aggregate and table binds.
		auto GetArgumentCount() const -> idx_t;
		// An owned copy of the argument's resolved type. Throws INVALID_INPUT
		// when index is out of range.
		auto GetArgumentType(idx_t index) const -> LogicalType;
		// Folds the argument to an owned value. Exists because bind runs before
		// optimizer constant folding. Throws INVALID_INPUT when the argument is
		// not constant-foldable or index is out of range.
		auto FoldArgument(idx_t index) const -> Value;
		// The slot's resolved name: the signature parameter name for a fixed
		// slot, the caller-provided name for a named vararg, empty for an
		// unnamed vararg. Throws INVALID_INPUT when index is out of range.
		auto GetArgumentName(idx_t index) const -> std::string;

		// The binding context (bind always runs under one). Borrowed, valid only
		// for the callback duration.
		auto GetContext() const -> Context;

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
		void *GetArgumentsHandle() const;
	};

	class InitInput {
		friend detail::Factory;

	public:
		template <class T, class... ARGS>
		void SetWorkerState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetWorkerStateInternal(ptr, detail::TypedDelete<T>);
		}

		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		// The user data set via ScalarFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// The context in which the function is being initialized; absent for
		// invocations that initialize the function without a client context
		// (some internal expression evaluations, e.g. index expressions, run
		// this way). Gate with HasContext(); GetContext() throws INVALID_INPUT
		// when absent. Borrowed, valid only for the callback duration.
		auto GetContext() const -> Context;
		auto HasContext() const -> bool;

	private:
		InitInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetWorkerStateInternal(void *data, void (*destructor)(void *));
		void *GetBindDataInternal() const;
		void *GetUserDataInternal() const;
	};

	class ExecInput {
		friend detail::Factory;

	public:
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}
		template <class T>
		auto GetWorkerState() const -> T & {
			auto ptr = GetWorkerStateInternal();
			return *static_cast<T *>(ptr);
		}

		// The user data set via ScalarFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		auto GetInputChunk() const -> DataChunk;
		auto GetResultVector() const -> Vector;

		// The execution context. Present during query execution; absent for
		// invocations that evaluate the function without a client context (some
		// internal expression evaluations run this way). Gate with HasContext();
		// GetContext() throws INVALID_INPUT when absent. Borrowed, valid only
		// for the callback duration. Enables context-dependent exec work: Arrow
		// conversions, casts.
		auto GetContext() const -> Context;
		auto HasContext() const -> bool;

	private:
		ExecInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void *GetBindDataInternal() const;
		void *GetWorkerStateInternal() const;
		void *GetUserDataInternal() const;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Aggregate Function
//----------------------------------------------------------------------------------------------------------------------

class AggregateFunction final : public detail::Handle<AggregateFunction> {
	friend detail::Factory;

public:
	// Whether the aggregate's result depends on the order in which rows are aggregated.
	enum class OrderDependence : uint8_t {
		/* The result depends on input order (default). */
		Dependent = 0,
		/* The result does not depend on input order. */
		Independent = 1,
	};

	// Whether the aggregate's result is affected by a DISTINCT modifier.
	enum class DistinctDependence : uint8_t {
		/* The result is affected by DISTINCT (default). */
		Dependent = 0,
		/* The result is not affected by DISTINCT. */
		Independent = 1,
	};

	class BindInput;
	class SizeInput;
	class InitializeInput;
	class UpdateInput;
	class CombineInput;
	class FinalizeInput;
	class DestroyInput;

	using BindCallback = void (*)(BindInput &input);
	using SizeCallback = void (*)(SizeInput &input);
	using InitializeCallback = void (*)(InitializeInput &input);
	using UpdateCallback = void (*)(UpdateInput &input);
	using CombineCallback = void (*)(CombineInput &input);
	using FinalizeCallback = void (*)(FinalizeInput &input);
	using DestroyCallback = void (*)(DestroyInput &input);

	AggregateFunction();

	~AggregateFunction() override;

	auto SetName(const std::string &name) & -> AggregateFunction &;
	// Copies a signature (parameter names, types, defaults, variadic tail, return
	// type) into the builder. Registration requires a return type that is not ANY.
	// Overwrites any prior signature.
	auto SetSignature(const FunctionSignature &sig) & -> AggregateFunction &;

	// Constructs user data of type T, carried by the registered function and
	// freed at engine teardown; read from any callback via the inputs'
	// GetUserData<T>. Consumed by Register: set it again before
	// re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> AggregateFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	// Optional. Invoked once during query planning; the bind data it sets is
	// visible to the update, combine, finalize, and destroy callbacks (not
	// to size and initialize).
	auto SetBindCallback(BindCallback callback) & -> AggregateFunction &;
	auto SetSizeCallback(SizeCallback callback) & -> AggregateFunction &;
	auto SetInitializeCallback(InitializeCallback callback) & -> AggregateFunction &;
	auto SetUpdateCallback(UpdateCallback callback) & -> AggregateFunction &;
	auto SetCombineCallback(CombineCallback callback) & -> AggregateFunction &;
	auto SetFinalizeCallback(FinalizeCallback callback) & -> AggregateFunction &;
	auto SetDestroyCallback(DestroyCallback callback) & -> AggregateFunction &;

	auto SetStability(FunctionStability value) & -> AggregateFunction &;
	auto GetStability() const -> FunctionStability;
	auto SetNullHandling(FunctionNullHandling value) & -> AggregateFunction &;
	auto GetNullHandling() const -> FunctionNullHandling;
	auto SetFallibility(FunctionFallibility value) & -> AggregateFunction &;
	auto GetFallibility() const -> FunctionFallibility;
	auto SetCollationHandling(FunctionCollationHandling value) & -> AggregateFunction &;
	auto GetCollationHandling() const -> FunctionCollationHandling;

	auto SetOrderDependence(OrderDependence value) & -> AggregateFunction &;
	auto GetOrderDependence() const -> OrderDependence;
	auto SetDistinctDependence(DistinctDependence value) & -> AggregateFunction &;
	auto GetDistinctDependence() const -> DistinctDependence;

	void Register(const Extension &extension);
	void Register(const Connection &conn);

public:
	class BindInput {
		friend detail::Factory;

	public:
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// The bound argument list in signature-slot order (fixed parameters plus
		// any varargs extras, already expanded). Arguments surface as types,
		// folded values, and slot names, never as expressions; the same accessor
		// family serves the aggregate and table binds.
		auto GetArgumentCount() const -> idx_t;
		// An owned copy of the argument's resolved type. Throws INVALID_INPUT
		// when index is out of range.
		auto GetArgumentType(idx_t index) const -> LogicalType;
		// Folds the argument to an owned value. Exists because bind runs before
		// optimizer constant folding. Throws INVALID_INPUT when the argument is
		// not constant-foldable or index is out of range.
		auto FoldArgument(idx_t index) const -> Value;
		// The slot's resolved name: the signature parameter name for a fixed
		// slot, the caller-provided name for a named vararg, empty for an
		// unnamed vararg. Throws INVALID_INPUT when index is out of range.
		auto GetArgumentName(idx_t index) const -> std::string;

		// The binding context (bind always runs under one). Borrowed, valid only
		// for the callback duration.
		auto GetContext() const -> Context;

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
		void *GetArgumentsHandle() const;
	};

	class SizeInput {
	public:
		class Inner;

		void Reserve(idx_t size_in_bytes);

		template <class T>
		void Reserve() {
			Reserve(sizeof(T));
		}

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit SizeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
	};

	class InitializeInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		T &Initialize(ARGS &&... args) {
			auto ptr = GetStatePointer();
			new (ptr) T(std::forward<ARGS>(args)...);
			return *static_cast<T *>(ptr);
		}

		void *GetStatePointer() const;

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit InitializeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
	};

	class UpdateInput {
	public:
		class Inner;

		auto GetInputChunk() const -> const DataChunk &;

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> T ** {
			auto ptr = GetStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit UpdateInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
		auto GetBindDataInternal() const -> const void *;
	};

	class CombineInput {
	public:
		class Inner;

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		auto GetStateCount() const -> idx_t;
		auto GetSourceStateArray() const -> void **;
		auto GetTargetStateArray() const -> void **;

		template <class T>
		auto GetSourceStateArray() const -> T ** {
			auto ptr = GetSourceStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		template <class T>
		auto GetTargetStateArray() const -> T ** {
			auto ptr = GetTargetStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit CombineInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
		auto GetBindDataInternal() const -> const void *;
	};

	class FinalizeInput {
	public:
		class Inner;

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> T ** {
			auto ptr = GetStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		auto GetResultVector() const -> Vector &;
		auto GetResultOffset() const -> idx_t;

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit FinalizeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
		auto GetBindDataInternal() const -> const void *;
	};

	class DestroyInput {
	public:
		class Inner;

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> const T ** {
			auto ptr = GetStateArray();
			return static_cast<const T **>(ptr);
		}

		// The user data set via AggregateFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		explicit DestroyInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetUserDataInternal() const -> void *;
		auto GetBindDataInternal() const -> const void *;
	};

private:
	BindCallback bind_callback = nullptr;
	SizeCallback size_callback = nullptr;
	InitializeCallback initialize_callback = nullptr;
	UpdateCallback update_callback = nullptr;
	CombineCallback combine_callback = nullptr;
	FinalizeCallback finalize_callback = nullptr;
	DestroyCallback destroy_callback = nullptr;
	detail::UserData user_data;

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// Table Function
//----------------------------------------------------------------------------------------------------------------------

class TableFunction final : public detail::Handle<TableFunction> {
	friend detail::Factory;

public:
	class BindInput;
	class InitGlobalInput;
	class InitLocalInput;
	class ExecInput;
	class PushdownInput;

	using BindCallback = void (*)(BindInput &input);
	using InitGlobalCallback = void (*)(InitGlobalInput &input);
	using InitLocalCallback = void (*)(InitLocalInput &input);
	using ExecCallback = void (*)(ExecInput &input);
	using PushdownCallback = void (*)(PushdownInput &input);

	TableFunction();

	~TableFunction() override;

	auto SetName(const std::string &name) & -> TableFunction &;
	// Copies a signature into the builder. A parameter without a default becomes a
	// required positional argument; a parameter with a default becomes a named
	// argument the caller may omit, its default injected by the bridge when the
	// call site omits it. The variadic tail applies to the positional arguments.
	// Registration rejects a signature with a return type set (a table function
	// declares its columns in bind). Overwrites any prior signature.
	auto SetSignature(const FunctionSignature &sig) & -> TableFunction &;

	// Constructs user data of type T, carried by the registered function and
	// freed at engine teardown; read from any callback via the inputs'
	// GetUserData<T>. Consumed by Register: set it again before
	// re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> TableFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> TableFunction &;
	auto SetInitGlobalCallback(InitGlobalCallback callback) & -> TableFunction &;
	auto SetInitLocalCallback(InitLocalCallback callback) & -> TableFunction &;
	auto SetExecCallback(ExecCallback callback) & -> TableFunction &;

	// Optional. Invoked during optimization with the candidate filter
	// expressions; the function claims the ones it will apply itself (see
	// PushdownInput).
	auto SetPushdownComplexFilterCallback(PushdownCallback callback) & -> TableFunction &;
	// When enabled, the engine may remove unused columns from the scan: the
	// init inputs report the projected columns and the exec chunk is sized to
	// them.
	auto SetProjectionPushdown(bool enable) & -> TableFunction &;

	void Register(const Extension &extension);
	void Register(const Connection &conn);

private:
	BindCallback bind_callback = nullptr;
	InitGlobalCallback init_global_callback = nullptr;
	InitLocalCallback init_local_callback = nullptr;
	ExecCallback exec_callback = nullptr;
	PushdownCallback pushdown_callback = nullptr;
	detail::UserData user_data;

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;

public:
	class BindInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		// The user data set via TableFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		auto AddResultColumn(const std::string &name, const LogicalType &type) -> void;

		// Declares one result column per schema field, in order (e.g. from
		// ArrowConversionPlan::GetSchema).
		auto AddResultColumns(const Schema &schema) -> void;

		// The call's arguments in signature-slot order: the fixed parameters
		// (a value for every one, omitted defaults injected), then any varargs
		// extras. The same accessor family as the scalar and aggregate binds.
		auto GetArgumentCount() const -> idx_t;
		// An owned copy of the argument's type. Throws INVALID_INPUT when index
		// is out of range.
		auto GetArgumentType(idx_t index) const -> LogicalType;
		// An owned copy of the argument's value (table arguments are constants,
		// so folding always succeeds). Throws INVALID_INPUT when index is out of
		// range.
		auto FoldArgument(idx_t index) const -> Value;
		// The slot's name: the signature parameter name for a fixed slot, empty
		// for a vararg. Throws INVALID_INPUT when index is out of range.
		auto GetArgumentName(idx_t index) const -> std::string;

		// Binding context (callback-duration).
		auto GetContext() const -> Context;

		// Static row-count hint for the optimizer.
		auto SetCardinality(idx_t cardinality, bool is_exact) -> void;

		explicit BindInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
		void *GetArgumentsHandle() const;
	};

	class InitGlobalInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		void SetGlobalState(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetGlobalStateInternal(ptr, detail::TypedDelete<T>);
		}

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		// The user data set via TableFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// Scan context (callback-duration).
		auto GetContext() const -> Context;

		// The projected columns (see SetProjectionPushdown): how many columns
		// the engine needs, and each one's original bind-time index.
		auto GetColumnCount() const -> idx_t;
		auto GetColumnIndex(idx_t projected_index) const -> idx_t;

		// Upper bound on worker threads for this scan (default 1).
		auto SetMaxThreads(idx_t max_threads) -> void;

		explicit InitGlobalInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto SetGlobalStateInternal(void *data, void (*destructor)(void *)) -> void;
		auto GetBindDataInternal() const -> void *;
		auto GetUserDataInternal() const -> void *;
	};

	class InitLocalInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		auto SetLocalState(ARGS &&... args) -> void {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetLocalStateInternal(ptr, detail::TypedDelete<T>);
		}

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		// The user data set via TableFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// The shared global state set in the init_global callback, to derive
		// per-thread local state from.
		template <class T>
		auto GetGlobalState() const -> T & {
			auto ptr = GetGlobalStateInternal();
			return *static_cast<T *>(ptr);
		}

		// Scan context (callback-duration).
		auto GetContext() const -> Context;

		// The projected columns, as on InitGlobalInput.
		auto GetColumnCount() const -> idx_t;
		auto GetColumnIndex(idx_t projected_index) const -> idx_t;

		explicit InitLocalInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto SetLocalStateInternal(void *data, void (*destructor)(void *)) -> void;
		auto GetBindDataInternal() const -> void *;
		auto GetUserDataInternal() const -> void *;
		auto GetGlobalStateInternal() const -> void *;
	};

	class ExecInput {
	public:
		class Inner;

		// Throws INVALID_INPUT when bind data was never set.
		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		template <class T>
		auto GetGlobalState() const -> T & {
			auto ptr = GetGlobalStateInternal();
			return *static_cast<T *>(ptr);
		}

		template <class T>
		auto GetLocalState() const -> T & {
			auto ptr = GetLocalStateInternal();
			return *static_cast<T *>(ptr);
		}

		auto GetResultChunk() const -> DataChunk &;

		// The user data set via TableFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// Execution context (callback-duration).
		auto GetContext() const -> Context;

		explicit ExecInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetBindDataInternal() const -> void *;
		auto GetGlobalStateInternal() const -> void *;
		auto GetLocalStateInternal() const -> void *;
		auto GetUserDataInternal() const -> void *;
	};

	// Passed to the pushdown callback with the candidate filter expressions
	// the engine would otherwise apply above the scan. Valid only for the
	// callback duration, as are the borrowed expressions it hands out. Claim
	// a filter with MarkHandled after extracting what exec needs into bind
	// data; the engine drops claimed filters and applies the rest.
	class PushdownInput {
	public:
		class Inner;

		// Mutable, unlike the other phases: this callback exists to extract
		// claimed predicates into bind data. Throws INVALID_INPUT when bind
		// data was never set.
		template <class T>
		auto GetBindData() -> T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<T *>(ptr);
		}

		// The user data set via TableFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		// Optimization context (callback-duration).
		auto GetContext() const -> Context;

		// The candidate filters: read-only bound expression trees.
		auto GetCount() const -> idx_t;
		auto GetExpression(idx_t index) const -> Expression;

		// The scan's pushdown-time column list: what BoundColumnRef
		// column_index values in the candidate filters index. Resolve each
		// position to its bind-declared column via GetColumnIndex (the engine
		// may re-prune the projection after handled filters drop, so the init
		// callbacks' projected list cannot decode filter references).
		auto GetColumnCount() const -> idx_t;
		auto GetColumnIndex(idx_t index) const -> idx_t;

		// Claims filter `index`: the engine drops it and the function must
		// apply it itself (e.g. while producing rows in exec).
		auto MarkHandled(idx_t index) -> void;

		explicit PushdownInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetBindDataInternal() const -> void *;
		auto GetUserDataInternal() const -> void *;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Copy Function
//----------------------------------------------------------------------------------------------------------------------

// A copy function exposes the (batch) COPY ... TO API. The engine accumulates the rows being copied into a
// ColumnDataCollection and hands each batch to the callbacks in this order: bind (planning) -> init (once per
// output file) -> batch (per accumulated batch) -> flush (per prepared batch) -> finalize (once per output file).
//
// State threads forward through the data setters: bind data is immutable and visible everywhere; init data is the
// per-file global state; batch data is produced from a batch and consumed by flush.
class CopyFunction final : public detail::Handle<CopyFunction> {
	friend detail::Factory;

public:
	class BindInput;
	class InitInput;
	class BatchInput;
	class FlushInput;
	class FinalizeInput;

	using BindCallback = void (*)(BindInput &input);
	using InitCallback = void (*)(InitInput &input);
	using BatchCallback = void (*)(BatchInput &input);
	using FlushCallback = void (*)(FlushInput &input);
	using FinalizeCallback = void (*)(FinalizeInput &input);

	CopyFunction();

	CopyFunction(CopyFunction &&) = default;
	CopyFunction &operator=(CopyFunction &&) = default;

	~CopyFunction() override;

	auto SetName(const std::string &name) & -> CopyFunction &;

	// Constructs user data of type T, carried by the registered function and
	// freed at engine teardown; read from any callback via the inputs'
	// GetUserData<T>. Consumed by Register: set it again before
	// re-registering.
	template <class T, class... ARGS>
	auto SetUserData(ARGS &&... args) & -> CopyFunction & {
		auto ptr = new T(std::forward<ARGS>(args)...);
		SetUserDataInternal(ptr, detail::TypedDelete<T>);
		return *this;
	}

	auto SetBindCallback(BindCallback callback) & -> CopyFunction &;
	auto SetInitCallback(InitCallback callback) & -> CopyFunction &;
	auto SetBatchCallback(BatchCallback callback) & -> CopyFunction &;
	auto SetFlushCallback(FlushCallback callback) & -> CopyFunction &;
	auto SetFinalizeCallback(FinalizeCallback callback) & -> CopyFunction &;

	auto Register(const Extension &extension) -> void;
	auto Register(const Connection &conn) -> void;

public:
	class BindInput {
		friend detail::Factory;

	public:
		auto GetContext() const -> Context;

		// The names and types of the columns being copied
		auto GetColumnCount() const -> idx_t;
		auto GetColumnName(idx_t index) const -> std::string_view;
		auto GetColumnType(idx_t index) const -> LogicalType; // TODO: Borrowed handle to column type

		// Set immutable bind data, visible to all later callbacks.
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		// The user data set via CopyFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		BindInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	class InitInput {
		friend detail::Factory;

	public:
		auto GetContext() const -> Context;

		// The output file path this state is being initialized for.
		auto GetFilePath() const -> std::string_view;

		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		// Set the per-file global state, visible to batch, flush and finalize.
		template <class T, class... ARGS>
		void SetInitData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetInitDataInternal(ptr, detail::TypedDelete<T>);
		}

		// The user data set via CopyFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		InitInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		const void *GetBindDataInternal() const;
		void SetInitDataInternal(void *data, void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	class BatchInput {
		friend detail::Factory;

	public:
		auto GetContext() const -> Context;

		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		// The batch of rows to copy. Ownership belongs to this input: it is destroyed when the callback returns.
		auto GetBatch() -> ColumnDataCollection & {
			return collection;
		}

		// Set the prepared batch data, consumed by the flush callback.
		template <class T, class... ARGS>
		void SetBatchData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBatchDataInternal(ptr, detail::TypedDelete<T>);
		}

		// The user data set via CopyFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		BatchInput(void *args, void *context, ColumnDataCollection &&collection)
		    : args(args), context(context), collection(std::move(collection)) {
		}

		void *args;
		void *context;
		ColumnDataCollection collection;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void SetBatchDataInternal(void *data, void (*destructor)(void *));
		void *GetUserDataInternal() const;
	};

	class FlushInput {
		friend detail::Factory;

	public:
		auto GetContext() const -> Context;

		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		template <class T>
		auto GetBatchData() const -> T & {
			return *static_cast<T *>(GetBatchDataInternal());
		}

		// The user data set via CopyFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		FlushInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetBatchDataInternal() const;
		void *GetUserDataInternal() const;
	};

	class FinalizeInput {
		friend detail::Factory;

	public:
		auto GetContext() const -> Context;

		template <class T>
		auto GetBindData() const -> const T & {
			return *static_cast<const T *>(GetBindDataInternal());
		}

		template <class T>
		auto GetInitData() const -> T & {
			return *static_cast<T *>(GetInitDataInternal());
		}

		// The user data set via CopyFunction::SetUserData; throws
		// INVALID_INPUT when none was set.
		template <class T>
		auto GetUserData() const -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		FinalizeInput(void *args, void *context) : args(args), context(context) {
		}

		void *args;
		void *context;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetUserDataInternal() const;
	};

private:
	BindCallback bind_callback = nullptr;
	InitCallback init_callback = nullptr;
	BatchCallback batch_callback = nullptr;
	FlushCallback flush_callback = nullptr;
	FinalizeCallback finalize_callback = nullptr;
	detail::UserData user_data;

	auto SetUserDataInternal(void *data, void (*destructor)(void *)) -> void;
};

//----------------------------------------------------------------------------------------------------------------------
// Cast Function
//----------------------------------------------------------------------------------------------------------------------

// A cast function converts values of a source logical type into a target logical type. The exec callback is handed the
// flattened input vector, the output vector to write into, the number of rows, and the cast mode. In CAST_MODE::TRY the
// callback should write NULL for rows it cannot convert; in CAST_MODE::NORMAL it should throw to abort the query.
class CastFunction final : public detail::Handle<CastFunction> {
	friend detail::Factory;

public:
	enum class CastMode : uint8_t {
		/* A regular cast: conversion failures should be reported (by throwing) and abort the query. */
		NORMAL = 0,
		/* A "try" cast: conversion failures should be written as NULLs in the output instead of throwing. */
		TRY = 1,
	};

	class ExecInput;

	using ExecCallback = void (*)(ExecInput &input);

	CastFunction();

	~CastFunction() override;

	auto SetSourceType(const LogicalType &type) & -> CastFunction &;
	auto SetTargetType(const LogicalType &type) & -> CastFunction &;
	auto SetImplicitCastCost(int64_t cost) & -> CastFunction &;
	auto SetExecCallback(ExecCallback callback) & -> CastFunction &;

	void Register(const Extension &extension);
	void Register(const Connection &conn);

private:
	ExecCallback exec_callback = nullptr;

public:
	class ExecInput {
		friend detail::Factory;

	public:
		// The input vector holding the source values to cast.
		auto GetInput() const -> Vector;
		// The output vector the callback writes the converted values into.
		auto GetOutput() const -> Vector;
		// The number of rows to cast.
		auto GetCount() const -> idx_t;
		// The mode the cast is executing in (NORMAL vs TRY).
		auto GetCastMode() const -> CastMode;

	private:
		explicit ExecInput(void *args) : args(args) {
		}

		void *args;
	};
};

//----------------------------------------------------------------------------------------------------------------------
// Custom Type
//----------------------------------------------------------------------------------------------------------------------

// Registers a user-defined logical type in the catalog so it can be referenced by name in SQL. A custom type is
// currently an alias of an existing "base" type (e.g. an INTEGER aliased as TEMPERATURE): it shares the base type's
// physical representation but is logically distinct, so it can carry its own cast functions. Configure the builder via
// SetName / SetBaseType, then Register it. Obtain a LogicalType for the registered type via base.WithAlias(name).
class CustomType final : public detail::Handle<CustomType> {
	friend detail::Factory;

public:
	CustomType();

	~CustomType() override;

	auto SetName(const std::string &name) & -> CustomType &;
	auto SetBaseType(const LogicalType &type) & -> CustomType &;

	void Register(const Extension &extension);
	void Register(const Connection &conn);
};

} // namespace duckdb_api

//----------------------------------------------------------------------------------------------------------------------
// Extension entrypoint
//----------------------------------------------------------------------------------------------------------------------

namespace duckdb_api {
namespace detail {
// Performs the V2 C API extension-load handshake, invokes init_cb with the loading extension, and reports errors.
// The `extension` and `access` are the opaque loader arguments of the C entrypoint.
// The concrete types live in duckdb_extension_v2.h, which only the implementation file includes.
bool ExtensionEntrypoint(void *extension, void *access, void (*init_cb)(Extension &extension));
} // namespace detail
} // namespace duckdb_api

#define DUCKDB_CPP_EXTENSION_GLUE_HELPER(x, y) x##y
#define DUCKDB_CPP_EXTENSION_GLUE(x, y)        DUCKDB_CPP_EXTENSION_GLUE_HELPER(x, y)

// Opaque forward declarations of the loader's types.
// The entrypoint below must have exactly the loader's function type, spelled with the same tags.
// The definitions live in the C headers, which users of this header never include.
struct _duckdb_extension_info;
struct duckdb_extension_access;

#ifdef _WIN32
#define DUCKDB_CPP_ENTRY_VISIBILITY __declspec(dllexport)
#else
#define DUCKDB_CPP_ENTRY_VISIBILITY __attribute__((visibility("default")))
#endif

// Defines the extension entrypoint for a C API extension written against the stable C++ API. Requires
// DUCKDB_EXTENSION_NAME to be set. The body receives the extension currently being loaded and may throw; exceptions
// are reported to the loader as an initialization error.
// Usage:
//
//		DUCKDB_CPP_EXTENSION_ENTRYPOINT(extension) {
//			ScalarFunction function;
//			...
//			function.Register(extension);
//		}
//
#define DUCKDB_CPP_EXTENSION_ENTRYPOINT(EXTENSION_PARAM)                                                               \
	static void DUCKDB_CPP_EXTENSION_GLUE(DUCKDB_EXTENSION_NAME, _cpp_api_impl)(duckdb_api::Extension &);              \
	extern "C" DUCKDB_CPP_ENTRY_VISIBILITY bool DUCKDB_CPP_EXTENSION_GLUE(DUCKDB_EXTENSION_NAME, _init_c_api)(         \
	    struct _duckdb_extension_info * info, struct duckdb_extension_access * access) {                               \
		return duckdb_api::detail::ExtensionEntrypoint(                                                                \
		    info, access, DUCKDB_CPP_EXTENSION_GLUE(DUCKDB_EXTENSION_NAME, _cpp_api_impl));                            \
	}                                                                                                                  \
	static void DUCKDB_CPP_EXTENSION_GLUE(DUCKDB_EXTENSION_NAME, _cpp_api_impl)(duckdb_api::Extension & EXTENSION_PARAM)
