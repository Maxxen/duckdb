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
	Exception(uint32_t code, std::string message, std::string raw_message = {})
	    : std::runtime_error(std::move(message)), code(code), raw_message(std::move(raw_message)) {
	}

	uint32_t GetCode() const {
		return code;
	}

	// The message body with what()'s "<Type> Error: " prefix stripped, or empty.
	// Not derivable from what() (no type name here to rebuild the prefix); in the
	// engine's rendered form (location block, or JSON under errors_as_json).
	const std::string &GetRawMessage() const {
		return raw_message;
	}

private:
	uint32_t code;
	std::string raw_message;
};

//----------------------------------------------------------------------------------------------------------------------
// Database Option
//----------------------------------------------------------------------------------------------------------------------

class DatabaseOption final : public detail::Handle<DatabaseOption> {
	friend detail::Factory;

public:
	DatabaseOption(const std::string &name, const std::string &value);

	std::string_view GetName() const;
	std::string_view GetValue() const;
	std::string_view GetDefaultValue() const;
	std::string_view GetDescription() const;

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

class Context final : public detail::Handle<Context> {
	friend detail::Factory;

public:
	~Context() override;

	FileSystem GetFileSystem() const;

	// Log a message from this connection. This is infallible and will not throw exceptions.
	void Log(LogLevel level, const std::string &message) const noexcept;

private:
	explicit Context(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// SQL statements
//----------------------------------------------------------------------------------------------------------------------

// An owned, parsed SQL statement, yielded by StatementIterator::Next and
// consumed by Connection::Query.
class SqlStatement final : public detail::Handle<SqlStatement> {
	friend detail::Factory;

public:
	SqlStatement(SqlStatement &&) noexcept = default;
	SqlStatement &operator=(SqlStatement &&) noexcept = default;

	~SqlStatement() override;

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
	void SetOption(const DatabaseOption &option);

	void WithTransaction(std::function<void(const Context &ctx)> callback);

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
};

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------
class LogicalType final : public detail::Handle<LogicalType> {
	friend detail::Factory;

public:
	LogicalType(LogicalType &&) noexcept = default;
	LogicalType &operator=(LogicalType &&) noexcept = default;

	~LogicalType() override;

	static LogicalType VARCHAR();
	static LogicalType INTEGER();
	static LogicalType BIGINT();

	LogicalType WithAlias(std::string_view alias) const;

	std::string_view GetAlias() const;
	bool operator==(const LogicalType &other) const;
	bool operator!=(const LogicalType &other) const {
		return !(*this == other);
	}

private:
	explicit LogicalType(void *impl);
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
// Value
//----------------------------------------------------------------------------------------------------------------------
class Value final : public detail::Handle<Value> {
	friend detail::Factory;

public:
	~Value() override;

	Value(Value &&) noexcept = default;
	Value &operator=(Value &&) noexcept = default;

	// Construct an owned value. The library copies the input.
	static Value FromI64(int64_t value);
	static Value FromVarchar(const std::string &value);

	auto IsNull() const -> bool;
	auto GetLogicalType() const -> LogicalType;
	auto ToString() const -> std::string;

	auto AsBool() const -> bool;

	auto AsI8() const -> int8_t;
	auto AsU8() const -> uint8_t;

	auto AsI16() const -> int16_t;
	auto AsU16() const -> uint16_t;

	auto AsI32() const -> int32_t;
	auto AsU32() const -> uint32_t;

	auto AsI64() const -> int64_t;
	auto AsU64() const -> uint64_t;

	auto AsF32() const -> float;
	auto AsF64() const -> double;

	auto AsVarchar() const -> std::string_view;

	// TODO: Add more

private:
	explicit Value(void *impl);
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
struct StringStorage {
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
	static auto Inlined(const char *data, uint32_t len) -> StringStorage {
		assert(len <= INLINE_LENGTH);
		StringStorage storage {};
		storage.value.inlined.length = len;
		if (len > 0) {
			std::memcpy(storage.value.inlined.inlined, data, len);
		}
		return storage;
	}

	// Non-inlined token over `len` bytes at `heap_data` (from Allocate); sets the
	// prefix. `len` must exceed INLINE_LENGTH, else it would read as inlined.
	static auto FromHeapData(char *heap_data, uint32_t len) -> StringStorage {
		assert(len > INLINE_LENGTH);
		StringStorage storage {};
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
// returns StringStorage tokens to place in any order (dedup, scatter). Borrowed;
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
	auto Add(std::string_view data) -> StringStorage {
		if (data.size() > UINT32_MAX) {
			ThrowStringTooLong(data.size());
		}
		if (data.size() <= StringStorage::INLINE_LENGTH) {
			return StringStorage::Inlined(data.data(), static_cast<uint32_t>(data.size()));
		}
		auto len = static_cast<uint32_t>(data.size());
		auto *bytes = Allocate(len);
		std::memcpy(bytes, data.data(), len);
		return StringStorage::FromHeapData(reinterpret_cast<char *>(bytes), len);
	}

	// Bulk Add: interns every view, returning the tokens in order.
	auto AddMany(const std::vector<std::string_view> &data) -> std::vector<StringStorage> {
		std::vector<StringStorage> out;
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

// Writer over a FLAT vector's validity mask (from Vector::GetValidityMutable).
// Word W bit N covers row W*64+N; a set bit means valid (not NULL).
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

	// ---- End vector read surface ----

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
	auto SetString(idx_t index, StringStorage value) -> void;

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
	DataChunk(const Context &context, const std::vector<LogicalType> &types);

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

	ColumnDataCollection(ColumnDataCollection &&other) noexcept = default;
	ColumnDataCollection &operator=(ColumnDataCollection &&other) noexcept = default;

	~ColumnDataCollection() override;

	// Get the number of rows currently stored in the collection.
	auto GetRowCount() const -> idx_t;

	// Merge the other collection into this one, destroying it in the process.
	auto Combine(ColumnDataCollection other) -> void;

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

	QueryResult(QueryResult &&) noexcept = default;
	QueryResult &operator=(QueryResult &&) noexcept = default;

	~QueryResult() override;

	// The result's output schema (its column names and types) as one owned Schema.
	auto GetSchema() const -> Schema;

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
	explicit LogStorage(const Context &ctx);

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
	auto Register(const Context &ctx) -> void;

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

	explicit ScalarFunction(const Context &ctx);

	~ScalarFunction() override;

	auto SetName(const std::string &name) & -> ScalarFunction &;
	auto AddParameter(const std::string &name, const LogicalType &type) & -> ScalarFunction &;
	auto SetReturnType(const LogicalType &type) & -> ScalarFunction &;

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

	void Register(const Context &ctx);

private:
	BindCallback bind_callback = nullptr;
	InitCallback init_callback = nullptr;
	ExecCallback exec_callback = nullptr;

public:
	class BindInput {
		friend detail::Factory;

	public:
		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		template <class T>
		auto GetBindData() -> T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		explicit BindInput(void *args) : args(args) {
		}

		void *args;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
		void *GetBindDataInternal() const;
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

		template <class T>
		auto GetWorkerState() -> T & {
			auto ptr = GetWorkerStateInternal();
			return *static_cast<T *>(ptr);
		}

	private:
		explicit InitInput(void *args) : args(args) {
		}

		void *args;

		void SetWorkerStateInternal(void *data, void (*destructor)(void *));
		void *GetWorkerStateInternal() const;
		void *GetBindDataInternal() const;
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

		auto GetInputChunk() const -> DataChunk;
		auto GetResultVector() const -> Vector;

	private:
		explicit ExecInput(void *args) : args(args) {
		}

		void *args;

		void *GetBindDataInternal() const;
		void *GetWorkerStateInternal() const;
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

	class SizeInput;
	class InitializeInput;
	class UpdateInput;
	class CombineInput;
	class FinalizeInput;
	class DestroyInput;

	using SizeCallback = void (*)(SizeInput &input);
	using InitializeCallback = void (*)(InitializeInput &input);
	using UpdateCallback = void (*)(UpdateInput &input);
	using CombineCallback = void (*)(CombineInput &input);
	using FinalizeCallback = void (*)(FinalizeInput &input);
	using DestroyCallback = void (*)(DestroyInput &input);

	explicit AggregateFunction(const Context &ctx);

	~AggregateFunction() override;

	auto SetName(const std::string &name) & -> AggregateFunction &;
	auto AddParameter(const std::string &name, const LogicalType &type) & -> AggregateFunction &;
	auto SetReturnType(const LogicalType &type) & -> AggregateFunction &;

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

	void Register(const Context &ctx);

public:
	class SizeInput {
	public:
		class Inner;

		void Reserve(idx_t size_in_bytes);

		template <class T>
		void Reserve() {
			Reserve(sizeof(T));
		}

		explicit SizeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
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

		explicit InitializeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

	class UpdateInput {
	public:
		class Inner;

		auto GetInputChunk() const -> const DataChunk &;

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> T ** {
			auto ptr = GetStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		explicit UpdateInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

	class CombineInput {
	public:
		class Inner;

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

		explicit CombineInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

	class FinalizeInput {
	public:
		class Inner;

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> T ** {
			auto ptr = GetStateArray();
			return reinterpret_cast<T **>(ptr);
		}

		auto GetResultVector() const -> Vector &;
		auto GetResultOffset() const -> idx_t;

		explicit FinalizeInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

	class DestroyInput {
	public:
		class Inner;

		auto GetStateCount() const -> idx_t;
		auto GetStateArray() const -> void **;

		template <class T>
		auto GetStateArray() const -> const T ** {
			auto ptr = GetStateArray();
			return static_cast<const T **>(ptr);
		}

		explicit DestroyInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;
	};

private:
	SizeCallback size_callback = nullptr;
	InitializeCallback initialize_callback = nullptr;
	UpdateCallback update_callback = nullptr;
	CombineCallback combine_callback = nullptr;
	FinalizeCallback finalize_callback = nullptr;
	DestroyCallback destroy_callback = nullptr;
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

	explicit TableFunction(const Context &ctx);

	~TableFunction() override;

	auto SetName(const std::string &name) & -> TableFunction &;
	auto AddParameter(const LogicalType &type) & -> TableFunction &;
	auto AddNamedParameter(const std::string &name, const LogicalType &type) & -> TableFunction &;

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

	void Register(const Context &ctx);

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

		auto GetParameter(idx_t index) const -> Value;
		auto GetNamedParameter(const std::string &name) const -> Value;

		auto TryGetParameter(idx_t index) const -> std::optional<Value>;
		auto TryGetNamedParameter(const std::string &name) const -> std::optional<Value>;

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

	explicit CopyFunction(const Context &ctx);

	CopyFunction(CopyFunction &&) = default;
	CopyFunction &operator=(CopyFunction &&) = default;

	~CopyFunction() override;

	auto SetName(const std::string &name) & -> CopyFunction &;
	auto SetBindCallback(BindCallback callback) & -> CopyFunction &;
	auto SetInitCallback(InitCallback callback) & -> CopyFunction &;
	auto SetBatchCallback(BatchCallback callback) & -> CopyFunction &;
	auto SetFlushCallback(FlushCallback callback) & -> CopyFunction &;
	auto SetFinalizeCallback(FinalizeCallback callback) & -> CopyFunction &;

	auto Register(const Context &ctx) -> void;

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

	private:
		explicit BindInput(void *args) : args(args) {
		}

		void *args;

		void SetBindDataInternal(void *data, bool (*equals)(void *a, void *b), void (*destructor)(void *));
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

	private:
		explicit InitInput(void *args) : args(args) {
		}

		void *args;

		const void *GetBindDataInternal() const;
		void SetInitDataInternal(void *data, void (*destructor)(void *));
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

	private:
		BatchInput(void *args, ColumnDataCollection &&collection) : args(args), collection(std::move(collection)) {
		}

		void *args;
		ColumnDataCollection collection;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void SetBatchDataInternal(void *data, void (*destructor)(void *));
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

	private:
		explicit FlushInput(void *args) : args(args) {
		}

		void *args;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
		void *GetBatchDataInternal() const;
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

	private:
		explicit FinalizeInput(void *args) : args(args) {
		}

		void *args;

		const void *GetBindDataInternal() const;
		void *GetInitDataInternal() const;
	};

private:
	BindCallback bind_callback = nullptr;
	InitCallback init_callback = nullptr;
	BatchCallback batch_callback = nullptr;
	FlushCallback flush_callback = nullptr;
	FinalizeCallback finalize_callback = nullptr;
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

	explicit CastFunction(const Context &ctx);

	~CastFunction() override;

	auto SetSourceType(const LogicalType &type) & -> CastFunction &;
	auto SetTargetType(const LogicalType &type) & -> CastFunction &;
	auto SetImplicitCastCost(int64_t cost) & -> CastFunction &;
	auto SetExecCallback(ExecCallback callback) & -> CastFunction &;

	void Register(const Context &ctx);

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
	explicit CustomType(const Context &ctx);

	~CustomType() override;

	auto SetName(const std::string &name) & -> CustomType &;
	auto SetBaseType(const LogicalType &type) & -> CustomType &;

	void Register(const Context &ctx);
};

} // namespace duckdb_api
