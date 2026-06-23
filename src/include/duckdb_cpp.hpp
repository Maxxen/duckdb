#pragma once

#include <functional>
#include <utility>
#include <string>
#include <optional>
#include <stdexcept>
#include <cstdint>
#include <memory>

// (Experimental) Stable C++ API

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
	CREATE = 4,
	/* Create a new file, or fail if it already exists. */
	CREATE_NEW = 8,
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

//----------------------------------------------------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------------------------------------------------

class QueryResult;

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

	// Starts lazy, streaming execution of a parsed statement; nothing runs
	// until the result is stepped. The statement is consumed. Throws
	// RESOURCE_IN_USE while a live result exists.
	QueryResult Query(SqlStatement statement);

	// Single-statement convenience over ParseSQL + Query: throws
	// INVALID_INPUT unless the input contains exactly one statement.
	QueryResult Query(const std::string &sql);

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

class Database final : public detail::Handle<Database> {
	friend detail::Factory;

public:
	~Database() override;

	size_t GetOptionCount() const;
	DatabaseOption GetOptionByIndex(size_t index) const;
	void SetOption(const DatabaseOption &option);

	Connection Connect();

private:
	explicit Database(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Environment
//----------------------------------------------------------------------------------------------------------------------

class Environment final : public detail::Handle<Environment> {
	friend detail::Factory;

public:
	Environment();
	~Environment() override;

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
// Value
//----------------------------------------------------------------------------------------------------------------------
class Value final : public detail::Handle<Value> {
	friend detail::Factory;

public:
	~Value() override;

	Value(Value &&) noexcept = default;
	Value &operator=(Value &&) noexcept = default;

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
// Vector
//----------------------------------------------------------------------------------------------------------------------
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

private:
	explicit Vector(void *impl);
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

private:
	explicit DataChunk(void *impl, bool owned);
	bool owned = false; // UGLY, this should probably be done c++-side.
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

	auto GetColumnCount() const -> idx_t;
	auto GetColumnName(idx_t index) const -> std::string_view;
	auto GetColumnType(idx_t index) const -> LogicalType;

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

private:
	explicit QueryResult(void *impl);
};

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

	using BindCallback = void (*)(BindInput &input);
	using InitGlobalCallback = void (*)(InitGlobalInput &input);
	using InitLocalCallback = void (*)(InitLocalInput &input);
	using ExecCallback = void (*)(ExecInput &input);

	explicit TableFunction(const Context &ctx);

	~TableFunction() override;

	auto SetName(const std::string &name) & -> TableFunction &;
	auto AddParameter(const LogicalType &type) & -> TableFunction &;
	auto AddNamedParameter(const std::string &name, const LogicalType &type) & -> TableFunction &;

	auto SetBindCallback(BindCallback callback) & -> TableFunction &;
	auto SetInitGlobalCallback(InitGlobalCallback callback) & -> TableFunction &;
	auto SetInitLocalCallback(InitLocalCallback callback) & -> TableFunction &;
	auto SetExecCallback(ExecCallback callback) & -> TableFunction &;

	void Register(const Context &ctx);

private:
	BindCallback bind_callback = nullptr;
	InitGlobalCallback init_global_callback = nullptr;
	InitLocalCallback init_local_callback = nullptr;
	ExecCallback exec_callback = nullptr;

public:
	class BindInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		void SetBindData(ARGS &&... args) {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetBindDataInternal(ptr, detail::TypedEquals<T>, detail::TypedDelete<T>);
		}

		template <class T>
		auto GetUserData() -> T & {
			auto ptr = GetUserDataInternal();
			return *static_cast<T *>(ptr);
		}

		auto AddResultColumn(const std::string &name, const LogicalType &type) -> void;

		auto GetParameter(idx_t index) const -> Value;
		auto GetNamedParameter(const std::string &name) const -> Value;

		auto TryGetParameter(idx_t index) const -> std::optional<Value>;
		auto TryGetNamedParameter(const std::string &name) const -> std::optional<Value>;

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

		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		explicit InitGlobalInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto SetGlobalStateInternal(void *data, void (*destructor)(void *)) -> void;
		auto GetBindDataInternal() const -> void *;
	};

	class InitLocalInput {
	public:
		class Inner;

		template <class T, class... ARGS>
		auto SetLocalState(ARGS &&... args) -> void {
			auto ptr = new T(std::forward<ARGS>(args)...);
			SetLocalStateInternal(ptr, detail::TypedDelete<T>);
		}

		template <class T>
		auto GetBindData() const -> const T & {
			auto ptr = GetBindDataInternal();
			return *static_cast<const T *>(ptr);
		}

		explicit InitLocalInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto SetLocalStateInternal(void *data, void (*destructor)(void *)) -> void;
		auto GetBindDataInternal() const -> void *;
	};

	class ExecInput {
	public:
		class Inner;

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

		explicit ExecInput(Inner &inner) : inner(inner) {
		}

	private:
		Inner &inner;

		auto GetBindDataInternal() const -> void *;
		auto GetGlobalStateInternal() const -> void *;
		auto GetLocalStateInternal() const -> void *;
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
