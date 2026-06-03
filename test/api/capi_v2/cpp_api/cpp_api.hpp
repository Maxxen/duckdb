#pragma once

#include <functional>
#include <utility>
#include <string>
#include <optional>

// (Experimental) Stable C++ API

namespace duckdb_api {

//----------------------------------------------------------------------------------------------------------------------
// Internal Implementation Details
//----------------------------------------------------------------------------------------------------------------------

namespace detail {

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

	virtual ~Handle() noexcept = default;

protected:
	Handle() : impl(nullptr) {
	}
	explicit Handle(void *impl) : impl(impl) {
	}

	void *impl;

private:
	friend void *GetHandle(const Handle &handle);
	friend void *LeakHandle(Handle &handle);
};

inline void *GetHandle(const Handle &handle) {
	return handle.impl;
}
inline void *LeakHandle(Handle &handle) {
	void *impl = handle.impl;
	handle.impl = nullptr;
	return impl;
}

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
void *TypedCopy(void *ptr) {
	auto typed_ptr = static_cast<T *>(ptr);
	return new T(*typed_ptr);
}

template <class T>
bool TypedEquals(void *ptr_a, void *ptr_b) {
	auto typed_a = static_cast<T *>(ptr_a);
	auto typed_b = static_cast<T *>(ptr_b);
	return *typed_a == *typed_b;
}

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Exceptions
//----------------------------------------------------------------------------------------------------------------------

class Exception : public std::runtime_error {
public:
	// TODO: add more exception types!
	Exception(int32_t code, std::string message) : std::runtime_error(std::move(message)), code(code) {
	}

	int32_t GetCode() const {
		return code;
	}

private:
	int32_t code;
};

//----------------------------------------------------------------------------------------------------------------------
// Database Option
//----------------------------------------------------------------------------------------------------------------------

class DatabaseOption final : public detail::Handle {
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

class FileHandle final : public detail::Handle {
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

class FileSystem final : public detail::Handle {
	friend detail::Factory;

public:
	~FileSystem() override;
	FileHandle OpenFile(const std::string &path, FileFlags flags) const;

private:
	explicit FileSystem(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

class Context final : public detail::Handle {
	friend detail::Factory;

public:
	~Context() override;

	FileSystem GetFileSystem() const;

private:
	explicit Context(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Connection
//----------------------------------------------------------------------------------------------------------------------

class QueryResult;

class Connection final : public detail::Handle {
	friend detail::Factory;

public:
	~Connection() override;

	size_t GetOptionCount() const;
	DatabaseOption GetOptionByIndex(size_t index) const;
	void SetOption(const DatabaseOption &option);

	void WithTransaction(std::function<void(const Context &ctx)> callback);

	QueryResult Query(const std::string &sql);

private:
	explicit Connection(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Database
//----------------------------------------------------------------------------------------------------------------------

class Database final : public detail::Handle {
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

class Environment final : public detail::Handle {
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
class LogicalType final : public detail::Handle {
	friend detail::Factory;

public:
	LogicalType(LogicalType &&) noexcept = default;
	LogicalType &operator=(LogicalType &&) noexcept = default;

	~LogicalType() override;

	static LogicalType VARCHAR();
	static LogicalType INTEGER();
	static LogicalType BIGINT();

	std::string_view GetAlias() const;

private:
	explicit LogicalType(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Value
//----------------------------------------------------------------------------------------------------------------------
class Value final : public detail::Handle {
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
class Vector final : public detail::Handle {
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
class DataChunk final : public detail::Handle {
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
	bool owned; // UGLY, this should probably be done c++-side.
};

class ColumnDataCollection final : public detail::Handle {
	friend detail::Factory;

public:
	ColumnDataCollection(const Context &context, const std::vector<LogicalType> &types);

	ColumnDataCollection(ColumnDataCollection &&other) noexcept = default;
	ColumnDataCollection &operator=(ColumnDataCollection &&other) noexcept = default;

	~ColumnDataCollection() override;

	// Get the number of rows currently stored in the collection.
	auto GetRowCount() const -> idx_t;

	// Merge the other collection into this one, destroying it in the process.
	auto Combine(ColumnDataCollection &&other) -> void;

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
	class AppendState final : public detail::Handle {
		friend detail::Factory;

	public:
		AppendState(AppendState &&other) noexcept = default;
		AppendState &operator=(AppendState &&other) noexcept = default;
		~AppendState() override;

	private:
		explicit AppendState(void *impl);
	};

	class ScanState final : public detail::Handle {
		friend detail::Factory;

	public:
		ScanState(ScanState &&other) noexcept = default;
		ScanState &operator=(ScanState &&other) noexcept = default;
		~ScanState() override;

	private:
		explicit ScanState(void *impl);
	};

	class SharedScanState final : public detail::Handle {
		friend detail::Factory;

	public:
		SharedScanState(SharedScanState &&other) noexcept = default;
		SharedScanState &operator=(SharedScanState &&other) noexcept = default;
		~SharedScanState() override;

	private:
		explicit SharedScanState(void *impl);
	};

	class WorkerScanState final : public detail::Handle {
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
class QueryResult final : public detail::Handle {
	friend detail::Factory;

public:
	QueryResult(QueryResult &&) noexcept = default;
	QueryResult &operator=(QueryResult &&) noexcept = default;

	~QueryResult() override;

	auto GetColumnCount() const -> idx_t;
	auto GetColumnName(idx_t index) const -> std::string_view;
	auto GetColumnType(idx_t index) const -> LogicalType;
	auto GetRowsChanged() const -> idx_t;

	auto GetChunkCount() const -> idx_t;
	auto GetChunk(idx_t index) const -> DataChunk;

private:
	explicit QueryResult(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Scalar Function
//----------------------------------------------------------------------------------------------------------------------

class ScalarFunction final : public detail::Handle {
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
			SetBindDataInternal(ptr, detail::TypedCopy<T>, detail::TypedEquals<T>, detail::TypedDelete<T>);
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

		void SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
		                         void (*destructor)(void *));
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

class AggregateFunction final : public detail::Handle {
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

class TableFunction final : public detail::Handle {
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
			SetBindDataInternal(ptr, detail::TypedCopy<T>, detail::TypedEquals<T>, detail::TypedDelete<T>);
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

		void SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
		                         void (*destructor)(void *));
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

} // namespace duckdb_api
