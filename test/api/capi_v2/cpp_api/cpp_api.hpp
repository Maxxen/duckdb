#pragma once

#include <functional>
#include <utility>
#include <string>

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
	~LogicalType() override;

	static LogicalType VARCHAR();
	static LogicalType INTEGER();

	std::string_view GetAlias() const;

private:
	explicit LogicalType(void *impl);
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

private:
	explicit Vector(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// DataChunk
//----------------------------------------------------------------------------------------------------------------------
class DataChunk final : public detail::Handle {
	friend detail::Factory;

public:
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
	class BindArgs;

	class BindInput {
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

		explicit BindInput(BindArgs &args) : args(args) {
		}

	private:
		BindArgs &args;

		void SetBindDataInternal(void *data, void *(*copy)(void *), bool (*equals)(void *a, void *b),
		                         void (*destructor)(void *));
		void *GetBindDataInternal() const;
	};

	class InitArgs;

	class InitInput {
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

		explicit InitInput(InitArgs &args) : args(args) {
		}

	private:
		InitArgs &args;

		void SetWorkerStateInternal(void *data, void (*destructor)(void *));
		void *GetWorkerStateInternal() const;
		void *GetBindDataInternal() const;
	};

	class ExecArgs;

	class ExecInput {
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

		explicit ExecInput(ExecArgs &args) : args(args) {
		}

	private:
		ExecArgs &args;

		void *GetBindDataInternal() const;
		void *GetWorkerStateInternal() const;
	};
};

} // namespace duckdb_api
