#pragma once

//----------------------------------------------------------------------------------------------------------------------
// 8888888b.                    888      8888888b.  888888b.               d8888 8888888b. 8888888
// 888  "Y88b                   888      888  "Y88b 888  "88b             d88888 888   Y88b  888
// 888    888                   888      888    888 888  .88P            d88P888 888    888  888
// 888    888 888  888  .d8888b 888  888 888    888 8888888K.           d88P 888 888   d88P  888
// 888    888 888  888 d88P"    888 .88P 888    888 888  "Y88b         d88P  888 8888888P"   888
// 888    888 888  888 888      888888K  888    888 888    888        d88P   888 888         888
// 888  .d88P Y88b 888 Y88b.    888 "88b 888  .d88P 888   d88P       d8888888888 888         888
// 8888888P"   "Y88888  "Y8888P 888  888 8888888P"  8888888P"       d88P     888 888       8888888
//----------------------------------------------------------------------------------------------------------------------

#include <functional>
#include <utility>
#include <tuple>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <optional>
#include <stdexcept>
#include <memory>

namespace duckdb {
namespace cxx {

//----------------------------------------------------------------------------------------------------------------------
// Common Typedefs and forward declarations
//----------------------------------------------------------------------------------------------------------------------

typedef uint64_t idx_t;

class Exception;
class DatabaseOption;
class Environment;
class Database;
class Connection;
class SqlStatement;
class StatementIterator;
class Schema;
class Signature;
class LogicalType;
class Value;
class Vector;
class StringHeap;
class DataChunk;
class QueryResult;

struct TypeParam;
struct NamedParam;

enum class LogicalTypeId : uint32_t;

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
	friend TYPE;

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

	// Returns the underlying C-API handle, indirectly typed via HandleTraits<TYPE>.
	// This is a member template so it is only instantiated where it is called
	// (in the .cpp, after the matching HandleTraits specialization is visible).
	// This keeps handle types out of this header.
	template <class TR = HandleTraits<TYPE>>
	auto handle() const -> typename TR::handle {
		return static_cast<typename TR::handle>(impl);
	}

	// True when this wrapper holds a live handle. Moved-from wrappers and
	// "no value" results (e.g. end-of-stream chunks) are empty.
	explicit operator bool() const noexcept {
		return impl != nullptr;
	}

private:
	Handle() : impl(nullptr) {
	}
	explicit Handle(void *impl) : impl(impl) {
	}

	// Detaches and returns the underlying handle, leaving the wrapper
	// empty; for transfer-semantics calls where the C API consumes the
	// handle.
	auto release() noexcept -> void * {
		auto *detached = impl;
		impl = nullptr;
		return detached;
	}
	void *impl;
};

struct Factory {
	template <class T, class... ARGS>
	static auto Make(ARGS &&... args) -> T {
		return T(std::forward<ARGS>(args)...);
	}

	template <class T>
	static auto Release(T &t) -> void {
		return t.release();
	}
};

} // namespace detail

//----------------------------------------------------------------------------------------------------------------------
// Exceptions
//----------------------------------------------------------------------------------------------------------------------

class Exception : public std::runtime_error {
public:
	// TODO: add more exception types!
	Exception(int code, const std::string &message, std::string raw_message = {})
	    : std::runtime_error(message), code(code), raw_message(std::move(raw_message)) {
	}

	auto GetCode() const -> int {
		return code;
	}

	// The message body with what()'s "<Type> Error: " prefix stripped, or empty.
	// Not derivable from what() (no type name here to rebuild the prefix); in the
	// engine's rendered form (location block, or JSON under errors_as_json).
	auto GetRawMessage() const -> const std::string & {
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
	UNKNOWN = 0,
	/* May only be written at GLOBAL (database) scope. */
	GLOBAL_ONLY = 1,
	/* May only be written at LOCAL (session) scope. */
	LOCAL_ONLY = 2,
	/* May be written at either scope; defaults to GLOBAL when unspecified. */
	GLOBAL_DEFAULT = 3,
	/* May be written at either scope; defaults to LOCAL when unspecified. */
	LOCAL_DEFAULT = 4,
};

// Scope of a connection-level option write. Mirrors DUCKDB_V2_SETTING_SCOPE
// numerically (parity pinned in the .cpp). Automatic resolves from the
// option's declared target scope, exactly like SQL `SET name = value`.
enum class SettingScope : uint8_t {
	AUTOMATIC = 0,
	GLOBAL = 1,
	LOCAL = 2,
};

class DatabaseOption final : public detail::Handle<DatabaseOption> {
	friend detail::Factory;

public:
	DatabaseOption(const std::string &name, const std::string &value);

	DatabaseOption(DatabaseOption &&) noexcept = default;
	DatabaseOption &operator=(DatabaseOption &&) noexcept = default;

	auto GetName() const -> std::string_view;
	auto GetValue() const -> std::string_view;
	auto GetDefaultValue() const -> std::string_view;
	auto GetDescription() const -> std::string_view;

	auto GetTargetScope() const -> OptionTargetScope;

	auto GetAliasCount() const -> size_t;
	auto GetAliasByIndex(size_t index) const -> std::string_view;

	~DatabaseOption() override;

private:
	explicit DatabaseOption(void *impl);
};

//----------------------------------------------------------------------------------------------------------------------
// Context
//----------------------------------------------------------------------------------------------------------------------

class Context final : public detail::Handle<Context> {
	friend detail::Factory;

public:
	~Context() override;

	// Parses a SQL type expression into an owned logical type: primitives,
	// parameterized kinds, and catalog-registered names alike. Usable
	// wherever a Context is live (function bind callbacks). From outside,
	// Connection::ParseType resolves the same directly from the connection.
	auto ParseType(std::string_view text) const -> LogicalType;

	// Builds a logical type from a catalog type name plus value parameters,
	// mirroring how SQL binds a type expression; registered extension types
	// construct through the same call. A TypeParam with an empty name is
	// positional. Connection::CreateType is the sugar.
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params = {}) const -> LogicalType;
	// The id-keyed twin: with no params this instantiates a primitive
	// directly; with params the id binds like its canonical name does.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params = {}) const -> LogicalType;

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

	auto GetOptionCount() const -> size_t;
	auto GetOptionByIndex(size_t index) const -> DatabaseOption;
	auto GetOption(std::string_view name) const -> DatabaseOption;
	auto SetOption(const DatabaseOption &option) -> void;
	auto SetOption(const DatabaseOption &option, SettingScope scope) -> void;

	// Parses a SQL string into an iterator over its statements. Parsing
	// only: no binding, no catalog access, no transaction.
	auto ParseSQL(const char *sql) -> StatementIterator;
	// Inline forwarder: keeps std::string out of the compiled interface.
	auto ParseSQL(const std::string &sql) -> StatementIterator {
		return ParseSQL(sql.c_str());
	}

	// Executes a parsed statement, returning a lazy streaming result; nothing runs
	// until the result is stepped. Borrowed (executed via a copy), not consumed, so
	// re-executable. parameters bind positionally ($1 = parameters[0]). Throws
	// RESOURCE_IN_USE while a live result exists.
	auto Execute(const SqlStatement &statement, const Value *parameters, idx_t parameter_count) -> QueryResult;
	// No-parameter convenience.
	auto Execute(const SqlStatement &statement) -> QueryResult;
	// std::vector convenience (defined inline below, once Value is complete, to keep
	// std::vector off the compiled boundary).
	auto Execute(const SqlStatement &statement, const std::vector<Value> &parameters) -> QueryResult;
	// Named-parameter convenience: each NamedParam binds its value to the named
	// parameter, or positionally when its name is empty.
	auto Execute(const SqlStatement &statement, const std::vector<NamedParam> &parameters) -> QueryResult;

	// Single-statement SQL convenience over ParseSQL + Execute: throws INVALID_INPUT
	// unless the input contains exactly one statement.
	auto Execute(const std::string &sql) -> QueryResult;

	// Binds a parsed statement without executing, returning its signature (output
	// schema of result columns, input schema of parameter types). Borrowed, not
	// consumed.
	auto Bind(const SqlStatement &statement) const -> Signature;

	// Connection-level counterpart to Context::ParseType: resolves the type
	// directly from the connection (its own transaction), no context scope needed.
	auto ParseType(std::string_view text) -> LogicalType;

	// Connection-level counterpart to Context::CreateType: resolves directly
	// from the connection (its own transaction), no context scope needed.
	auto CreateType(std::string_view name, const std::vector<TypeParam> &params = {}) -> LogicalType;
	// The id-keyed twin: with no params this instantiates a primitive
	// directly; with params the id binds like its canonical name does.
	auto CreateType(LogicalTypeId id, const std::vector<TypeParam> &params = {}) const -> LogicalType;

	// Requests cancellation of the active query. Safe to call from any
	// thread; a no-op when no query is active.
	auto Interrupt() -> void;

	// Reads the active query's progress. Safe to call from any thread.
	auto GetQueryProgress() const -> QueryProgress;

	static auto FromOpaque(void *opaque) -> Connection {
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
	Database(Database &&) noexcept = default;
	Database &operator=(Database &&) noexcept = default;

	auto GetOptionCount() const -> size_t;
	auto GetOptionByIndex(size_t index) const -> DatabaseOption;
	// By-name get: an alias resolves to its canonical option. Throws
	// INVALID_INPUT for an unknown name.
	auto GetOption(std::string_view name) const -> DatabaseOption;
	auto SetOption(const DatabaseOption &option) -> void;

	auto Connect() -> Connection;

private:
	explicit Database(void *impl);
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

private:
	explicit Extension(void *impl);
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

	auto GetOpenDatabaseCount() const -> size_t;

	auto Open(const std::string &path) -> Database;
	// Open with pre-open DBConfig options (access_mode, memory_limit, storage
	// options, ...). Options are borrowed; the engine copies what it needs.
	auto Open(const std::string &path, const std::vector<DatabaseOption> &options) -> Database;
};

// The version string of the linked DuckDB engine.
auto LibraryVersion() -> std::string;

//----------------------------------------------------------------------------------------------------------------------
// Logical Type
//----------------------------------------------------------------------------------------------------------------------

enum class LogicalTypeId : uint32_t {
	INVALID = 0,
	SQLNULL = 1,
	UNKNOWN = 2,
	ANY = 3,
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
	TUPLE = 110,
};

class LogicalType final : public detail::Handle<LogicalType> {
	friend detail::Factory;

public:
	LogicalType(LogicalType &&) noexcept = default;
	LogicalType &operator=(LogicalType &&) noexcept = default;

	~LogicalType() override;

	// A copy of this type carrying `alias` as its name. Relabels only: no
	// catalog lookup, so no context is involved.
	auto WithAlias(std::string_view alias) const -> LogicalType;

	// The type's name: the alias when set, otherwise the canonical fixed
	// name of the type id. Never empty; exactly the vocabulary CreateType
	// consumes. Borrowed; valid until this LogicalType is destroyed.
	auto GetName() const -> std::string_view;
	bool operator==(const LogicalType &other) const;
	bool operator!=(const LogicalType &other) const {
		return !(*this == other);
	}

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
	auto GetDecimalInternalTypeId() const -> LogicalTypeId;
	auto GetEnumInternalTypeId() const -> LogicalTypeId;

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
	auto GetFieldCount() const -> idx_t;
	// Borrowed field name, valid for this Schema's lifetime; empty for an absent name.
	auto GetFieldName(idx_t index) const -> std::string_view;
	// An owned copy of the field type.
	auto GetFieldType(idx_t index) const -> LogicalType;

private:
	explicit Schema(void *impl);
};

// A bound statement's signature: its output schema (result columns) and input schema (parameter types).
// Returned by Connection::Bind.
class Signature {
public:
	Schema output;
	Schema parameters;
};

//----------------------------------------------------------------------------------------------------------------------
// Base types
//----------------------------------------------------------------------------------------------------------------------

// A decoded BIGNUM: big-endian magnitude bytes plus a sign flag. The integer
// is (-1)**is_negative * unsigned_big_endian(magnitude). Owned bytes.
struct DecodedBignum {
	std::vector<uint8_t> magnitude;
	bool is_negative;
};

// A decoded UUID: the canonical 16 big-endian bytes. The storage is an int128
// with its high bit flipped for sort order; this is the real value.
struct DecodedUuid {
	uint8_t bytes[16];
};

struct int128_t {
	uint64_t lower;
	int64_t upper;
};

struct uint128_t {
	uint64_t lower;
	uint64_t upper;
};

struct date_t {
	int32_t days = 0;
};

struct dtime_t {
	int64_t micros = 0;
};

struct dtime_ns_t {
	int64_t nanos = 0;
};

// TIME_TZ packs the time-of-day into the high 40 bits and a biased UTC offset
// into the low 24 -- the committed layout, so the packed integer sorts. The
// offset is stored reverse-ordered (MAX_OFFSET - seconds), which is why it
// cannot be read out with a plain shift.
struct dtime_tz_t {
	static constexpr int OFFSET_BITS = 24;
	static constexpr uint64_t OFFSET_MASK = (uint64_t(1) << OFFSET_BITS) - 1;
	static constexpr int32_t MAX_OFFSET = 16 * 60 * 60 - 1; // +/-15:59:59

	dtime_tz_t() = default;
	// The packed form, as a vector slot holds it.
	explicit dtime_tz_t(uint64_t bits) : bits(bits) {
	}
	// Micros since midnight plus the UTC offset in seconds, positive east.
	dtime_tz_t(int64_t micros, int32_t offset_seconds)
	    : bits((static_cast<uint64_t>(micros) << OFFSET_BITS) |
	           (static_cast<uint64_t>(MAX_OFFSET - offset_seconds) & OFFSET_MASK)) {
	}

	auto GetMicros() const -> int64_t {
		return static_cast<int64_t>(bits >> OFFSET_BITS);
	}
	auto GetOffset() const -> int32_t {
		return MAX_OFFSET - static_cast<int32_t>(bits & OFFSET_MASK);
	}
	auto GetBits() const -> uint64_t {
		return bits;
	}

private:
	uint64_t bits = 0;
};

struct timestamp_t {
	int64_t micros = 0;
};

struct timestamp_s_t {
	int64_t seconds = 0;
};

struct timestamp_ms_t {
	int64_t millis = 0;
};

struct timestamp_ns_t {
	int64_t nanos = 0;
};

struct timestamp_tz_t {
	int64_t micros = 0;
};

struct timestamp_tz_ns_t {
	int64_t nanos = 0;
};

struct interval_t {
	int32_t months;
	int32_t days;
	int64_t micros;
};

struct list_entry_t {
	uint64_t offset = 0;
	uint64_t length = 0;
};

template <int8_t WIDTH, uint8_t SCALE>
struct decimal_t {
	static_assert(SCALE <= WIDTH, "DECIMAL scale must be less than or equal to width");
	static_assert(WIDTH >= 1 && WIDTH <= 38, "DECIMAL type width must be between 1 and 38");
	using storage_type = std::conditional_t<
	    (WIDTH <= 4), int16_t,
	    std::conditional_t<(WIDTH <= 9), int32_t, std::conditional_t<(WIDTH <= 18), int64_t, int128_t>>>;

	storage_type value;
};

struct blob_t {
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

	blob_t() {
		memset(&value, 0, sizeof(value));
	}

	// NOLINTNEXTLINE
	blob_t(std::string_view str) : blob_t(str.data(), static_cast<uint32_t>(str.size())) {
	}

	blob_t(const char *heap_data, uint32_t len) {
		if (len <= INLINE_LENGTH) {
			memset(&value, 0, sizeof(value));
			value.inlined.length = len;
			std::memcpy(value.inlined.inlined, heap_data, len);
		} else {
			memset(&value, 0, sizeof(value));
			value.pointer.length = len;
			value.pointer.ptr = const_cast<char *>(heap_data); // NOLINT
			std::memcpy(value.pointer.prefix, heap_data, PREFIX_LENGTH);
		}
	}

	explicit operator std::string_view() const {
		return std::string_view(Data(), Length());
	}

	explicit operator std::string() const {
		return std::string(Data(), Length());
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
};

struct varchar_t : blob_t {
	using blob_t::blob_t;
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
	auto ToText() const -> std::string;

	// Cast a value to another type
	auto Cast(const Context &ctx, const LogicalType &target) const -> Value;
	auto Cast(Connection &conn, const LogicalType &target) const -> Value;

	//! Accessors
	template <class T>
	auto Get() const -> T = delete;

	// DECIMAL cannot join the set above: Get<T> is a function template, and a
	// function template cannot be partially specialized, so decimal_t<W, S>
	// has no way in. This is the same spelling keyed on the parameters
	// instead -- value.Get<18, 3>() -- and it checks them against the value's
	// own type rather than reinterpreting the payload.
	template <int8_t WIDTH, uint8_t SCALE>
	auto Get() const -> decimal_t<WIDTH, SCALE> {
		int128_t value {};
		GetDecimal(value, WIDTH, SCALE);
		return decimal_t<WIDTH, SCALE> {static_cast<typename decimal_t<WIDTH, SCALE>::storage_type>(value.lower)};
	}

	//! Null Constructors
	static auto CreateNull(Connection &conn, const LogicalType &type) -> Value;
	static auto CreateNull(Context &conn, const LogicalType &type) -> Value;

	//! Types Constructors
	static auto Create(Connection &conn, bool value) -> Value;
	static auto Create(Connection &conn, uint8_t value) -> Value;
	static auto Create(Connection &conn, uint16_t value) -> Value;
	static auto Create(Connection &conn, uint32_t value) -> Value;
	static auto Create(Connection &conn, uint64_t value) -> Value;
	static auto Create(Connection &conn, uint128_t value) -> Value;
	static auto Create(Connection &conn, int8_t value) -> Value;
	static auto Create(Connection &conn, int16_t value) -> Value;
	static auto Create(Connection &conn, int32_t value) -> Value;
	static auto Create(Connection &conn, int64_t value) -> Value;
	static auto Create(Connection &conn, int128_t value) -> Value;
	static auto Create(Connection &conn, float value) -> Value;
	static auto Create(Connection &conn, double value) -> Value;
	static auto Create(Connection &conn, varchar_t value) -> Value;
	static auto Create(Connection &conn, blob_t value) -> Value;
	static auto Create(Connection &conn, const LogicalType &type) -> Value;
	static auto Create(Connection &conn, date_t value) -> Value;
	static auto Create(Connection &conn, dtime_t value) -> Value;
	static auto Create(Connection &conn, dtime_ns_t value) -> Value;
	static auto Create(Connection &conn, dtime_tz_t value) -> Value;
	static auto Create(Connection &conn, timestamp_t value) -> Value;
	static auto Create(Connection &conn, timestamp_s_t value) -> Value;
	static auto Create(Connection &conn, timestamp_ms_t value) -> Value;
	static auto Create(Connection &conn, timestamp_ns_t value) -> Value;
	static auto Create(Connection &conn, timestamp_tz_t value) -> Value;
	static auto Create(Connection &conn, timestamp_tz_ns_t value) -> Value;
	static auto Create(Connection &conn, interval_t value) -> Value;

	template <int8_t WIDTH, uint8_t SCALE>
	static auto Create(Connection &conn, decimal_t<WIDTH, SCALE> value) -> Value {
		return CreateDecimal(conn, WidenDecimal(value.value), WIDTH, SCALE);
	}

	template <class T>
	static auto Create(Connection &conn, T value) -> Value = delete;

	static auto Create(Context &ctx, bool value) -> Value;
	static auto Create(Context &ctx, uint8_t value) -> Value;
	static auto Create(Context &ctx, uint16_t value) -> Value;
	static auto Create(Context &ctx, uint32_t value) -> Value;
	static auto Create(Context &ctx, uint64_t value) -> Value;
	static auto Create(Context &ctx, uint128_t value) -> Value;
	static auto Create(Context &ctx, int8_t value) -> Value;
	static auto Create(Context &ctx, int16_t value) -> Value;
	static auto Create(Context &ctx, int32_t value) -> Value;
	static auto Create(Context &ctx, int64_t value) -> Value;
	static auto Create(Context &ctx, int128_t value) -> Value;
	static auto Create(Context &ctx, float value) -> Value;
	static auto Create(Context &ctx, double value) -> Value;
	static auto Create(Context &ctx, varchar_t value) -> Value;
	static auto Create(Context &ctx, blob_t value) -> Value;
	static auto Create(Context &ctx, const LogicalType &type) -> Value;
	static auto Create(Context &ctx, date_t value) -> Value;
	static auto Create(Context &ctx, dtime_t value) -> Value;
	static auto Create(Context &ctx, dtime_ns_t value) -> Value;
	static auto Create(Context &ctx, dtime_tz_t value) -> Value;
	static auto Create(Context &ctx, timestamp_t value) -> Value;
	static auto Create(Context &ctx, timestamp_s_t value) -> Value;
	static auto Create(Context &ctx, timestamp_ms_t value) -> Value;
	static auto Create(Context &ctx, timestamp_ns_t value) -> Value;
	static auto Create(Context &ctx, timestamp_tz_t value) -> Value;
	static auto Create(Context &ctx, timestamp_tz_ns_t value) -> Value;
	static auto Create(Context &ctx, interval_t value) -> Value;

	template <int8_t WIDTH, uint8_t SCALE>
	static auto Create(Context &ctx, decimal_t<WIDTH, SCALE> value) -> Value {
		return CreateDecimal(ctx, WidenDecimal(value.value), WIDTH, SCALE);
	}

	template <class T>
	static auto Create(Context &ctx, T value) -> Value = delete;

	// Composite construction. Each infers the composite type from its children,
	// so the built-in composites are the whole reach here: an aliased or
	// extension-registered composite is built through Cast, which is the
	// engine's own construction path for those kinds.
	//
	// Children are borrowed for the duration of the call. The composite holds
	// its own copies, so the inputs stay owned by the caller.
	//
	// Each comes in both scope forms, like Create and CreateNull: the Context
	// one for a live bind / execution context, the Connection one for outside.
	using ValueRef = std::reference_wrapper<const Value>;
	using ValueList = const std::vector<ValueRef> &;
	using NamedValueList = const std::vector<std::pair<std::string_view, ValueRef>> &;
	using KeyValueList = const std::vector<std::pair<ValueRef, ValueRef>> &;

	// A LIST of the elements. The child type is the first element's; the rest are cast to it, so a set that does not
	// share a type surfaces the engine's cast error rather than widening silently.
	// An empty element list has no child type to infer and throws; build an empty LIST through the child-type overload
	// instead.
	static auto CreateList(Connection &conn, ValueList values) -> Value;
	static auto CreateList(Context &ctx, ValueList values) -> Value;

	// An empty LIST of `child_type`. Takes the element type, not the LIST type.
	static auto CreateList(Connection &conn, const LogicalType &child_type) -> Value;
	static auto CreateList(Context &ctx, const LogicalType &child_type) -> Value;

	// An ARRAY of the elements, sized by how many there are, with the same first-element child type as CreateList.
	// The engine's minimum array size is 1, so there is no empty ARRAY to build and an empty element list throws.
	static auto CreateArray(Connection &conn, ValueList values) -> Value;
	static auto CreateArray(Context &ctx, ValueList values) -> Value;

	// A TUPLE is an unnamed struct. The empty tuple is a real type too, so an empty field list is valid.
	static auto CreateTuple(Connection &conn, ValueList values = {}) -> Value;
	static auto CreateTuple(Context &ctx, ValueList values = {}) -> Value;

	// A STRUCT of the named fields, in order. The empty struct is a real type, so an empty field list is valid.
	static auto CreateStruct(Connection &conn, NamedValueList = {}) -> Value;
	static auto CreateStruct(Context &ctx, NamedValueList values = {}) -> Value;

	// A MAP of the key, value pairs, keyed and valued by the first pair's types, with the rest cast to them.
	// Keys must be unique and non-NULL, the engine enforces both.
	// An empty pair list has no types to infer and throws.
	// Build an empty MAP through the key/value-type overload instead.
	static auto CreateMap(Connection &conn, KeyValueList values) -> Value;
	static auto CreateMap(Context &ctx, KeyValueList values) -> Value;

	// An empty MAP of `key_type` and `value_type`. Takes the key and value types, not the MAP type.
	static auto CreateMap(Connection &conn, const LogicalType &key_type, const LogicalType &value_type) -> Value;
	static auto CreateMap(Context &ctx, const LogicalType &key_type, const LogicalType &value_type) -> Value;

	//! Composite values
	auto GetChildCount() const -> idx_t;
	auto GetChild(idx_t index) const -> Value;

	auto operator[](idx_t index) const -> Value {
		return GetChild(index);
	}

private:
	// Reads the backing integer, and throws unless the value's own width and
	// scale are the ones asked for: a DECIMAL(9, 2) is not a DECIMAL(18, 3),
	// and the payloads are not interchangeable across storage tiers.
	void GetDecimal(int128_t &out, uint8_t width, uint8_t scale) const;

	// The runtime forwarder behind the templated DECIMAL constructors; keeping
	// it here is what lets those be defined in the header without naming a C
	// type.
	static auto CreateDecimal(Connection &conn, int128_t value, uint8_t width, uint8_t scale) -> Value;
	static auto CreateDecimal(Context &ctx, int128_t value, uint8_t width, uint8_t scale) -> Value;

	// Sign-extends a DECIMAL's backing integer to the widest storage tier, so one
	// entry point can carry every tier without the header naming a C type per tier.
	static auto WidenDecimal(int64_t value) -> int128_t {
		return int128_t {static_cast<uint64_t>(value), value < 0 ? -1 : 0};
	}
	static auto WidenDecimal(int128_t value) -> int128_t {
		return value;
	}

	explicit Value(void *impl);
};

template <>
auto Value::Get() const -> bool;
template <>
auto Value::Get() const -> uint8_t;
template <>
auto Value::Get() const -> uint16_t;
template <>
auto Value::Get() const -> uint32_t;
template <>
auto Value::Get() const -> uint64_t;
template <>
auto Value::Get() const -> uint128_t;
template <>
auto Value::Get() const -> int8_t;
template <>
auto Value::Get() const -> int16_t;
template <>
auto Value::Get() const -> int32_t;
template <>
auto Value::Get() const -> int64_t;
template <>
auto Value::Get() const -> int128_t;
template <>
auto Value::Get() const -> float;
template <>
auto Value::Get() const -> double;
template <>
auto Value::Get() const -> varchar_t;
template <>
auto Value::Get() const -> blob_t;
template <>
auto Value::Get() const -> date_t;
template <>
auto Value::Get() const -> dtime_t;
template <>
auto Value::Get() const -> dtime_ns_t;
template <>
auto Value::Get() const -> dtime_tz_t;
template <>
auto Value::Get() const -> timestamp_t;
template <>
auto Value::Get() const -> timestamp_s_t;
template <>
auto Value::Get() const -> timestamp_ms_t;
template <>
auto Value::Get() const -> timestamp_ns_t;
template <>
auto Value::Get() const -> timestamp_tz_t;
template <>
auto Value::Get() const -> timestamp_tz_ns_t;
template <>
auto Value::Get() const -> interval_t;
template <>
auto Value::Get() const -> LogicalType;

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
// String Heap
//----------------------------------------------------------------------------------------------------------------------

// Borrowed handle to a vector's string heap. Reserves vector-lifetime bytes and
// returns BytesLayout tokens to place in any order (dedup, scatter). Borrowed;
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
	// `data` exceeds the uint32 length a duckdb_v2_bytes can hold.
	auto Add(std::string_view data) -> blob_t {
		if (data.size() > std::numeric_limits<uint32_t>::max()) {
			ThrowStringTooLong(data.size());
		}
		const auto size = static_cast<uint32_t>(data.size());
		if (size <= blob_t::INLINE_LENGTH) {
			return blob_t(data.data(), size);
		}
		auto *bytes = Allocate(size);
		std::memcpy(bytes, data.data(), size);
		return blob_t(reinterpret_cast<char *>(bytes), size);
	}

	// Bulk Add: interns every view, returning the tokens in order.
	auto AddMany(const std::vector<std::string_view> &data) -> std::vector<blob_t> {
		std::vector<blob_t> out;
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
	OTHER = 0,
	FLAT = 1,
	CONSTANT = 2,
	DICTIONARY = 3,
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

	auto Flatten() const -> void;

	auto GetSize() const -> idx_t;
	auto SetSize(idx_t size) -> void;

	// ---- Vector read surface ----

	// Reads the vector as a VectorView in one boundary crossing. Throws
	// INVALID_INPUT on VectorType::OTHER; Flatten() first. On DICTIONARY
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
	// ... for `count` rows. Reads as VectorType::OTHER; Flatten() before
	// reading via GetView().
	auto MakeSequence(int64_t start, int64_t increment, idx_t count) -> void;

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
	auto SetString(idx_t index, varchar_t value) -> void;

private:
	explicit Vector(void *impl);

	// Throws INVALID_INPUT if [start, start+count) is not writable: a CONSTANT
	// vector's data array holds a single slot, so only index 0 may be written.
	auto CheckWriteRange(idx_t start, idx_t count) const -> void;

	// Committed layout constants, shared by each Decode / Encode pair: TIME_TZ
	// packs its offset into the low 24 bits, UUID flips the sign bit so the
	// signed storage sorts like the unsigned value.
	static constexpr uint64_t TIME_TZ_OFFSET_MASK = ~uint64_t(0) >> 40;
	static constexpr uint64_t UUID_SORT_BIT = uint64_t(1) << 63;
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

private:
	explicit DataChunk(void *impl, bool owned);
	bool owned = false; // UGLY, this should probably be done c++-side.
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
inline auto Connection::Execute(const SqlStatement &statement, const std::vector<Value> &parameters) -> QueryResult {
	return Execute(statement, parameters.data(), parameters.size());
}

} // namespace cxx
} // namespace duckdb
