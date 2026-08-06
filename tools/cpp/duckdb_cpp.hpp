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
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <optional>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <cassert>
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
class ValueFactory;
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

	// Creates values of the built-in types through this context. Stateless and
	// returned by value, so calling it inline costs nothing over binding it.
	auto ValueFactory() const -> class ValueFactory;

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

	// Connection-level counterpart to Context::ValueFactory: casts run in the
	// connection's own transaction, no context scope needed.
	auto ValueFactory() -> class ValueFactory;

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

// Logical type identifier. Mirrors the C API's LOGICAL_TYPE_ID (and thereby
// duckdb::LogicalTypeId) numerically; parity pinned by static_asserts in the
// implementation.
enum class LogicalTypeId : uint32_t {
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

	auto GetId() const -> LogicalTypeId;

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

	// Shared gate for the per-kind sugar: throws INVALID_INPUT unless this
	// type's id is `expected`.
	auto RequireKind(LogicalTypeId expected, const char *what) const -> void;
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

// The committed backing integer of a DECIMAL of the given width: width <= 4
// int16, <= 9 int32, <= 18 int64, <= 38 int128 (boundaries inclusive). The
// compile-time dual of LogicalType::GetDecimalInternalTypeId, and what
// ValueFactory::CreateDecimal takes as its payload.
//
// Written as a struct rather than a bare alias so the width check fires here,
// when the alias is instantiated. As a plain alias an out-of-range width picks
// the widest tier and the error surfaces as a mismatched argument type instead.
template <uint8_t WIDTH>
struct DecimalStorageTraits {
	// The same bounds as duckdb::Decimal::IsValid; 38 is MAX_WIDTH_DECIMAL.
	static_assert(WIDTH >= 1 && WIDTH <= 38, "DECIMAL type width must be between 1 and 38");

	using type = typename std::conditional<
	    (WIDTH <= 4), int16_t,
	    typename std::conditional<(WIDTH <= 9), int32_t,
	                              typename std::conditional<(WIDTH <= 18), int64_t, HugeintLayout>::type>::type>::type;
};

template <uint8_t WIDTH>
using DecimalStorage = typename DecimalStorageTraits<WIDTH>::type;

// Values are read here and created through ValueFactory: the constructors are
// private because a value needs a LogicalType, and a logical type needs the
// catalog behind a Context or Connection. Reach them through
// Context::ValueFactory / Connection::ValueFactory.
class Value final : public detail::Handle<Value> {
	friend detail::Factory;
	friend class ValueFactory;

public:
	~Value() override;

	Value(Value &&) noexcept = default;
	Value &operator=(Value &&) noexcept = default;

	auto IsNull() const -> bool;
	auto GetLogicalType() const -> LogicalType;
	auto ToString() const -> std::string;


	// Cast a value to another type
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
	// VectorView. Borrows the payload in exactly the committed physical layout
	// (fixed layout for fixed kinds; decoded wire bytes for VARCHAR / BLOB /
	// BIT / BIGNUM); dispatch on GetLogicalType and cast, e.g. a DECIMAL reads
	// its backing integer plus GetDecimalScale. Throws INVALID_INPUT on a NULL
	// value. This is the complete read path; the As* below are gated
	// conveniences over it for the everyday types, and
	// ValueFactory::CreateFromData is its inverse.
	auto GetData() const -> std::pair<const void *, idx_t>; // borrowed, lifetime = this value

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

private:
	explicit Value(void *impl);

	// The construction surface, reached through ValueFactory. See the matching
	// CreateNull / CreateTypeValue / Create / CreateFromData there.
	static auto Null(const LogicalType &type) -> Value;
	static auto Type(const LogicalType &type) -> Value;
	static auto Create(const LogicalType &type, std::vector<Value> children) -> Value;
	static auto FromData(const LogicalType &type, const void *data, idx_t length) -> Value;
};

//----------------------------------------------------------------------------------------------------------------------
// Value Factory
//----------------------------------------------------------------------------------------------------------------------

// Creates values of the built-in types, bound to whichever of Context /
// Connection supplies the catalog.
//
// A C++ type cannot name a logical type: int64_t backs BIGINT, every TIME and
// TIMESTAMP variant, and more; bytes back VARCHAR, BLOB, BIT and BIGNUM. So
// the method name carries the type, not the argument -- Bigint, Timestamp and
// TimeNs are three functions over one int64_t payload. Payloads are in each
// kind's committed physical layout, making these the exact duals of the
// matching Value::As* getters.
//
// Deliberately a closed set over the built-ins: the logical type space is open
// (extensions register their own), so no fixed list can ever cover it. Anything
// outside this set -- extension types, UNION, ENUM, VARIANT -- is reached
// through Cast, which is the engine's own construction path for those kinds:
// build the nearest built-in, usually a CreateVarchar, and cast it.
class ValueFactory {
public:
	// The primary form takes a live Context (function bind callbacks); the
	// Connection form is for outside, and runs its casts in the connection's
	// own transaction.
	explicit ValueFactory(const Context &context);
	explicit ValueFactory(Connection &connection);

	auto CreateBoolean(bool value) const -> Value;

	auto CreateTinyInt(int8_t value) const -> Value;
	auto CreateSmallInt(int16_t value) const -> Value;
	auto CreateInteger(int32_t value) const -> Value;
	auto CreateBigInt(int64_t value) const -> Value;
	auto CreateHugeInt(HugeintLayout value) const -> Value;

	auto CreateUTinyInt(uint8_t value) const -> Value;
	auto CreateUSmallInt(uint16_t value) const -> Value;
	auto CreateUInteger(uint32_t value) const -> Value;
	auto CreateUBigInt(uint64_t value) const -> Value;
	auto CreateUHugeInt(UhugeintLayout value) const -> Value;

	auto CreateFloat(float value) const -> Value;
	auto CreateDouble(double value) const -> Value;

	// Temporal payloads are storage units, matching the As* getters: days for
	// DATE, and each type's native unit for the rest.
	auto CreateDate(int32_t days) const -> Value;
	auto CreateTime(int64_t micros) const -> Value;
	auto CreateTimeNs(int64_t nanos) const -> Value;
	auto CreateTimeTz(DecodedTimeTz value) const -> Value;
	auto CreateTimestampSec(int64_t seconds) const -> Value;
	auto CreateTimestampMs(int64_t millis) const -> Value;
	auto CreateTimestamp(int64_t micros) const -> Value;
	auto CreateTimestampNs(int64_t nanos) const -> Value;
	auto CreateTimestampTz(int64_t micros) const -> Value;
	auto CreateTimestampTzNs(int64_t nanos) const -> Value;
	auto CreateInterval(IntervalLayout value) const -> Value;

	auto CreateVarchar(std::string_view value) const -> Value;
	auto CreateBlob(const void *data, idx_t length) const -> Value;
	// BIT from its data bytes plus the count of leading bits in the first byte
	// that are not part of the bit string; the storage header is assembled here.
	auto CreateBit(const void *data, idx_t length, uint8_t padding_bits) const -> Value;
	// Canonical UUID bytes; the storage's sort-order bit flip is applied here.
	auto CreateUuid(DecodedUuid value) const -> Value;
	// Big-endian magnitude plus a sign flag, encoded to BIGNUM storage.
	auto CreateBignum(const DecodedBignum &value) const -> Value;

	// A DECIMAL of the given width and scale, from its backing integer: the
	// value scaled by 10^SCALE, so CreateDecimal<18, 3>(18500) is 18.500.
	//
	// The payload type follows from WIDTH alone -- int16, int32, int64 or
	// HugeintLayout per the storage tiers -- so the payload is always the width
	// the type expects, and a bad width or scale is a compile error. The value
	// itself is not range-checked against the width: like the rest of the
	// layout-raw surface, that is a cast's job.
	//
	// Only the scale is asserted here. The width is checked by
	// DecimalStorageTraits, which the parameter type instantiates, so an
	// out-of-range width fails before this body is ever reached; repeating it
	// here would be unreachable. Together they are duckdb::Decimal::IsValid.
	template <uint8_t WIDTH, uint8_t SCALE>
	auto CreateDecimal(DecimalStorage<WIDTH> value) const -> Value {
		static_assert(SCALE <= WIDTH, "DECIMAL type scale cannot be greater than width");
		return CreateFromData(GetDecimalType(WIDTH, SCALE), &value, sizeof(value));
	}

	// The generic leaf constructor, and the inverse of Value::GetData: `data`
	// holds the payload in exactly the target type's committed physical layout.
	// The complete path out of the named helpers above, and the way to reach a
	// kind they do not cover -- a DECIMAL's scaled integer, an ENUM's dictionary
	// index -- without going through a cast.
	auto CreateFromData(const LogicalType &type, const void *data, idx_t length) const -> Value;

	// A NULL of any type, built-in or not.
	auto CreateNull(const LogicalType &type) const -> Value;
	// A type carried as a value, e.g. a TypeParam's payload. Named for what it
	// produces: Context::CreateType makes a LogicalType, this makes a Value.
	auto CreateTypeValue(const LogicalType &type) const -> Value;

	// Composite construction. Each takes the composite type itself, not its
	// child type, so an aliased or extension-registered composite keeps its
	// identity. Children are cast to the declared child or field types, and
	// arity is checked here rather than surfacing from the engine.
	auto CreateStruct(const LogicalType &type, std::vector<Value> fields) const -> Value;
	// TUPLE is the unnamed struct: same positional children, distinct type id.
	auto CreateTuple(const LogicalType &type, std::vector<Value> fields) const -> Value;
	auto CreateList(const LogicalType &type, std::vector<Value> elements) const -> Value;
	auto CreateArray(const LogicalType &type, std::vector<Value> elements) const -> Value;
	// Keys and values as parallel vectors; the flat alternating child list the
	// engine wants is assembled here.
	auto CreateMap(const LogicalType &type, std::vector<Value> keys, std::vector<Value> values) const -> Value;

	// The unchecked generic form the four above are built on. See Value::Create
	// for the per-kind child layout.
	auto Create(const LogicalType &type, std::vector<Value> children) const -> Value;

	// The general path out of the built-in set. Non-strict, and registered
	// custom casts apply, so this reaches extension types, and is how UNION,
	// ENUM and VARIANT values are built.
	auto Cast(const Value &value, const LogicalType &target) const -> Value;

	// The logical type behind a built-in kind, for the surfaces that want the
	// type rather than a value. Owned, so it costs a round trip per call; the
	// engine is where any reuse of the built-in types belongs.
	auto GetType(LogicalTypeId id) const -> LogicalType;
	// DECIMAL takes its width and scale as type parameters, so it needs its own
	// accessor. The runtime counterpart to CreateDecimal's template arguments,
	// for when the width and scale are not known until run time.
	auto GetDecimalType(uint8_t width, uint8_t scale) const -> LogicalType;

private:
	// Exactly one is set, from whichever constructor ran. The factory holds no
	// other state, which is what makes it free to construct inline.
	const Context *context = nullptr;
	Connection *connection = nullptr;

	auto Leaf(LogicalTypeId id, const void *data, idx_t length) const -> Value;
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
// String Heap
//----------------------------------------------------------------------------------------------------------------------

// Transparent mirror of the C ABI's duckdb_v2_bytes (same layout as
// duckdb::string_t): 16-byte storage for VARCHAR / BLOB / BIT / BIGNUM, inlined
// when length <= INLINE_LENGTH. A non-inlined value is valid only in a slot of
// the vector whose heap produced it. Aggregate, so it writes straight into a
// slot; layout pinned by static_assert in the .cpp.
struct BytesLayout {
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
	static auto Inlined(const char *data, uint32_t len) -> BytesLayout {
		assert(len <= INLINE_LENGTH);
		BytesLayout storage {};
		storage.value.inlined.length = len;
		if (len > 0) {
			std::memcpy(storage.value.inlined.inlined, data, len);
		}
		return storage;
	}

	// Non-inlined token over `len` bytes at `heap_data` (from Allocate); sets the
	// prefix. `len` must exceed INLINE_LENGTH, else it would read as inlined.
	static auto FromHeapData(char *heap_data, uint32_t len) -> BytesLayout {
		assert(len > INLINE_LENGTH);
		BytesLayout storage {};
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
	auto Add(std::string_view data) -> BytesLayout {
		if (data.size() > UINT32_MAX) {
			ThrowStringTooLong(data.size());
		}
		if (data.size() <= BytesLayout::INLINE_LENGTH) {
			return BytesLayout::Inlined(data.data(), static_cast<uint32_t>(data.size()));
		}
		auto len = static_cast<uint32_t>(data.size());
		auto *bytes = Allocate(len);
		std::memcpy(bytes, data.data(), len);
		return BytesLayout::FromHeapData(reinterpret_cast<char *>(bytes), len);
	}

	// Bulk Add: interns every view, returning the tokens in order.
	auto AddMany(const std::vector<std::string_view> &data) -> std::vector<BytesLayout> {
		std::vector<BytesLayout> out;
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
	// as BytesLayout): byte 0 is the padding-bit count, bytes 1.. are the data.
	// Borrowed pointers; lifetime = the owning chunk. Inline pointer arithmetic,
	// no allocation or ABI crossing, so it inlines into a per-row vector loop.
	static auto DecodeBit(const BytesLayout &value) -> DecodedBit {
		const auto len = value.Length();
		const auto *bytes = reinterpret_cast<const uint8_t *>(value.Data());
		return DecodedBit {len > 0 ? bytes + 1 : bytes, len > 0 ? len - 1 : 0, len > 0 ? bytes[0] : uint8_t(0)};
	}

	// Decodes one BIGNUM storage value into an owned magnitude + sign.
	static auto DecodeBignum(const BytesLayout &value) -> DecodedBignum;

	// Unpacks one TIME_TZ slot (read as uint64) into micros + UTC offset.
	// Defined inline: a pure-arithmetic decode of the committed layout with no
	// allocation and no ABI crossing, so it inlines into a per-row vector loop.
	static auto DecodeTimeTz(uint64_t packed) -> DecodedTimeTz {
		constexpr int32_t MAX_OFFSET = 16 * 60 * 60 - 1;
		return DecodedTimeTz {static_cast<int64_t>(packed >> 24),
		                      MAX_OFFSET - static_cast<int32_t>(packed & TIME_TZ_OFFSET_MASK)};
	}

	// Undoes the sort-order high-bit flip on one UUID slot (read as
	// HugeintLayout) and returns the canonical 16 big-endian bytes. Inline for
	// the same reason as DecodeTimeTz.
	static auto DecodeUuid(HugeintLayout internal) -> DecodedUuid {
		const uint64_t upper = static_cast<uint64_t>(internal.upper) ^ UUID_SORT_BIT;
		DecodedUuid out {};
		for (int i = 0; i < 8; i++) {
			out.bytes[i] = static_cast<uint8_t>((upper >> (56 - 8 * i)) & 0xFF);
			out.bytes[8 + i] = static_cast<uint8_t>((internal.lower >> (56 - 8 * i)) & 0xFF);
		}
		return out;
	}

	// ---- End vector read surface ----

	// ---- Vector write surface: the encoders the decoders above undo ----

	// Packs micros + UTC offset into one TIME_TZ slot. Inverse of DecodeTimeTz.
	static auto EncodeTimeTz(DecodedTimeTz value) -> uint64_t {
		constexpr int32_t MAX_OFFSET = 16 * 60 * 60 - 1;
		return (static_cast<uint64_t>(value.micros) << 24) |
		       (static_cast<uint64_t>(MAX_OFFSET - value.offset_seconds) & TIME_TZ_OFFSET_MASK);
	}

	// Applies the sort-order high-bit flip to canonical UUID bytes, producing
	// the internal storage form. Inverse of DecodeUuid.
	static auto EncodeUuid(DecodedUuid value) -> HugeintLayout {
		uint64_t upper = 0;
		uint64_t lower = 0;
		for (int i = 0; i < 8; i++) {
			upper = (upper << 8) | value.bytes[i];
			lower = (lower << 8) | value.bytes[8 + i];
		}
		return HugeintLayout {lower, static_cast<int64_t>(upper ^ UUID_SORT_BIT)};
	}

	// Encodes a magnitude + sign into BIGNUM storage bytes. Inverse of
	// DecodeBignum; the magnitude must be canonical (at least one byte, no
	// leading zeroes; zero is a single 0x00 with is_negative false).
	static auto EncodeBignum(const DecodedBignum &value) -> std::vector<uint8_t>;

	// ---- End vector write surface ----

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
	auto SetString(idx_t index, BytesLayout value) -> void;

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
