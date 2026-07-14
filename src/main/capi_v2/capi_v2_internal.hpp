//===----------------------------------------------------------------------===//
//                         DuckDB
//
// capi_v2_internal.hpp
//
// Internal header for V2 C API bridge implementations.
//
//===----------------------------------------------------------------------===//

#pragma once

// DuckDB C++ internals (also pulls in duckdb.h which defines idx_t, etc.)
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/main/db_instance_cache.hpp"

// DuckDB internals used by the option set/get bridge.
#include "duckdb/main/setting_info.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"

// V2 C API header -- all types use duckdb_v2_ prefix, no collision with V1.
#include "duckdb_v2.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include <deque>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

// ABI guard: the bridge reinterpret_casts duckdb_v2_string <-> duckdb::string_t,
// so the layouts must match. sizeof/alignof tie them together; the offsetof
// checks pin duckdb_v2_string's field offsets (string_t's union is private, so
// offsetof can't reach into it).
static_assert(sizeof(duckdb_v2_string) == sizeof(duckdb::string_t),
              "duckdb_v2_string must match the size of duckdb::string_t");
static_assert(alignof(duckdb_v2_string) == alignof(duckdb::string_t),
              "duckdb_v2_string must match the alignment of duckdb::string_t");
static_assert(offsetof(duckdb_v2_string, value.pointer.length) == 0,
              "duckdb_v2_string value.pointer.length must be at offset 0");
static_assert(offsetof(duckdb_v2_string, value.pointer.prefix) == 4,
              "duckdb_v2_string value.pointer.prefix must be at offset 4");
static_assert(offsetof(duckdb_v2_string, value.pointer.ptr) == 8,
              "duckdb_v2_string value.pointer.ptr must be at offset 8");
static_assert(offsetof(duckdb_v2_string, value.inlined.inlined) == 4,
              "duckdb_v2_string value.inlined.inlined must be at offset 4");

namespace duckdb {

// Forward declarations.
struct EnvironmentWrapperV2;
struct DatabaseWrapperV2;
class StringHeap;

// Backing struct for the opaque duckdb_v2_environment_handle handle. Owns
// the DBInstanceCache used to share the path manager across all
// databases opened through it (so the same file opened twice is
// rejected with RESOURCE_IN_USE). Cache behavior at open time is
// NEVER_CACHE: the cache participates in path-manager coordination but
// never memoizes DatabaseInstances; every open produces a fresh handle.
//
// open_database_count is incremented at open time and decremented in
// duckdb_v2_close. duckdb_v2_destroy_environment refuses while the
// counter is > 0 (the only handle in V2 with strict refuse-on-destroy
// semantics).
struct EnvironmentWrapperV2 {
	unique_ptr<DBInstanceCache> cache;
	std::atomic<uint64_t> open_database_count {0};
};

struct DatabaseWrapperV2 {
	// Back-pointer used by duckdb_v2_close to decrement the
	// environment's open-database counter. Guaranteed valid for the
	// database's lifetime: destroy_environment refuses while this
	// database is still alive, so the env outlives the db.
	EnvironmentWrapperV2 *environment = nullptr;
	shared_ptr<DuckDB> database;
	// Internal connection used by database-scope option reads to drive
	// the legacy get_setting cascade (which requires a ClientContext).
	// Created at open and destroyed with the wrapper.
	unique_ptr<Connection> admin_connection;
};

// A connection runs one result at a time: while a live, non-terminal
// result exists, statement_execute refuses with RESOURCE_IN_USE. The busy
// check is bridge-side handle-liveness bookkeeping, deliberately not an
// engine-state inspection (the engine's active_query slot is private and
// cleared lazily, and "open transaction" is the wrong predicate under
// explicit BEGIN). The slot stores the owning ResultWrapperV2 so release
// is idempotent and only the current owner can free it.
//
// It lives in the ClientContext's registered-state map (one per
// connection, keyed by kBusySlotStateKey) rather than on the connection
// handle, so the V2 connection handle can stay a bare Connection * that is
// bit-identical to V1's duckdb_connection. Being a shared_ptr in that map,
// it outlives the connection if a result still holds it, and outlives a
// drained result because the map keeps it: either side can outlive the
// other.
struct ConnectionBusySlotV2 : public ClientContextState {
	std::atomic<void *> owner {nullptr};
	// True once the consumer called connection_interrupt for the active result.
	// This flag is how we distinguish a consumer cancellation (-> CANCELLED
	// status) from an engine-initiated interrupt that shares the INTERRUPT
	// exception type, e.g. a max_execution_time timeout (-> error). The engine's
	// own interrupt_state is not usable for this: the executor sets it to stop
	// sibling tasks on any error. Reset when a new query claims the slot.
	std::atomic<bool> cancel_requested {false};
};

inline constexpr const char *kBusySlotStateKey = "v2_connection_busy_slot";

inline shared_ptr<ConnectionBusySlotV2> GetBusySlot(ClientContext &context) {
	return context.registered_state->GetOrCreate<ConnectionBusySlotV2>(kBusySlotStateKey);
}

// Backing struct for the opaque duckdb_v2_query_progress_handle: an
// immutable snapshot of duckdb::QueryProgress, captured by
// connection_query_progress at call time (the live fields are atomics;
// the handle decouples readers from them).
struct QueryProgressWrapperV2 {
	double percentage = -1;
	uint64_t rows_processed = 0;
	uint64_t total_rows_to_process = 0;
};

// Backing struct for the opaque duckdb_v2_option_handle handle. Owns all
// strings; borrowed pointers returned by accessors are valid until the
// option is destroyed. Created via duckdb_v2_option_create with just
// (name, setting); metadata fields (description, default setting,
// target scope, aliases) are populated by the database/connection get
// paths.
struct OptionWrapperV2 {
	std::string name;
	std::string setting;
	std::string default_setting;
	std::string description;
	DUCKDB_V2_OPTION_TARGET_SCOPE target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	std::vector<std::string> aliases;
};

// V2-registered functions hold raw callback pointers, so their plans cannot
// serialize. The throwing serialize callbacks built on this also keep the
// debug verifier's deserialize step from re-running user binds on already
// mutated arguments (function_serialization.hpp rebinds when a function has
// no serialization callbacks).
[[noreturn]] inline void ThrowFunctionNotSerializable(const Identifier &name) {
	throw NotImplementedException("Function \"%s\" is registered through the C API and does not support serialization",
	                              name.GetIdentifierName());
}

// duckdb_v2_str conversions. A duckdb_v2_str is a borrowed,
// length-delimited view: not null-terminated, may contain interior null
// bytes. {NULL, 0} is the canonical empty view.
inline std::string ToString(duckdb_v2_str str) {
	return str.ptr ? std::string(str.ptr, str.len) : std::string();
}
inline Identifier ToIdentifier(duckdb_v2_str str) {
	return str.ptr ? Identifier(str.ptr, str.len) : Identifier();
}
// Borrow a std::string as a view. Valid only as long as the string is
// alive and unmodified.
inline duckdb_v2_str ToStr(const std::string &str) {
	duckdb_v2_str result;
	result.ptr = str.data();
	result.len = str.size();
	return result;
}
// Borrow a string_t's bytes as a view. Valid only as long as the
// backing storage (chunk / value) is alive.
inline duckdb_v2_str ToStr(const string_t &str) {
	duckdb_v2_str result;
	result.ptr = str.GetData();
	result.len = str.GetSize();
	return result;
}
inline duckdb_v2_str ToStr(const duckdb::Identifier &ident) {
	duckdb_v2_str result;
	result.ptr = ident.c_str();
	result.len = ident.size();
	return result;
}

// Opaque-handle casts used across the bridge. Inline so the unity build
// doesn't see duplicate definitions if two TUs are concatenated.
inline EnvironmentWrapperV2 *ToEnv(duckdb_v2_environment_handle ptr) {
	return reinterpret_cast<EnvironmentWrapperV2 *>(ptr);
}
inline DatabaseWrapperV2 *ToDb(duckdb_v2_database_handle ptr) {
	return reinterpret_cast<DatabaseWrapperV2 *>(ptr);
}
// The connection handle is not wrapped: a duckdb_v2_connection is a bare
// heap-allocated duckdb::Connection, bit-identical to V1's duckdb_connection
// (a Connection *). This lets the two handle types be cast to each other for
// operations (not for construction/destruction: a handle must be destroyed by
// the API family that created it). The one piece of per-connection bridge state
// (the busy slot) lives in the ClientContext's registered-state map, not here.
inline Connection *ToConn(duckdb_v2_connection_handle ptr) {
	return reinterpret_cast<Connection *>(ptr);
}
// duckdb_v2_context_handle is a borrowed duckdb::ClientContext, handed out only
// inside library-managed scopes (connection_execute_with_context, function
// bind callbacks, custom type registration), where the context lock is held
// and a transaction is active. The bridge never owns or destroys it.
inline ClientContext *ToContext(duckdb_v2_context_handle ptr) {
	return reinterpret_cast<ClientContext *>(ptr);
}
inline OptionWrapperV2 *ToOption(duckdb_v2_option_handle ptr) {
	return reinterpret_cast<OptionWrapperV2 *>(ptr);
}
inline QueryProgressWrapperV2 *ToQueryProgress(duckdb_v2_query_progress_handle ptr) {
	return reinterpret_cast<QueryProgressWrapperV2 *>(ptr);
}
// duckdb_v2_sql_statement_handle is a heap-allocated duckdb::SQLStatement
// (a derived class instance) with no wrapper. statement_execute reads it
// through this cast to Copy() it (non-consuming); sql_statement_destroy
// deletes through this cast.
inline SQLStatement *ToSqlStatement(duckdb_v2_sql_statement_handle ptr) {
	return reinterpret_cast<SQLStatement *>(ptr);
}

// Backing struct for the opaque duckdb_v2_statement_iterator_handle.
// The current backend parses eagerly (ClientContext::ParseStatements)
// and yields from the vector; the iterator-shaped contract permits a
// lazy parse-on-next backend later without an API change.
struct StatementIteratorWrapperV2 {
	vector<unique_ptr<SQLStatement>> statements;
	idx_t cursor = 0;
};
inline StatementIteratorWrapperV2 *ToStatementIterator(duckdb_v2_statement_iterator_handle ptr) {
	return reinterpret_cast<StatementIteratorWrapperV2 *>(ptr);
}
// The logical_type handle is not wrapped — the underlying duckdb::LogicalType
// is heap-allocated directly. The V2 test suite relies on this layout to
// share fixtures with V1 (V1-built composites are reinterpret-cast to V2
// handles); if a wrapper is added later, those tests must change too.
inline LogicalType *ToLogicalType(duckdb_v2_logical_type_handle ptr) {
	return reinterpret_cast<LogicalType *>(ptr);
}
// Same pattern as ToLogicalType: a duckdb_v2_value_handle is a heap-allocated
// duckdb::Value with no wrapper. Lets V2 tests adopt V1-built values the
// same way the logical_type bridge reuses V1 fixtures. Keep it identity —
// do not wrap.
inline Value *ToValue(duckdb_v2_value_handle ptr) {
	return reinterpret_cast<Value *>(ptr);
}

// Builds the engine's name-keyed parameter map from the parallel (names, values)
// arrays that statement_execute / prepared_execute receive. A parameter binds by
// name when parameter_names supplies a non-empty view for it (a case-insensitive
// Identifier, so $Name matches "name"); an absent or empty name binds positionally,
// keyed "1".."N" (the engine's positional convention, so dense $1..$N and ? work).
// parameter_names may be null (all positional). Throws InvalidInputException on a
// malformed name view ({NULL, len > 0}) or a null value handle. Shared by both
// execute bridges so the keying rule lives in exactly one place and cannot drift.
// The wrong key set (names for a positional statement, or the reverse) is not
// policed here: it surfaces as a bind error from the engine's VerifyParameters.
inline void BuildParameterMap(const duckdb_v2_str *parameter_names, const duckdb_v2_value_handle *parameter_values,
                              idx_t parameter_count, const char *function_name,
                              identifier_map_t<BoundParameterData> &out) {
	for (idx_t i = 0; i < parameter_count; i++) {
		auto name = parameter_names ? parameter_names[i] : duckdb_v2_str {nullptr, 0};
		// A {NULL, 0} view is a valid (empty) name; only a null pointer with a
		// nonzero length is malformed.
		if (!name.ptr && name.len > 0) {
			throw InvalidInputException("malformed parameter name passed to %s", function_name);
		}
		if (!parameter_values[i]) {
			throw InvalidInputException("null parameter value passed to %s", function_name);
		}
		// Named iff the name view is non-empty; otherwise positional.
		Identifier key = (name.ptr && name.len > 0) ? ToIdentifier(name) : Identifier(std::to_string(i + 1));
		out[key] = BoundParameterData(*ToValue(parameter_values[i]));
	}
}
// Backing struct for the opaque duckdb_v2_schema_handle: an ordered (name, type)
// row schema (no single engine object represents one). A deque so the name and
// type schema_get_field borrows stay valid for the schema's lifetime. The bind
// verbs (statement_bind, result_get_schema) populate fields directly.
struct SchemaFieldV2 {
	std::string name;
	LogicalType type;
};
struct SchemaWrapperV2 {
	std::deque<SchemaFieldV2> fields;
};
inline SchemaWrapperV2 *ToSchema(duckdb_v2_schema_handle ptr) {
	return reinterpret_cast<SchemaWrapperV2 *>(ptr);
}
// Backing struct for the opaque duckdb_v2_result_handle: the streaming
// result's state machine.
//
// statement_execute preprocesses the copied statement (pragma
// reparsing, expansion unpacking, transaction wrapping), which can turn
// one user statement into a group of engine statements ("fragments",
// e.g. PIVOT, or ALTER ... ADD COLUMN with a non-constant DEFAULT). The
// wrapper executes the fragments in order; each one passes through two
// internal phases with their own task-step API:
//
//   for each fragment:
//     PENDING (unique_ptr<PendingQueryResult>)
//       -> STREAMING (unique_ptr<QueryResult>, usually StreamQueryResult)
//   -> FINISHED | CANCELLED | ERRORED (sticky)
//
// (The error state is named ERRORED, not ERROR: windows.h defines ERROR
// as an object-like macro.)
//
// PENDING drives PendingQueryResult::ExecuteTask; once the result is
// ready, Execute() transitions to STREAMING, which drives
// StreamQueryResult::ExecuteTask + Fetch. Non-row statements can come
// back materialized even with ALLOW_STREAMING; the STREAMING phase
// drains those through the same Fetch loop. Terminal states drop the
// internal results, release the connection's busy slot, and never touch
// the internals again: steps keep reporting FINISHED / CANCELLED, and
// ERRORED keeps rethrowing the recorded ErrorData.
//
// Result selection mirrors ClientContext::Query's chain-append rule: the
// caller sees the chunks and metadata of the PRINCIPAL fragment, which
// is the first fragment whose return_type is QUERY_RESULT, or the last
// fragment when none produces rows. Other fragments execute through the
// same machinery with their output drained and discarded (the engine
// drops those results too). An expansion with more than one
// row-producing fragment cannot be streamed as one result and reports
// NOT_IMPLEMENTED (no known expansion produces one).
//
// Schema metadata (types, names, statement_type, properties) is copied
// from the principal fragment when it is prepared: at statement_execute
// time for non-expanding statements (the common case), or once stepping
// reaches the principal fragment otherwise. metadata_available gates the
// getters.
//
// Single-consumer: step/fetch/wait from one thread at a time.
// Cross-thread control goes through the connection
// (connection_interrupt / connection_query_progress).
struct ResultWrapperV2 {
	enum class State : uint8_t { PENDING, STREAMING, FINISHED, CANCELLED, ERRORED };

	~ResultWrapperV2() {
		// Finalize() (engine cleanup) runs in duckdb_v2_result_destroy, not here:
		// a destructor must not drive locked engine state behind a catch-all.
		pending.reset();
		result.reset();
		ReleaseBusySlot();
	}

	State state = State::PENDING;
	//! Live while state == PENDING.
	unique_ptr<PendingQueryResult> pending;
	//! Live while state == STREAMING.
	unique_ptr<QueryResult> result;

	//! Keeps the ClientContext alive for starting subsequent fragments and
	//! preserves the guarantee that an undrained result survives disconnect:
	//! the connection handle (a bare Connection *) may be destroyed while a
	//! result is live, but the context it shared with us stays alive here.
	shared_ptr<ClientContext> context;
	//! The preprocessed statement group; fragment_index points at the next
	//! fragment to start, fragment_count remembers the group size after
	//! fragments is cleared on terminal transitions.
	vector<unique_ptr<SQLStatement>> fragments;
	idx_t fragment_index = 0;
	idx_t fragment_count = 0;
	//! Positional parameter values for this execution, keyed by binding identifier
	//! ("1".."N"). Empty for an unparameterized statement (StartNextFragment takes
	//! the no-values path). Applied only to the first fragment (a parameterized
	//! statement does not expand into a group).
	identifier_map_t<BoundParameterData> param_values;
	//! True while the currently executing fragment is the principal one
	//! (its chunks are surfaced; other fragments' output is discarded).
	bool principal_active = false;
	//! True once a row-producing fragment has been selected as principal.
	bool principal_seen = false;
	//! True once the principal fragment's metadata has been captured.
	bool metadata_available = false;
	//! True when statement_execute injected its own wrapping transaction for
	//! this group (autocommit input that preprocessing expanded and wrapped
	//! in BEGIN ... COMMIT). Distinguishes a bridge-owned transaction, which
	//! must be rolled back on incomplete destroy, from a user-managed one,
	//! which must not be touched. Captured at query time because the engine's
	//! auto_rollback flag is not reliably observable from the bridge's
	//! fragment execution path.
	bool owns_wrapping_transaction = false;

	//! The owning connection's busy slot; released on terminal transition
	//! or destroy, whichever comes first.
	shared_ptr<ConnectionBusySlotV2> busy_slot;

	//! Principal fragment's metadata, valid once metadata_available.
	vector<LogicalType> types;
	vector<string> names;
	StatementType statement_type = StatementType::INVALID_STATEMENT;
	StatementProperties properties;

	//! Sticky error, recorded when state == ERRORED.
	ErrorData error;

	//! Mirrors ClientContext::Query's error handling for expanded groups
	//! (client_context.cpp, chain-append loop): when the group cannot
	//! complete, roll back the transaction statement_execute injected to wrap
	//! it. A no-op for non-expanded statements, for groups that ran inside a
	//! user-managed transaction (which the bridge never wraps), and when the
	//! transaction is already gone.
	void RollbackIncompleteGroup() {
		if (!owns_wrapping_transaction || !context) {
			return;
		}
		if (context->transaction.HasActiveTransaction()) {
			// Mirrors Connection::Rollback (Query("ROLLBACK") + throw on error),
			// driven through the retained context so it works after disconnect.
			auto result = context->Query("ROLLBACK", QueryResultOutputType::FORCE_MATERIALIZED);
			if (result->HasError()) {
				result->ThrowError();
			}
		}
	}

	//! Close() the live engine result so an abandoned active query is cleaned
	//! up (freeing the executor, which breaks the ClientContext ref cycle),
	//! then roll back an injected group transaction. May throw; the terminal
	//! states leave pending/result null, so this is then a no-op.
	void Finalize() {
		if (pending) {
			pending->Close();
		} else if (result && result->type == QueryResultType::STREAM_RESULT) {
			result->Cast<StreamQueryResult>().Close();
		}
		RollbackIncompleteGroup();
	}

	//! Frees the connection for its next query. Only the current owner can
	//! release the slot, so a release after the connection moved on is a
	//! no-op.
	void ReleaseBusySlot() {
		if (busy_slot) {
			void *expected = this;
			busy_slot->owner.compare_exchange_strong(expected, nullptr);
			busy_slot.reset();
		}
	}

	// State-machine entry points; defined in query_result-v2.cpp. All of
	// them throw DuckDB exceptions on failure (callers wrap in
	// WithErrorHandler) and record sticky errors before throwing.

	//! Adopts an already-produced pending query into the state machine: the single
	//! seam both the stateless (fragment) and prepared paths reach. When is_principal,
	//! captures its metadata and surfaces its chunks. Throws on a pending prepare error.
	void BeginPending(unique_ptr<PendingQueryResult> pending, bool is_principal);
	//! Starts the pending query for the next fragment, selecting it as
	//! principal per the engine-mirrored rule and adopting it via BeginPending.
	//! Throws on prepare errors.
	void StartNextFragment();
	//! Drives one unit of work; never blocks. On CHUNK, out_chunk holds the
	//! produced chunk; on every other status it is reset.
	DUCKDB_V2_RESULT_STEP_STATUS Step(unique_ptr<DataChunk> &out_chunk);
	//! Blocks until Step can make progress. No-op on terminal states.
	void Wait();
	//! Blocking convenience: steps/waits until a chunk is produced (returned)
	//! or the stream ends (nullptr). Cancellation throws InterruptException.
	unique_ptr<DataChunk> FetchChunkBlocking();
	//! Throws unless the principal fragment's metadata is available.
	void RequireMetadata() const;

private:
	//! Shared error sink: interrupts become the sticky CANCELLED state
	//! (returned as a status), everything else becomes the sticky ERRORED
	//! state and throws.
	DUCKDB_V2_RESULT_STEP_STATUS HandleExecutionError(ErrorData error_data);
};

inline ResultWrapperV2 *ToResult(duckdb_v2_result_handle ptr) {
	return reinterpret_cast<ResultWrapperV2 *>(ptr);
}
// duckdb_v2_data_chunk_handle is a heap-allocated duckdb::DataChunk with no
// wrapper. Cardinality flows through the API explicitly; no per-chunk
// state needs caching. duckdb_v2_data_chunk_destroy deletes through this
// cast.
inline DataChunk *ToDataChunk(duckdb_v2_data_chunk_handle ptr) {
	return reinterpret_cast<DataChunk *>(ptr);
}
// duckdb_v2_vector_handle is a borrowed duckdb::Vector. Top-level vectors
// point into the owning DataChunk's data[]; nested children point into
// core's ListVector / ArrayVector / StructVector / etc storage. No
// wrapper — vector_get_view extracts (data, validity, sel) directly via
// the matching FlatVector / ConstantVector / DictionaryVector core
// helpers, without caching a UnifiedVectorFormat.
inline Vector *ToVector(duckdb_v2_vector_handle ptr) {
	return reinterpret_cast<Vector *>(ptr);
}
// duckdb_v2_string_heap_handle is a borrowed duckdb::StringHeap living inside a
// vector's auxiliary buffer. No wrapper — the handle is the bare StringHeap
// pointer, obtained via vector_get_string_heap and valid only while the owning
// vector's heap is. The caller never destroys it.
inline StringHeap *ToStringHeap(duckdb_v2_string_heap_handle ptr) {
	return reinterpret_cast<StringHeap *>(ptr);
}
// duckdb_v2_expression_handle is a borrowed duckdb::Expression living inside the
// engine's plan. No wrapper — accessors read directly off the Expression and
// its bound subclasses. The handle is read-only and never destroyed by the
// caller; children borrowed via expression_get_child share the parent's
// lifetime and are likewise unwrapped Expression pointers.
inline Expression *ToExpression(duckdb_v2_expression_handle ptr) {
	return reinterpret_cast<Expression *>(ptr);
}

// Backing struct for the opaque duckdb_v2_bind_arguments_handle: a borrowed view
// of a function's bound argument list during a scalar / aggregate bind callback,
// reached via the `arguments` field on the bind-args struct. `arguments` points
// at the argument expressions the binder threads through (the children); the
// optional `argument_types` points at the bound function's argument-type list.
// Truncation shrinks both so they stay in sync, which keeps the scalar path
// (children mutated directly) and the aggregate path (children resized to the
// argument-type list) both valid. Lives on the trampoline's stack for the
// duration of the callback; the handle is never owned or destroyed by the caller.
struct BindArgumentsV2 {
	vector<unique_ptr<Expression>> *arguments = nullptr;
	vector<LogicalType> *argument_types = nullptr;
};
inline BindArgumentsV2 *ToBindArguments(duckdb_v2_bind_arguments_handle ptr) {
	return reinterpret_cast<BindArgumentsV2 *>(ptr);
}

// Map core's VectorType to the V2 surface. FSST / SEQUENCE / SHREDDED
// collapse into OTHER — V2's untyped view rejects those kinds and
// requires an explicit duckdb_v2_vector_flatten first.
inline DUCKDB_V2_VECTOR_TYPE MapVectorType(VectorType vt) {
	switch (vt) {
	case VectorType::FLAT_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_FLAT;
	case VectorType::CONSTANT_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_CONSTANT;
	case VectorType::DICTIONARY_VECTOR:
		return DUCKDB_V2_VECTOR_TYPE_DICTIONARY;
	default:
		return DUCKDB_V2_VECTOR_TYPE_OTHER;
	}
}

// Map DuckDB's SettingScopeTarget to the V2 enum. Legacy options
// (declared via DUCKDB_GLOBAL / DUCKDB_LOCAL / DUCKDB_GLOBAL_LOCAL)
// carry SettingScopeTarget::INVALID; we surface that as UNKNOWN so V2
// callers can distinguish "unconstrained legacy" from a declared scope.
inline DUCKDB_V2_OPTION_TARGET_SCOPE MapScopeTarget(SettingScopeTarget s) {
	switch (s) {
	case SettingScopeTarget::GLOBAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY;
	case SettingScopeTarget::LOCAL_ONLY:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY;
	case SettingScopeTarget::GLOBAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT;
	case SettingScopeTarget::LOCAL_DEFAULT:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT;
	default:
		return DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	}
}

// Map V2's user-facing scope choice to DuckDB's SetScope.
inline SetScope MapSettingScopeV2(DUCKDB_V2_SETTING_SCOPE s) {
	switch (s) {
	case DUCKDB_V2_SETTING_SCOPE_GLOBAL:
		return SetScope::GLOBAL;
	case DUCKDB_V2_SETTING_SCOPE_LOCAL:
		return SetScope::SESSION;
	default:
		return SetScope::AUTOMATIC;
	}
}

// Scan setting_aliases[] for entries pointing at the same canonical
// option (matched by name) and append their alias names.
inline void PopulateOptionAliases(OptionWrapperV2 &out, const char *canonical_name) {
	auto alias_count = DBConfig::GetAliasCount();
	for (idx_t i = 0; i < alias_count; i++) {
		auto alias = DBConfig::GetAliasByIndex(i);
		if (!alias) {
			continue;
		}
		auto aliased = DBConfig::GetOptionByIndex(alias->option_index);
		if (aliased && std::strcmp(aliased->name, canonical_name) == 0) {
			out.aliases.emplace_back(alias->alias);
		}
	}
}

// Read the effective setting for `name` through `client`'s setting
// cascade. For a database admin client (no LOCAL overrides) this
// returns GLOBAL → static default; for a connection client it returns
// LOCAL → GLOBAL → static default. Falls back to `fallback_default` if
// the cascade returned NULL.
inline std::string ReadEffectiveSetting(ClientContext &client, const std::string &name,
                                        const std::string &fallback_default) {
	Value result;
	if (client.TryGetCurrentSetting(name, result) && !result.IsNull()) {
		return result.ToString();
	}
	return fallback_default;
}

// Populate `out` from a core ConfigurationOption resolved by name. The
// canonical name is taken from the option (not the input), so passing
// an alias resolves to the canonical with the alias listed in
// out.aliases.
inline void PopulateOptionFromCore(OptionWrapperV2 &out, const ConfigurationOption &option, ClientContext &client) {
	out.name = option.name ? option.name : "";
	out.description = option.description ? option.description : "";
	out.target_scope = MapScopeTarget(option.scope);
	out.default_setting = option.default_value ? option.default_value : "";
	out.aliases.clear();
	PopulateOptionAliases(out, out.name.c_str());
	out.setting = ReadEffectiveSetting(client, out.name, out.default_setting);
}

// Populate `out` from an extension option. Extension options carry no
// SettingScopeTarget (the V2 enum reports UNKNOWN) and no aliases.
inline void PopulateOptionFromExtension(OptionWrapperV2 &out, const std::string &name,
                                        const ExtensionOption &ext_option, ClientContext &client) {
	out.name = name;
	out.description = ext_option.description;
	out.target_scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	out.default_setting = ext_option.default_value.IsNull() ? std::string() : ext_option.default_value.ToString();
	out.aliases.clear();
	out.setting = ReadEffectiveSetting(client, name, out.default_setting);
}

// Resolve `name` to either a core option or an extension option and
// populate `out`. Throws InvalidInputException if neither matches. The
// bridge layer is responsible for translating the exception into a V2
// error code (matches the pattern PhysicalSet uses internally).
inline void BuildOptionByName(OptionWrapperV2 &out, ClientContext &client, DBConfig &config, const std::string &name) {
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		PopulateOptionFromCore(out, *option, client);
		return;
	}
	ExtensionOption ext_option;
	if (config.TryGetExtensionOption(name, ext_option)) {
		PopulateOptionFromExtension(out, name, ext_option, client);
		return;
	}
	throw InvalidInputException("unknown configuration option: %s", name);
}

// Populate `out` from the option at `index` in the {core ∪ extension}
// space. Indices [0, core_count) hit core options;
// [core_count, total) hit extension options. Throws
// InvalidInputException for an out-of-range index.
inline void BuildOptionByIndex(OptionWrapperV2 &out, ClientContext &client, DBConfig &config, idx_t index) {
	auto core_count = DBConfig::GetOptionCount();
	if (index < core_count) {
		auto option = DBConfig::GetOptionByIndex(index);
		if (!option) {
			throw InvalidInputException("core option not found at given index");
		}
		PopulateOptionFromCore(out, *option, client);
		return;
	}
	idx_t ext_rel = index - core_count;
	auto ext_settings = config.GetExtensionSettings();
	if (ext_rel >= ext_settings.size()) {
		throw InvalidInputException("option index out of range");
	}
	idx_t i = 0;
	for (auto &kv : ext_settings) {
		if (i == ext_rel) {
			PopulateOptionFromExtension(out, kv.first, kv.second, client);
			return;
		}
		++i;
	}
	throw InvalidInputException("option index out of range");
}

// Backing struct for the opaque duckdb_v2_prepared_statement_handle. The engine
// PreparedStatement co-owns the ClientContext via shared_ptr, so the handle stays
// valid across executions and even after the connection is destroyed (acceptable
// for this opt-in cached path).
struct PreparedStatementWrapperV2 {
	unique_ptr<PreparedStatement> prepared;
};
inline PreparedStatementWrapperV2 *ToPreparedStatement(duckdb_v2_prepared_statement_handle ptr) {
	return reinterpret_cast<PreparedStatementWrapperV2 *>(ptr);
}

// The honest reuse predicate: reuses iff all parameter types resolved at prepare
// time and the plan is cacheable. The exact negation of the engine's rebind gate
// (PreparedStatementData::RequireRebind: !bound_all_parameters or
// always_require_rebind forces a rebind); always_require_rebind is set for a
// non-cacheable plan (a table scan; PreparedStatement::CanCachePlan).
inline bool PreparedReusesPlan(const StatementProperties &properties) {
	return properties.bound_all_parameters && !properties.always_require_rebind;
}

// Forward declaration (defined below); lets ErrorInfoV2::ThrowAsException
// rethrow with the mapped exception class.
inline bool TryGetExceptionTypeFromErrorCode(DUCKDB_V2_API_CALL_t code, ExceptionType &out_type);

// Backing struct for the opaque duckdb_v2_error_info_handle handle. Allocated
// only on failure paths and only when the caller requested detail (i.e.
// passed a non-null err out-parameter).
struct ErrorInfoV2 {
	DUCKDB_V2_API_CALL_t code = DUCKDB_V2_ERROR_NONE;
	// message: full "<Type> Error: <raw>" (ErrorData::Message()). raw_message: the
	// body with that prefix stripped (ErrorData::RawMessage()), in the engine's
	// rendered form (caret block, or JSON under errors_as_json); empty for a
	// directly-set message. Both written on the error path (WithErrorHandler).
	std::string message;
	std::string raw_message;

	bool HasError() const {
		return code != DUCKDB_V2_ERROR_NONE;
	}

	[[noreturn]] void ThrowAsException() const {
		// Only throw if there's actually an error!
		D_ASSERT(HasError());

		// Rethrow with the exception class the code maps to, so the error class
		// round-trips (mirrors InvokeWithErrorSlot); InvalidInput is the fallback
		// for codes with no specific type.
		ExceptionType type;
		if (TryGetExceptionTypeFromErrorCode(code, type)) {
			throw duckdb::Exception(type, message);
		}
		throw duckdb::InvalidInputException(message);
	}
};

// Failure path. Ensure *err holds an ErrorInfoV2 (lazy-allocate if the slot
// is empty), then write code + message in place. Reuses any pre-existing
// slot content — which may be heap-allocated by a prior call OR caller-
// owned / trampoline-owned stack memory. Never destroys the slot. Safe to
// call with err == nullptr (caller opted out of detail). Returns the
// supplied code; the return value is authoritative regardless of err.
//
// Slot cleanup is the caller's job. External callers destroy via
// `error_info_destroy` on slots they own; callbacks must NEVER destroy the
// err slot handed to them by the library (the slot's storage may live on
// the library's stack).
inline DUCKDB_V2_API_CALL_t SetErrorInfo(duckdb_v2_error_info_handle *err, DUCKDB_V2_API_CALL_t code, const char *msg) {
	if (err) {
		if (!*err) {
			*err = reinterpret_cast<_duckdb_v2_error_info *>(new ErrorInfoV2());
		}
		auto &info = *reinterpret_cast<ErrorInfoV2 *>(*err);
		info.code = code;
		info.message = msg ? msg : "";
		// Directly-set message has no body; clear any from a prior failure.
		info.raw_message.clear();
	}
	return code;
}

// Inverse of GetErrorCodeFromExceptionType: maps a V2 error code back to the
// DuckDB ExceptionType that round-trips to it. Returns false for codes with no
// specific exception type (notably the DUCKDB_V2_API_ERROR sentinel and any
// unmapped value), so callers can fall back to a phase-appropriate default.
inline bool TryGetExceptionTypeFromErrorCode(DUCKDB_V2_API_CALL_t code, ExceptionType &out_type) {
	switch (code) {
	// Invalid Input
	case DUCKDB_V2_ERROR_INVALID_INPUT:
		out_type = ExceptionType::INVALID_INPUT;
		return true;
	case DUCKDB_V2_ERROR_OUT_OF_RANGE:
		out_type = ExceptionType::OUT_OF_RANGE;
		return true;
	case DUCKDB_V2_ERROR_OBJECT_SIZE:
		out_type = ExceptionType::OBJECT_SIZE;
		return true;
	// IO
	case DUCKDB_V2_ERROR_IO_GENERAL:
		out_type = ExceptionType::IO;
		return true;
	case DUCKDB_V2_ERROR_IO_NETWORK:
		out_type = ExceptionType::NETWORK;
		return true;
	case DUCKDB_V2_ERROR_IO_HTTP:
		out_type = ExceptionType::HTTP;
		return true;
	// Resource
	case DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY:
		out_type = ExceptionType::OUT_OF_MEMORY;
		return true;
	case DUCKDB_V2_ERROR_RESOURCE_CONNECTION:
		out_type = ExceptionType::CONNECTION;
		return true;
	case DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY:
		out_type = ExceptionType::DEPENDENCY;
		return true;
	case DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION:
		out_type = ExceptionType::MISSING_EXTENSION;
		return true;
	case DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD:
		out_type = ExceptionType::AUTOLOAD;
		return true;
	// Type
	case DUCKDB_V2_ERROR_TYPE_CONVERSION:
		out_type = ExceptionType::CONVERSION;
		return true;
	case DUCKDB_V2_ERROR_TYPE_UNKNOWN:
		out_type = ExceptionType::UNKNOWN_TYPE;
		return true;
	case DUCKDB_V2_ERROR_TYPE_INVALID:
		out_type = ExceptionType::INVALID_TYPE;
		return true;
	case DUCKDB_V2_ERROR_TYPE_MISMATCH:
		out_type = ExceptionType::MISMATCH_TYPE;
		return true;
	case DUCKDB_V2_ERROR_TYPE_DECIMAL:
		out_type = ExceptionType::DECIMAL;
		return true;
	case DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO:
		out_type = ExceptionType::DIVIDE_BY_ZERO;
		return true;
	// Query
	case DUCKDB_V2_ERROR_QUERY_PARSER:
		out_type = ExceptionType::PARSER;
		return true;
	case DUCKDB_V2_ERROR_QUERY_SYNTAX:
		out_type = ExceptionType::SYNTAX;
		return true;
	case DUCKDB_V2_ERROR_QUERY_BINDER:
		out_type = ExceptionType::BINDER;
		return true;
	case DUCKDB_V2_ERROR_QUERY_PLANNER:
		out_type = ExceptionType::PLANNER;
		return true;
	case DUCKDB_V2_ERROR_QUERY_OPTIMIZER:
		out_type = ExceptionType::OPTIMIZER;
		return true;
	case DUCKDB_V2_ERROR_QUERY_EXPRESSION:
		out_type = ExceptionType::EXPRESSION;
		return true;
	case DUCKDB_V2_ERROR_QUERY_EXECUTOR:
		out_type = ExceptionType::EXECUTOR;
		return true;
	case DUCKDB_V2_ERROR_QUERY_SCHEDULER:
		out_type = ExceptionType::SCHEDULER;
		return true;
	case DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED:
		out_type = ExceptionType::NOT_IMPLEMENTED;
		return true;
	case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED:
		out_type = ExceptionType::PARAMETER_NOT_RESOLVED;
		return true;
	case DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED:
		out_type = ExceptionType::PARAMETER_NOT_ALLOWED;
		return true;
	// Database
	case DUCKDB_V2_ERROR_DATABASE_CATALOG:
		out_type = ExceptionType::CATALOG;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_TRANSACTION:
		out_type = ExceptionType::TRANSACTION;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_CONSTRAINT:
		out_type = ExceptionType::CONSTRAINT;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_INDEX:
		out_type = ExceptionType::INDEX;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_SEQUENCE:
		out_type = ExceptionType::SEQUENCE;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_STATISTICS:
		out_type = ExceptionType::STAT;
		return true;
	case DUCKDB_V2_ERROR_DATABASE_SERIALIZATION:
		out_type = ExceptionType::SERIALIZATION;
		return true;
	// Configuration
	case DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS:
		out_type = ExceptionType::SETTINGS;
		return true;
	case DUCKDB_V2_ERROR_CONFIGURATION_INVALID:
		out_type = ExceptionType::INVALID_CONFIGURATION;
		return true;
	case DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION:
		out_type = ExceptionType::PERMISSION;
		return true;
	// Runtime
	case DUCKDB_V2_ERROR_RUNTIME_INTERNAL:
		out_type = ExceptionType::INTERNAL;
		return true;
	case DUCKDB_V2_ERROR_RUNTIME_FATAL:
		out_type = ExceptionType::FATAL;
		return true;
	case DUCKDB_V2_ERROR_RUNTIME_INTERRUPT:
		out_type = ExceptionType::INTERRUPT;
		return true;
	case DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER:
		out_type = ExceptionType::NULL_POINTER;
		return true;
	default:
		return false;
	}
}

// Bridge helper for callback trampolines. Hands the user C callback a fresh,
// stack-backed error slot, invokes it via `invoke`, and — if the callback
// signalled an error — rethrows it as a DuckDB exception so the engine surfaces
// it as a normal query error. The thrown exception's type is derived from the
// error code the callback signalled, so the code round-trips faithfully (a
// callback signalling OUT_OF_MEMORY surfaces as a resource error, not the
// phase default). EX is the fallback exception type used only when the code has
// no specific mapping (e.g. the DUCKDB_V2_API_ERROR sentinel); it should be a
// phase-appropriate default (BinderException for bind, InvalidInputException
// for init/exec). The slot lives on this function's stack; per the slot
// contract the callback must never destroy it. `invoke` receives the slot as
// `duckdb_v2_error_info_handle *` and forwards it to the callback; any cb_info
// inspection happens at the call site after this returns.
template <class EX, class FN>
inline void InvokeWithErrorSlot(FN &&invoke) {
	ErrorInfoV2 err {};
	auto err_ptr = reinterpret_cast<_duckdb_v2_error_info *>(&err);
	invoke(&err_ptr);
	if (err.code == DUCKDB_V2_ERROR_NONE) {
		return;
	}
	ExceptionType type;
	if (TryGetExceptionTypeFromErrorCode(err.code, type)) {
		throw Exception(type, err.message);
	}
	throw EX(err.message);
}

// Shared BIGNUM magnitude/sign decoder. Used by both
// duckdb_v2_value_get_bignum (PR2 value-side) and duckdb_v2_bignum_decode
// (PR4 vector-side) so the magnitude reconstruction + sign extraction
// lives in exactly one place. Allocates an owned buffer with malloc
// (caller frees with free()).
inline DUCKDB_V2_API_CALL_t DecodeBignumStringT(const string_t &storage, uint8_t **out_data, idx_t *out_length,
                                                bool *out_is_negative, const char *function_name,
                                                duckdb_v2_error_info_handle *err) {
	*out_data = nullptr;
	*out_length = 0;
	*out_is_negative = false;
	try {
		vector<uint8_t> magnitude;
		bool is_negative = false;
		Bignum::GetByteArray(magnitude, is_negative, storage);
		auto alloc = magnitude.empty() ? size_t {1} : magnitude.size();
		auto *buf = static_cast<uint8_t *>(std::malloc(alloc));
		if (!buf) {
			std::string msg = std::string(function_name) + ": malloc failed";
			return SetErrorInfo(err, DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY, msg.c_str());
		}
		if (!magnitude.empty()) {
			std::memcpy(buf, magnitude.data(), magnitude.size());
		}
		*out_data = buf;
		*out_length = magnitude.size();
		*out_is_negative = is_negative;
		return DUCKDB_V2_ERROR_NONE;
	} catch (std::exception &e) {
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, e.what());
	} catch (...) {
		std::string msg = std::string("unknown error in ") + function_name;
		return SetErrorInfo(err, DUCKDB_V2_API_ERROR, msg.c_str());
	}
}

inline DUCKDB_V2_API_CALL_t GetErrorCodeFromExceptionType(ExceptionType type) {
	switch (type) {
	// Invalid Input
	case ExceptionType::INVALID_INPUT:
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	case ExceptionType::OUT_OF_RANGE:
		return DUCKDB_V2_ERROR_OUT_OF_RANGE;
	case ExceptionType::OBJECT_SIZE:
		return DUCKDB_V2_ERROR_OBJECT_SIZE;
	// IO
	case ExceptionType::IO:
		return DUCKDB_V2_ERROR_IO_GENERAL;
	case ExceptionType::NETWORK:
		return DUCKDB_V2_ERROR_IO_NETWORK;
	case ExceptionType::HTTP:
		return DUCKDB_V2_ERROR_IO_HTTP;
	// Resource
	case ExceptionType::OUT_OF_MEMORY:
		return DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY;
	case ExceptionType::CONNECTION:
		return DUCKDB_V2_ERROR_RESOURCE_CONNECTION;
	case ExceptionType::DEPENDENCY:
		return DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY;
	case ExceptionType::MISSING_EXTENSION:
		return DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION;
	case ExceptionType::AUTOLOAD:
		return DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD;
	// Type
	case ExceptionType::CONVERSION:
		return DUCKDB_V2_ERROR_TYPE_CONVERSION;
	case ExceptionType::UNKNOWN_TYPE:
		return DUCKDB_V2_ERROR_TYPE_UNKNOWN;
	case ExceptionType::INVALID_TYPE:
		return DUCKDB_V2_ERROR_TYPE_INVALID;
	case ExceptionType::MISMATCH_TYPE:
		return DUCKDB_V2_ERROR_TYPE_MISMATCH;
	case ExceptionType::DECIMAL:
		return DUCKDB_V2_ERROR_TYPE_DECIMAL;
	case ExceptionType::DIVIDE_BY_ZERO:
		return DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO;
	// Query
	case ExceptionType::PARSER:
		return DUCKDB_V2_ERROR_QUERY_PARSER;
	case ExceptionType::SYNTAX:
		return DUCKDB_V2_ERROR_QUERY_SYNTAX;
	case ExceptionType::BINDER:
		return DUCKDB_V2_ERROR_QUERY_BINDER;
	case ExceptionType::PLANNER:
		return DUCKDB_V2_ERROR_QUERY_PLANNER;
	case ExceptionType::OPTIMIZER:
		return DUCKDB_V2_ERROR_QUERY_OPTIMIZER;
	case ExceptionType::EXPRESSION:
		return DUCKDB_V2_ERROR_QUERY_EXPRESSION;
	case ExceptionType::EXECUTOR:
		return DUCKDB_V2_ERROR_QUERY_EXECUTOR;
	case ExceptionType::SCHEDULER:
		return DUCKDB_V2_ERROR_QUERY_SCHEDULER;
	case ExceptionType::NOT_IMPLEMENTED:
		return DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED;
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED;
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED;
	// Database
	case ExceptionType::CATALOG:
		return DUCKDB_V2_ERROR_DATABASE_CATALOG;
	case ExceptionType::TRANSACTION:
		return DUCKDB_V2_ERROR_DATABASE_TRANSACTION;
	case ExceptionType::CONSTRAINT:
		return DUCKDB_V2_ERROR_DATABASE_CONSTRAINT;
	case ExceptionType::INDEX:
		return DUCKDB_V2_ERROR_DATABASE_INDEX;
	case ExceptionType::SEQUENCE:
		return DUCKDB_V2_ERROR_DATABASE_SEQUENCE;
	case ExceptionType::STAT:
		return DUCKDB_V2_ERROR_DATABASE_STATISTICS;
	case ExceptionType::SERIALIZATION:
		return DUCKDB_V2_ERROR_DATABASE_SERIALIZATION;
	// Configuration
	case ExceptionType::SETTINGS:
		return DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS;
	case ExceptionType::INVALID_CONFIGURATION:
		return DUCKDB_V2_ERROR_CONFIGURATION_INVALID;
	case ExceptionType::PERMISSION:
		return DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION;
	// Runtime
	case ExceptionType::INTERNAL:
		return DUCKDB_V2_ERROR_RUNTIME_INTERNAL;
	case ExceptionType::FATAL:
		return DUCKDB_V2_ERROR_RUNTIME_FATAL;
	case ExceptionType::INTERRUPT:
		return DUCKDB_V2_ERROR_RUNTIME_INTERRUPT;
	case ExceptionType::NULL_POINTER:
		return DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER;
	default:
		return DUCKDB_V2_API_ERROR;
	}
}

template <class T>
DUCKDB_V2_API_CALL_t WithErrorHandler(duckdb_v2_error_info_handle *err, T callback) {
	auto code = static_cast<DUCKDB_V2_API_CALL_t>(DUCKDB_V2_ERROR_NONE);
	auto text = string();
	auto raw_message = string();

	try {
		// Invoke the callback
		callback();
	} catch (const duckdb::Exception &ex) {
		ErrorData error_data(ex);
		code = GetErrorCodeFromExceptionType(error_data.Type());
		text = error_data.Message();
		// The unprefixed body (ErrorData::RawMessage()), in the engine's rendered form.
		raw_message = error_data.RawMessage();
	} catch (const std::exception &ex) {
		code = DUCKDB_V2_API_ERROR;
		text = ex.what();
	} catch (...) {
		code = DUCKDB_V2_API_ERROR;
		text = "An unknown error occurred.";
	}

	// Success leaves the slot untouched: the return code is authoritative, and
	// writing on every successful call (allocating one on first use) is pure
	// overhead. A stale info from an earlier failure may therefore survive a
	// later successful call — that is fine, because the caller keys off the
	// return code, not the slot, and reads *err only after a failing return.
	if (code == DUCKDB_V2_ERROR_NONE) {
		return code;
	}

	// Failure: report detail through the slot if the caller provided one.
	if (err) {
		if (!*err) {
			*err = reinterpret_cast<_duckdb_v2_error_info *>(new ErrorInfoV2());
		}
		auto &out = *reinterpret_cast<ErrorInfoV2 *>(*err);
		out.code = code;
		out.message = std::move(text);
		// Overwrite in place; empty when this error carried only a flat message,
		// so a stale body from an earlier failure never survives.
		out.raw_message = std::move(raw_message);
	}

	return code;
}

// Function property channel (api_spec/v2/function/function.yaml). Translates the
// generic (key, value) enum pair onto the engine's FunctionProperties /
// AggregateFunctionProperties. Defined in function_properties-v2.cpp. All throw
// InvalidInputException on a value that does not match the key or a key that is
// not valid for the function type. Forward-declared to keep this header light.
class FunctionProperties;
class AggregateFunctionProperties;

void SetScalarFunctionProperty(FunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                               DUCKDB_V2_FUNCTION_PROPERTY_VALUE value);
DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetScalarFunctionProperty(const FunctionProperties &props,
                                                            DUCKDB_V2_FUNCTION_PROPERTY_KEY key);
void SetAggregateFunctionProperty(AggregateFunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                  DUCKDB_V2_FUNCTION_PROPERTY_VALUE value);
DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetAggregateFunctionProperty(const AggregateFunctionProperties &props,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_KEY key);

// Owning RAII wrapper for an opaque resource with optional destroy and equals callbacks.
class OpaqueDataHandle {
public:
	OpaqueDataHandle() = default;

	explicit OpaqueDataHandle(void *data, duckdb_v2_opaque_destroy_fn destroy_cb = nullptr,
	                          duckdb_v2_opaque_equals_fn equals_cb = nullptr)
	    : data(data), destroy_cb(destroy_cb), equals_cb(equals_cb) {
	}

	// Moveable
	OpaqueDataHandle(OpaqueDataHandle &&other) noexcept
	    : data(other.data), destroy_cb(other.destroy_cb), equals_cb(other.equals_cb) {
		other.data = nullptr;
		other.destroy_cb = nullptr;
		other.equals_cb = nullptr;
	}

	OpaqueDataHandle &operator=(OpaqueDataHandle &&other) noexcept {
		std::swap(data, other.data);
		std::swap(destroy_cb, other.destroy_cb);
		std::swap(equals_cb, other.equals_cb);
		return *this;
	}

	// Not copyable
	OpaqueDataHandle(const OpaqueDataHandle &) = delete;
	OpaqueDataHandle &operator=(const OpaqueDataHandle &) = delete;

	~OpaqueDataHandle() {
		if (data && destroy_cb) {
			destroy_cb(data);
		}
	}

	bool operator==(const OpaqueDataHandle &other) const {
		if (equals_cb) {
			return equals_cb(data, other.data);
		}
		return data == other.data;
	}

	bool operator!=(const OpaqueDataHandle &other) const {
		return !(*this == other);
	}

	bool Equals(const OpaqueDataHandle &other) const {
		return *this == other;
	}

	void *GetData() const {
		return data;
	}

private:
	void *data = nullptr;
	duckdb_v2_opaque_destroy_fn destroy_cb = nullptr;
	duckdb_v2_opaque_equals_fn equals_cb = nullptr;
};

} // namespace duckdb
