//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

class BoundWindowExpression;
struct WindowSharedExpressions;
class WindowExecutor;
class GlobalSinkState;
class LocalSinkState;
class WindowCollection;

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t {
	PARTITION_BEGIN,
	PARTITION_END,
	PEER_BEGIN,
	PEER_END,
	VALID_BEGIN,
	VALID_END,
	FRAME_BEGIN,
	FRAME_END
};

// C++ 11 won't do this automatically...
struct WindowBoundsHash {
	inline uint64_t operator()(const WindowBounds &value) const {
		return value;
	}
};

using WindowBoundsSet = unordered_set<WindowBounds, WindowBoundsHash>;

struct WindowFunctionInfo {
	DUCKDB_API virtual ~WindowFunctionInfo();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class BindWindowFunctionInput;
class BoundWindowFunction;

//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*window_bind_function_t)(BindWindowFunctionInput &input);

//! Validates the additional ordering usage.
typedef void (*window_validate_function_t)(ClientContext &context, BoundWindowFunction &function,
                                           vector<unique_ptr<Expression>> &arguments, vector<OrderByNode> &orders,
                                           vector<OrderByNode> &arg_orders);

//! Requests framing bounds that the function uses
typedef void (*window_bounds_function_t)(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr);

//! Requests expression sharing. If not provided, all children will be registered for evaluate time.
typedef void (*window_sharing_function_t)(WindowExecutor &executor, WindowSharedExpressions &sharing);

//! Constructs a global state for the hash group.
//! If not provided, a default WindowExecutorGlobalState will be generated, with references to the parameters
typedef unique_ptr<GlobalSinkState> (*window_global_function_t)(ClientContext &client, const WindowExecutor &executor,
                                                                const idx_t payload_count,
                                                                const ValidityMask &partition_mask,
                                                                const ValidityMask &order_mask);

//! Constructs a thread local state for the hash group.
//! If not provided, a default WindowExecutorLocalState will be generated, with references to the parameters
typedef unique_ptr<LocalSinkState> (*window_local_function_t)(ExecutionContext &context, const GlobalSinkState &gstate);

//! Sinks data into the thread-local state
typedef void (*window_sink_function_t)(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                       idx_t input_idx, OperatorSinkInput &sink);

//! Finalizes the thread-local state (builds all data structures needed for
typedef void (*window_finalize_function_t)(ExecutionContext &context, optional_ptr<WindowCollection> collection,
                                           OperatorSinkInput &sink);

//! Serialization of the binding data (if any)
typedef void (*window_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                   const BoundWindowFunction &function);
typedef unique_ptr<FunctionData> (*window_deserialize_t)(Deserializer &deserializer, BoundWindowFunction &function);

class BaseWindowFunction : public SimpleFunction {
public:
	// Inherit base constructors
	using SimpleFunction::SimpleFunction;

public:
	// clang-format off
	auto HasBindCallback() const -> bool { return bind != nullptr; }
	auto GetBindCallback() const -> window_bind_function_t { return bind; }
	auto SetBindCallback(window_bind_function_t value) -> void { bind = value; }

	auto HasValidateCallback() const -> bool { return validate != nullptr; }
	auto GetValidateCallback() const -> window_validate_function_t { return validate; }
	auto SetValidateCallback(window_validate_function_t value) -> void { validate = value; }

	auto HasBoundsCallback() const -> bool { return bounds != nullptr; }
	auto GetBoundsCallback() const -> window_bounds_function_t { return bounds; }
	auto SetBoundsCallback(window_bounds_function_t value) -> void { bounds = value; }

	auto HasSharingCallback() const -> bool { return sharing != nullptr; }
	auto GetSharingCallback() const -> window_sharing_function_t { return sharing; }
	auto SetSharingCallback(window_sharing_function_t value) -> void { sharing = value; }

	auto HasGlobalCallback() const -> bool { return global != nullptr; }
	auto GetGlobalCallback() const -> window_global_function_t { return global; }
	auto SetGlobalCallback(window_global_function_t value) -> void { global = value; }

	auto HasLocalCallback() const -> bool { return local != nullptr; }
	auto GetLocalCallback() const -> window_local_function_t { return local; }
	auto SetLocalCallback(window_local_function_t value) -> void { local = value; }

	auto HasSinkCallback() const -> bool { return sink != nullptr; }
	auto GetSinkCallback() const -> window_sink_function_t { return sink; }
	auto SetSinkCallback(window_sink_function_t value) -> void { sink = value; }

	auto HasFinalizeCallback() const -> bool { return finalize != nullptr; }
	auto GetFinalizeCallback() const -> window_finalize_function_t { return finalize; }
	auto SetFinalizeCallback(window_finalize_function_t value) -> void { finalize = value; }

	auto HasSerializationCallbacks() const -> bool { return serialize != nullptr && deserialize != nullptr; }
	auto SetSerializeCallback(window_serialize_t value) -> void { serialize = value; }
	auto SetDeserializeCallback(window_deserialize_t value) -> void { deserialize = value; }
	auto GetSerializeCallback() const -> window_serialize_t { return serialize; }
	auto GetDeserializeCallback() const -> window_deserialize_t { return deserialize; }

	auto SupportsDistinct() const -> bool { return can_distinct; }
	auto SetSupportsDistinct(bool value) -> void { can_distinct = value; }

	auto SupportsFilter() const -> bool { return can_filter; }
	auto SetSupportsFilter(bool value) -> void { can_filter = value; }

	auto SupportsOrderBy() const -> bool { return can_order_by; }
	auto SetSupportsOrderBy(bool value) -> void { can_order_by = value; }

	auto SupportsExclude() const -> bool { return can_exclude; }
	auto SetSupportsExclude(bool value) -> void { can_exclude = value; }

	auto SupportsIgnoreNulls() const -> bool { return can_ignore_nulls; }
	auto SetSupportsIgnoreNulls(bool value) -> void { can_ignore_nulls = value; }

	ExpressionType GetExpressionType() const { return window_enum; }
	// clang-format on

protected:
	//! The expression enum for the window function
	ExpressionType window_enum;

	//! Does the window function support DISTINCT?
	bool can_distinct = false;
	//! Does the window function support FILTER?
	bool can_filter = false;
	//! Does the window function support ORDER BY arguments?
	bool can_order_by = true;
	//! Does the window function support EXCLUDE?
	bool can_exclude = false;
	//! Does the window function support RESPECT/IGNORE NULLS?
	bool can_ignore_nulls = true;

	//! The bind function (may be null)
	window_bind_function_t bind = nullptr;
	//! The sort validation function
	window_validate_function_t validate = nullptr;
	//! The framing bounds lists
	window_bounds_function_t bounds = nullptr;
	//! The children sharing requirements
	window_sharing_function_t sharing = nullptr;
	//! The global state constructor
	window_global_function_t global = nullptr;
	//! The local state constructor
	window_local_function_t local = nullptr;
	//! The local state data sink
	window_sink_function_t sink = nullptr;
	//! The local state finalize operation
	window_finalize_function_t finalize = nullptr;

	//! Serialization specialization. Not yet implemented
	window_serialize_t serialize = nullptr;
	window_deserialize_t deserialize = nullptr;

	//! Additional function info, passed to the bind
	shared_ptr<WindowFunctionInfo> function_info;

public:
	bool operator==(const BaseWindowFunction &rhs) const {
		return name == rhs.name;
	}
	bool operator!=(const BaseWindowFunction &rhs) const {
		return !(*this == rhs);
	}
};

class WindowFunction : public BaseWindowFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr,
	               window_bounds_function_t bounds = nullptr, window_sharing_function_t sharing = nullptr,
	               window_global_function_t global = nullptr, window_local_function_t local = nullptr,
	               window_sink_function_t sink = nullptr, window_finalize_function_t finalize = nullptr)
	    : BaseWindowFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		this->window_enum = window_enum;
		this->bind = bind;
		this->bounds = bounds;
		this->sharing = sharing;
		this->global = global;
		this->local = local;
		this->sink = sink;
		this->finalize = finalize;
	}

	WindowFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, ExpressionType window_enum,
	               window_bind_function_t bind = nullptr, window_bounds_function_t bounds = nullptr,
	               window_sharing_function_t sharing = nullptr, window_global_function_t global = nullptr,
	               window_local_function_t local = nullptr, window_sink_function_t sink = nullptr,
	               window_finalize_function_t finalize = nullptr)
	    : WindowFunction(string(), arguments, return_type, window_enum, bind, bounds, sharing, global, local, sink,
	                     finalize) {
	}

	unique_ptr<BoundWindowExpression> Bind(ClientContext &context,
	                                       vector<unique_ptr<Expression>> arguments = vector<unique_ptr<Expression>>());
};

class BoundWindowFunction : public BaseWindowFunction {
public:
	BoundWindowFunction(const WindowFunction &function, vector<LogicalType> arguments, LogicalType returns)
	    : BaseWindowFunction(function), arguments(std::move(arguments)), return_type(std::move(returns)) {
		// Intentionally slice the WindowFunction here, as we only want the BaseWindowFunction part of it
	}

	// Bound function only
	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;

	LogicalType return_type;

	const LogicalType &GetReturnType() const {
		return return_type;
	}
	LogicalType &GetReturnType() {
		return return_type;
	}
	void SetReturnType(const LogicalType &return_type) {
		this->return_type = return_type;
	}
};

class BindWindowFunctionInput {
public:
	BindWindowFunctionInput(ClientContext &context_p, BoundWindowFunction &bound_function_p,
	                        vector<unique_ptr<Expression>> &arguments_p)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	BoundWindowFunction &GetBoundFunction() const {
		return bound_function;
	}
	vector<unique_ptr<Expression>> &GetArguments() const {
		return arguments;
	}

private:
	ClientContext &context;
	BoundWindowFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
};

} // namespace duckdb
