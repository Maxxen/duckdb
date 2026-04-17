//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BufferManager;
class InterruptState;

//! A half-open range of frame boundary values _relative to the current row_
//! This is why they are signed values.
struct FrameDelta {
	FrameDelta() : begin(0), end(0) {};
	FrameDelta(int64_t begin, int64_t end) : begin(begin), end(end) {};
	int64_t begin = 0;
	int64_t end = 0;
};

//! The half-open ranges of frame boundary values relative to the current row
using FrameStats = array<FrameDelta, 2>;

//! The partition data for custom window functions
//! Note that if the inputs is nullptr then the column count is 0,
//! but the row count will still be valid
class ColumnDataCollection;
struct WindowPartitionInput {
	WindowPartitionInput(ExecutionContext &context, const ColumnDataCollection *inputs, const idx_t count,
	                     const vector<column_t> &column_ids, const vector<bool> &all_valid,
	                     const ValidityMask &filter_mask, const FrameStats &stats, InterruptState &interrupt_state)
	    : context(context), inputs(inputs), count(count), column_ids(column_ids), all_valid(all_valid),
	      filter_mask(filter_mask), stats(stats), interrupt_state(interrupt_state) {
	}
	ExecutionContext &context;
	const ColumnDataCollection *inputs;
	const idx_t count;
	const vector<column_t> column_ids;
	const vector<bool> &all_valid;
	const ValidityMask &filter_mask;
	const FrameStats stats;
	InterruptState &interrupt_state;
};

class BindAggregateFunctionInput;
class BoundAggregateFunction;

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)(const BoundAggregateFunction &function);
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(const BoundAggregateFunction &function, data_ptr_t state);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                   Vector &state, idx_t count);
//! The type used for combining hashed aggregate states
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                                     idx_t offset);
//! The type used for propagating statistics in aggregate functions (optional)
typedef unique_ptr<BaseStatistics> (*aggregate_statistics_t)(ClientContext &context, BoundAggregateExpression &expr,
                                                             AggregateStatisticsInput &input);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_aggregate_function_t)(BindAggregateFunctionInput &input);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state, AggregateInputData &aggr_input_data, idx_t count);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                          data_ptr_t state, idx_t count);

//! The type used for computing complex/custom windowed aggregate functions (optional)
typedef void (*aggregate_window_t)(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes,
                                   Vector &result, idx_t rid);

//! The type used for initializing shared complex/custom windowed aggregate state (optional)
typedef void (*aggregate_wininit_t)(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                    data_ptr_t g_state);

typedef void (*aggregate_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                      const BoundAggregateFunction &function);
typedef unique_ptr<FunctionData> (*aggregate_deserialize_t)(Deserializer &deserializer,
                                                            BoundAggregateFunction &function);

typedef LogicalType (*aggregate_get_state_type_t)(const AggregateFunction &function);

struct AggregateFunctionInfo {
	DUCKDB_API virtual ~AggregateFunctionInfo();

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

enum class AggregateDestructorType {
	STANDARD,
	// legacy destructors allow non-trivial destructors in aggregate states
	// these might not be trivial to off-load to disk
	LEGACY
};

class BaseAggregateFunction : public SimpleFunction {
public:
	// Inherit base constructors
	using SimpleFunction::SimpleFunction;

public:
	// clang-format off
	bool CanAggregate() const { return update || combine || finalize; }
	bool CanWindow() const { return window; }

	auto HasBindCallback() const -> bool { return bind != nullptr; }
	auto GetBindCallback() const -> bind_aggregate_function_t { return bind; }
	auto SetBindCallback(bind_aggregate_function_t callback) -> void { bind = callback; }

	auto HasStateInitCallback() const -> bool { return initialize != nullptr; }
	auto GetStateInitCallback() const -> aggregate_initialize_t { return initialize; }
	auto SetStateInitCallback(aggregate_initialize_t callback) -> void { initialize = callback; }

	auto HasStateSizeCallback() const -> bool { return state_size != nullptr; }
	auto GetStateSizeCallback() const -> aggregate_size_t { return state_size; }
	auto SetStateSizeCallback(aggregate_size_t callback) -> void { state_size = callback; }

	auto HasStateDestructorCallback() const -> bool { return destructor != nullptr; }
	auto GetStateDestructorCallback() const -> aggregate_destructor_t { return destructor; }
	auto SetStateDestructorCallback(aggregate_destructor_t callback) -> void { destructor = callback; }

	auto HasStateUpdateCallback() const -> bool { return update != nullptr; }
	auto GetStateUpdateCallback() const -> aggregate_update_t { return update; }
	auto SetStateUpdateCallback(aggregate_update_t callback) -> void { update = callback; }

	auto HasStateSimpleUpdateCallback() const -> bool { return simple_update != nullptr; }
	auto GetStateSimpleUpdateCallback() const -> aggregate_simple_update_t { return simple_update; }
	auto SetStateSimpleUpdateCallback(aggregate_simple_update_t callback) -> void { simple_update = callback; }

	auto SetStateCombineCallback(aggregate_combine_t callback) -> void { combine = callback; }
	auto GetStateCombineCallback() const -> aggregate_combine_t { return combine; }
	auto HasStateCombineCallback() const -> bool { return combine != nullptr; }

	auto SetStateFinalizeCallback(aggregate_finalize_t callback) -> void { finalize = callback; }
	auto GetStateFinalizeCallback() const -> aggregate_finalize_t { return finalize; }
	auto HasStateFinalizeCallback() const -> bool { return finalize != nullptr; }

	auto HasWindowCallback() const -> bool { return window != nullptr; }
	auto GetWindowCallback() const -> aggregate_window_t { return window; }
	auto SetWindowCallback(aggregate_window_t callback) -> void { window = callback; }

	auto SetWindowInitCallback(aggregate_wininit_t callback) -> void { window_init = callback; }
	auto GetWindowInitCallback() const -> aggregate_wininit_t { return window_init; }
	auto HasWindowInitCallback() const -> bool { return window_init != nullptr; }

	auto HasStatisticsCallback() const -> bool { return statistics != nullptr; }
	auto GetStatisticsCallback() const -> aggregate_statistics_t { return statistics; }
	auto SetStatisticsCallback(aggregate_statistics_t callback) -> void { statistics = callback; }

	auto HasSerializationCallbacks() const -> bool { return serialize != nullptr && deserialize != nullptr; }
	auto SetSerializeCallback(aggregate_serialize_t callback) -> void { serialize = callback; }
	auto SetDeserializeCallback(aggregate_deserialize_t callback) -> void { deserialize = callback; }
	auto GetSerializeCallback() const -> aggregate_serialize_t { return serialize; }
	auto GetDeserializeCallback() const -> aggregate_deserialize_t { return deserialize; }

	auto HasExportTypeCallback() const -> bool { return get_state_type != nullptr; }
	auto GetExportTypeCallback() const -> aggregate_get_state_type_t { return get_state_type; }
	auto SetExportTypeCallback(aggregate_get_state_type_t callback) -> void { get_state_type = callback; }

	auto HasExtraFunctionInfo() const -> bool { return function_info != nullptr; }
	auto GetExtraFunctionInfo() const -> AggregateFunctionInfo& { D_ASSERT(function_info.get()); return *function_info; }
	auto SetExtraFunctionInfo(shared_ptr<AggregateFunctionInfo> info) -> void { function_info = std::move(info); }
	template <class T, class... ARGS>
	auto SetExtraFunctionInfo(ARGS &&... args) -> void { function_info = make_shared_ptr<T>(std::forward<ARGS>(args)...); }

	// clang-format on
public:
	auto GetOrderDependent() const -> AggregateOrderDependent {
		return order_dependent;
	}
	auto SetOrderDependent(AggregateOrderDependent value) -> void {
		order_dependent = value;
	}
	auto GetDistinctDependent() const -> AggregateDistinctDependent {
		return distinct_dependent;
	}
	auto SetDistinctDependent(AggregateDistinctDependent value) -> void {
		distinct_dependent = value;
	}

	auto GetStateType() const -> LogicalType {
		throw NotImplementedException("TODO: Get state type");
		/*
		D_ASSERT(get_state_type);
		const auto result = get_state_type(*this);
		// The underlying type of the AggregateState should be a struct
		D_ASSERT(result.id() == LogicalTypeId::STRUCT);
		return result;
		*/
	}

	hash_t Hash() const;

protected:
	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function (may be null, if window is set)
	aggregate_update_t update;
	//! The hashed aggregate combine states function (may be null, if window is set)
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function (may be null, if window is set)
	aggregate_finalize_t finalize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;
	//! The windowed aggregate custom function (may be null)
	aggregate_window_t window;
	//! The windowed aggregate custom initialization function (may be null)
	aggregate_wininit_t window_init = nullptr;

	//! The bind function (may be null)
	bind_aggregate_function_t bind;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	//! The statistics propagation function (may be null)
	aggregate_statistics_t statistics;

	aggregate_serialize_t serialize;
	aggregate_deserialize_t deserialize;

	//! Whether or not the aggregate is order dependent
	AggregateOrderDependent order_dependent;
	//! Whether or not the aggregate is affect by distinct modifiers
	AggregateDistinctDependent distinct_dependent;

	aggregate_get_state_type_t get_state_type = nullptr;

	//! Additional function info, passed to the bind
	shared_ptr<AggregateFunctionInfo> function_info;
};

class AggregateFunction : public BaseAggregateFunction { // NOLINT: work-around bug in clang-tidy
public:
	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : BaseAggregateFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                            LogicalType(LogicalTypeId::INVALID), null_handling) {
		this->state_size = state_size;
		this->initialize = initialize;
		this->update = update;
		this->combine = combine;
		this->finalize = finalize;
		this->simple_update = simple_update;
		this->window = window;
		this->bind = bind;
		this->destructor = destructor;
		this->statistics = statistics;
		this->serialize = serialize;
		this->deserialize = deserialize;
		this->order_dependent = AggregateOrderDependent::ORDER_DEPENDENT;
		this->distinct_dependent = AggregateDistinctDependent::DISTINCT_DEPENDENT;
	}

	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : BaseAggregateFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                            LogicalType(LogicalTypeId::INVALID)) {
		this->state_size = state_size;
		this->initialize = initialize;
		this->update = update;
		this->combine = combine;
		this->finalize = finalize;
		this->simple_update = simple_update;
		this->window = window;
		this->bind = bind;
		this->destructor = destructor;
		this->statistics = statistics;
		this->serialize = serialize;
		this->deserialize = deserialize;
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, simple_update, bind, destructor, statistics, window, serialize,
	                        deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING, simple_update, bind, destructor, statistics,
	                        window, serialize, deserialize) {
	}

	// Window constructor
	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_wininit_t window_init, aggregate_window_t window,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : BaseAggregateFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                            LogicalType(LogicalTypeId::INVALID)) {
		this->state_size = state_size;
		this->initialize = initialize;
		this->window = window;
		this->window_init = window_init;
		this->bind = bind;
		this->destructor = destructor;
		this->statistics = statistics;
		this->serialize = serialize;
		this->deserialize = deserialize;
		this->order_dependent = AggregateOrderDependent::ORDER_DEPENDENT;
		this->distinct_dependent = AggregateDistinctDependent::DISTINCT_DEPENDENT;
	}

public:
	auto SetExportTypeCallback(aggregate_get_state_type_t callback) & -> AggregateFunction & {
		get_state_type = callback;
		return *this;
	}
	auto SetExportTypeCallback(aggregate_get_state_type_t callback) && -> AggregateFunction && {
		get_state_type = callback;
		return std::move(*this);
	}

	pair<unique_ptr<BoundAggregateFunction>, unique_ptr<FunctionData>> Bind(BindAggregateFunctionInput &bind_input);
	pair<unique_ptr<BoundAggregateFunction>, unique_ptr<FunctionData>> Bind(ClientContext &context,
	                                                                        vector<unique_ptr<Expression>> &arguments);

public:
	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize && window == rhs.window;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

public:
	template <class STATE, class RESULT_TYPE, class OP>
	static AggregateFunction NullaryAggregate(LogicalType return_type) {
		return AggregateFunction(
		    {}, return_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    AggregateFunction::NullaryScatterUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
		    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::NullaryUpdate<STATE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction
	UnaryAggregate(const LogicalType &input_type, LogicalType return_type,
	               FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return AggregateFunction({input_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                         AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, null_handling,
		                         AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP, destructor_type>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction BinaryAggregate(const LogicalType &a_type, const LogicalType &b_type,
	                                         LogicalType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	}

public:
	template <class STATE>
	static idx_t StateSize(const BoundAggregateFunction &) {
		return sizeof(STATE);
	}

	template <class STATE, class OP, AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static void StateInitialize(const BoundAggregateFunction &, data_ptr_t state) {
		// FIXME: we should remove the "destructor_type" option in the future
#if !defined(__GNUC__) || (__GNUC__ >= 5)
		static_assert(std::is_trivially_move_constructible<STATE>::value ||
		                  destructor_type == AggregateDestructorType::LEGACY,
		              "Aggregate state must be trivially move constructible");
#endif
		OP::Initialize(*reinterpret_cast<STATE *>(state));
	}

	template <class STATE, class OP>
	static void NullaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                 Vector &states, idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryScatter<STATE, OP>(states, aggr_input_data, count);
	}

	template <class STATE, class OP>
	static void NullaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                          idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryUpdate<STATE, OP>(state, aggr_input_data, count);
	}

	template <class STATE, class T, class OP>
	static void UnaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                               Vector &states, idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryScatter<STATE, T, OP>(inputs[0], states, aggr_input_data, count);
	}

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                        idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[0], aggr_input_data, state, count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                Vector &states, idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[0], inputs[1], states,
		                                                            count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[0], inputs[1], state, count);
	}

	template <class STATE, class OP>
	static void StateCombine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
		AggregateExecutor::Combine<STATE, OP>(source, target, aggr_input_data, count);
	}

	template <class STATE, class RESULT_TYPE, class OP>
	static void StateFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                          idx_t offset) {
		AggregateExecutor::Finalize<STATE, RESULT_TYPE, OP>(states, aggr_input_data, result, count, offset);
	}

	template <class STATE, class OP>
	static void StateVoidFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                              idx_t offset) {
		AggregateExecutor::VoidFinalize<STATE, OP>(states, aggr_input_data, result, count, offset);
	}

	template <class STATE, class OP>
	static void StateDestroy(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		AggregateExecutor::Destroy<STATE, OP>(states, aggr_input_data, count);
	}
};

class BoundAggregateFunction : public BaseAggregateFunction {
public:
	// Bound function only
	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;

	LogicalType return_type;

	bool operator==(const BoundAggregateFunction &other) const;
	bool operator!=(const BoundAggregateFunction &other) const {
		return !(*this == other);
	}

	// Is this bound function derived from the given aggregate function
	// (i.e. is the given aggregate function the "template" of this bound function)?
	bool IsBoundFrom(const AggregateFunction &aggregate_function) const;

	// Copy over the function pointers and other relevant information from the given aggregate function to this bound
	// function
	void ReplaceDefinition(const AggregateFunction &aggregate_function);
};

class BindAggregateFunctionInput {
public:
	BindAggregateFunctionInput(ClientContext &context_p, BoundAggregateFunction &bound_function_p,
	                           vector<unique_ptr<Expression>> &arguments_p)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	BoundAggregateFunction &GetBoundFunction() const {
		return bound_function;
	}
	vector<unique_ptr<Expression>> &GetArguments() const {
		return arguments;
	}

private:
	ClientContext &context;
	BoundAggregateFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
};

inline pair<unique_ptr<BoundAggregateFunction>, unique_ptr<FunctionData>>
AggregateFunction::Bind(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
	// BindAggregateFunctionInput bind_input(context, *this, arguments);
	// return Bind(bind_input);

	// TODO!
	throw NotImplementedException("AggregateFunction::Bind");
}

} // namespace duckdb
