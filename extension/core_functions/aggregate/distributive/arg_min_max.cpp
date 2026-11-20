#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/minmax_n_helpers.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

namespace {

//! Assigns a value to the state, holding any extra information required to do so.
//! For non-string types this is empty and the assignment is a plain copy.
template <class T>
struct ArgMinMaxValueAssign {
	void Assign(T &target, T new_value, AggregateInputData &aggregate_input_data) {
		target = new_value;
	}
};

//! Strings are stored in the arena allocator, re-using the previous allocation when overwriting the value.
//! The allocation size is kept here - it is not part of the exported state.
template <>
struct ArgMinMaxValueAssign<string_t> {
	//! The size of the arena allocation for a non-inlined string value
	idx_t alloc_size;

	void Assign(string_t &target, string_t new_value, AggregateInputData &aggregate_input_data) {
		if (new_value.IsInlined()) {
			target = new_value;
			alloc_size = 0;
		} else {
			auto len = UnsafeNumericCast<idx_t>(new_value.GetSize());
			char *ptr;
			if (alloc_size >= len) {
				ptr = target.GetDataWriteable();
			} else {
				alloc_size = UnsafeNumericCast<idx_t>(NextPowerOfTwo(len));
				ptr = char_ptr_cast(aggregate_input_data.allocator.Allocate(alloc_size));
			}
			memcpy(ptr, new_value.GetData(), len);
			target = string_t(ptr, len);
		}
	}
};

template <typename T>
inline void ArgMinMaxReadValue(Vector &result, T &arg, T &target) {
	target = arg;
}

template <>
inline void ArgMinMaxReadValue(Vector &result, string_t &arg, string_t &target) {
	target = StringVector::AddStringOrBlob(result, arg);
}

//! The aggregate state of arg_min/arg_max is nullable on two levels: the state itself is NULL when no row has been
//! recorded yet (is_set, the outer optional), while the recorded "arg" and "by" values can themselves be NULL (the
//! inner optionals). Valid exported states are e.g. NULL, {'arg': NULL, 'by': 42} and {'arg': 1, 'by': 42}.
template <class A, class B>
struct ArgMinMaxState {
	using ARG_TYPE = A;
	using BY_TYPE = B;

	static constexpr const char *STATE_NAMES[] = {"arg", "by"};
	//! Both values are exported with the corresponding runtime types of the bound function - e.g. the argument
	//! can be a DATE or BLOB value instead of the physical type it is stored as
	using STATE_TYPE = OptionalStateType<StructStateType<OptionalStateType<StateTypedValue<A, StateReturnType>>,
	                                                     OptionalStateType<StateTypedValue<B, StateInputType<1>>>>>;

	A arg;
	//! Whether the recorded argument is valid (i.e. not NULL)
	bool arg_is_valid;
	B value;
	//! Whether the recorded "by" value is valid (i.e. not NULL)
	bool value_is_valid;
	//! Whether the state has been set (i.e. we have recorded a row)
	bool is_set;
	//! Assignment helpers - these hold the arena allocation size for string values (and are empty otherwise),
	//! and are not part of the exported state
	ArgMinMaxValueAssign<A> arg_assign;
	ArgMinMaxValueAssign<B> value_assign;

	void AssignArg(A input, AggregateInputData &aggregate_input_data) {
		arg_assign.Assign(arg, input, aggregate_input_data);
	}
	void AssignBy(B input, AggregateInputData &aggregate_input_data) {
		value_assign.Assign(value, input, aggregate_input_data);
	}
};

//! State for the vector arg_min/arg_max variants: the argument is stored as a binary sort key, while the "by"
//! value is typed - exported with the return type and the second argument type of the bound function respectively.
template <OrderType ORDER, class B>
struct ArgMinMaxVectorState : ArgMinMaxState<string_t, B> {
	using STATE_TYPE = OptionalStateType<StructStateType<OptionalStateType<StateSortKey<StateReturnType, ORDER>>,
	                                                     OptionalStateType<StateTypedValue<B, StateInputType<1>>>>>;
};

//! State for the generic arg_min/arg_max variants: both the argument and the "by" value are stored as binary
//! sort keys.
template <OrderType ORDER>
struct ArgMinMaxSortKeyState : ArgMinMaxState<string_t, string_t> {
	using STATE_TYPE = OptionalStateType<StructStateType<OptionalStateType<StateSortKey<StateReturnType, ORDER>>,
	                                                     OptionalStateType<StateSortKey<StateInputType<1>, ORDER>>>>;
};

template <class COMPARATOR>
struct ArgMinMaxBase {
	template <class A_TYPE, class B_TYPE, class STATE>
	static void Assign(STATE &state, const A_TYPE &x, const B_TYPE &y, const bool x_null, const bool y_null,
	                   AggregateInputData &aggregate_input_data) {
		D_ASSERT(aggregate_input_data.bind_data);
		const auto &bind_data = aggregate_input_data.bind_data->Cast<ArgMinMaxFunctionData>();

		if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL) {
			state.arg_is_valid = true;
			state.value_is_valid = true;
			state.AssignArg(x, aggregate_input_data);
			state.AssignBy(y, aggregate_input_data);
		} else {
			state.arg_is_valid = !x_null;
			state.value_is_valid = !y_null;
			if (state.arg_is_valid) {
				state.AssignArg(x, aggregate_input_data);
			}
			if (state.value_is_valid) {
				state.AssignBy(y, aggregate_input_data);
			}
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &x, const B_TYPE &y, AggregateBinaryInput &binary) {
		D_ASSERT(binary.input.bind_data);
		const auto &bind_data = binary.input.bind_data->Cast<ArgMinMaxFunctionData>();
		if (!state.is_set) {
			if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL &&
			    binary.left_mask.RowIsValid(binary.lidx) && binary.right_mask.RowIsValid(binary.ridx)) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_set = true;
				return;
			}
			if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ARG_NULL &&
			    binary.right_mask.RowIsValid(binary.ridx)) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_set = true;
				return;
			}
			if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ANY_NULL) {
				Assign(state, x, y, !binary.left_mask.RowIsValid(binary.lidx),
				       !binary.right_mask.RowIsValid(binary.ridx), binary.input);
				state.is_set = true;
			}
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x, y, binary);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE &state, A_TYPE x_data, B_TYPE y_data, AggregateBinaryInput &binary) {
		D_ASSERT(binary.input.bind_data);
		const auto &bind_data = binary.input.bind_data->Cast<ArgMinMaxFunctionData>();

		if (binary.right_mask.RowIsValid(binary.ridx) &&
		    (!state.value_is_valid || COMPARATOR::Operation(y_data, state.value))) {
			if (bind_data.null_handling != ArgMinMaxNullHandling::IGNORE_ANY_NULL ||
			    binary.left_mask.RowIsValid(binary.lidx)) {
				Assign(state, x_data, y_data, !binary.left_mask.RowIsValid(binary.lidx), false, binary.input);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggregate_input_data) {
		if (!source.is_set) {
			return;
		}

		if (!target.is_set || !target.value_is_valid ||
		    (source.value_is_valid && COMPARATOR::Operation(source.value, target.value))) {
			Assign(target, source.arg, source.value, !source.arg_is_valid, false, aggregate_input_data);
			target.is_set = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || !state.arg_is_valid) {
			finalize_data.ReturnNull();
		} else {
			ArgMinMaxReadValue<T>(finalize_data.result, state.arg, target);
		}
	}

	static bool IgnoreNull() {
		return false;
	}

	template <ArgMinMaxNullHandling NULL_HANDLING>
	static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &input) {
		auto &context = input.GetClientContext();
		auto &function = input.GetBoundFunction();
		auto &arguments = input.GetArguments();
		ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->GetReturnType());
		function.GetArguments()[0] = arguments[0]->GetReturnType();
		function.GetArguments()[1] = arguments[1]->GetReturnType();
		function.SetReturnType(arguments[0]->GetReturnType());

		auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING);
		return unique_ptr<FunctionData>(std::move(function_data));
	}
};

struct SpecializedGenericArgMinMaxState {
	static bool CreateExtraState() {
		// nop extra state
		return false;
	}

	static void PrepareData(const Vector &by, bool &, UnifiedVectorFormat &result) {
		by.ToUnifiedFormat(result);
	}
};

template <OrderType ORDER_TYPE>
struct GenericArgMinMaxState {
	static Vector CreateExtraState() {
		return Vector(LogicalType::BLOB);
	}

	static void PrepareData(const Vector &by, Vector &extra_state, UnifiedVectorFormat &result) {
		OrderModifiers modifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(by, extra_state, modifiers);
		extra_state.ToUnifiedFormat(result);
	}
};

template <typename COMPARATOR, OrderType ORDER_TYPE, class UPDATE_TYPE = SpecializedGenericArgMinMaxState>
struct VectorArgMinMaxBase : ArgMinMaxBase<COMPARATOR> {
	static constexpr OrderType ORDER = ORDER_TYPE;

	template <class STATE>
	static void Update(Vector inputs[], AggregateInputData &aggregate_input_data, idx_t input_count,
	                   Vector &state_vector, idx_t count) {
		D_ASSERT(aggregate_input_data.bind_data);
		const auto &bind_data = aggregate_input_data.bind_data->Cast<ArgMinMaxFunctionData>();

		auto &arg = inputs[0];
		UnifiedVectorFormat adata;
		arg.ToUnifiedFormat(adata);

		using BY_TYPE = typename STATE::BY_TYPE;
		auto &by = inputs[1];
		UnifiedVectorFormat bdata;
		auto extra_state = UPDATE_TYPE::CreateExtraState();
		UPDATE_TYPE::PrepareData(by, extra_state, bdata);
		const auto bys = UnifiedVectorFormat::GetData<BY_TYPE>(bdata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(sdata);

		STATE *last_state = nullptr;
		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;

		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];

			const auto aidx = adata.sel->get_index(i);
			const auto arg_null = !adata.validity.RowIsValid(aidx);

			if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL && arg_null) {
				continue;
			}

			const auto bidx = bdata.sel->get_index(i);

			if (!bdata.validity.RowIsValid(bidx)) {
				if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ANY_NULL && !state.is_set) {
					state.value_is_valid = false;
					if (!arg_null) {
						state.is_set = true;
						state.arg_is_valid = true;
						if (&state == last_state) {
							assign_count--;
						}
						assign_sel[assign_count++] = UnsafeNumericCast<sel_t>(i);
						last_state = &state;
					}
				}
				continue;
			}

			const auto bval = bys[bidx];

			if (!state.is_set || !state.value_is_valid || COMPARATOR::template Operation<BY_TYPE>(bval, state.value)) {
				state.AssignBy(bval, aggregate_input_data);
				state.value_is_valid = true;
				state.arg_is_valid = !arg_null;
				// micro-adaptivity: it is common we overwrite the same state repeatedly
				// e.g. when running arg_max(val, ts) and ts is sorted in ascending order
				// this check essentially says:
				// "if we are overriding the same state as the last row, the last write was pointless"
				// hence we skip the last write altogether
				if (!arg_null) {
					if (&state == last_state) {
						assign_count--;
					}
					assign_sel[assign_count++] = UnsafeNumericCast<sel_t>(i);
					last_state = &state;
				}
				state.is_set = true;
			}
		}
		if (assign_count == 0) {
			// no need to assign anything: nothing left to do
			return;
		}
		Vector sort_key(LogicalType::BLOB);
		auto modifiers = OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST);
		// slice with a selection vector and generate sort keys
		SelectionVector sel(assign_sel, assign_count);
		Vector sliced_input(arg, sel, assign_count);
		CreateSortKeyHelpers::CreateSortKey(sliced_input, modifiers, sort_key);
		auto sort_key_data = FlatVector::GetData<string_t>(sort_key);

		// now assign sort keys
		for (idx_t i = 0; i < assign_count; i++) {
			const auto sidx = sdata.sel->get_index(sel.get_index(i));
			auto &state = *states[sidx];
			state.AssignArg(sort_key_data[i], aggregate_input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggregate_input_data) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set || !target.value_is_valid ||
		    (source.value_is_valid && COMPARATOR::Operation(source.value, target.value))) {
			target.value_is_valid = source.value_is_valid;
			if (target.value_is_valid) {
				target.AssignBy(source.value, aggregate_input_data);
			}
			target.arg_is_valid = source.arg_is_valid;
			if (target.arg_is_valid) {
				target.AssignArg(source.arg, aggregate_input_data);
			}
			target.is_set = true;
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || !state.arg_is_valid) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.arg, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST));
		}
	}

	template <ArgMinMaxNullHandling NULL_HANDLING>
	static unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &input) {
		auto &context = input.GetClientContext();
		auto &function = input.GetBoundFunction();
		auto &arguments = input.GetArguments();
		ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->GetReturnType());
		function.GetArguments()[0] = arguments[0]->GetReturnType();
		function.GetArguments()[1] = arguments[1]->GetReturnType();
		function.SetReturnType(arguments[0]->GetReturnType());

		auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING);
		return unique_ptr<FunctionData>(std::move(function_data));
	}
};

template <class OP>
bind_aggregate_function_t GetBindFunction(const ArgMinMaxNullHandling null_handling) {
	switch (null_handling) {
	case ArgMinMaxNullHandling::HANDLE_ARG_NULL:
		return OP::template Bind<ArgMinMaxNullHandling::HANDLE_ARG_NULL>;
	case ArgMinMaxNullHandling::HANDLE_ANY_NULL:
		return OP::template Bind<ArgMinMaxNullHandling::HANDLE_ANY_NULL>;
	default:
		return OP::template Bind<ArgMinMaxNullHandling::IGNORE_ANY_NULL>;
	}
}

//! The implementation over the sort keys of the values it is selected by, for a value and a key of any type
template <class OP>
AggregateFunctionCallbacks GetGenericArgMinMaxCallbacks(const ArgMinMaxNullHandling null_handling) {
	using STATE = ArgMinMaxSortKeyState<OP::ORDER>;
	AggregateFunctionCallbacks result;
	result.state_size = AggregateFunction::StateSize<STATE>;
	result.initialize = AggregateFunction::StateInitialize<STATE, OP>;
	result.update = OP::template Update<STATE>;
	result.combine = AggregateFunction::StateCombine<STATE, OP>;
	result.finalize = AggregateFunction::StateVoidFinalize<STATE, OP>;
	result.bind = GetBindFunction<OP>(null_handling);
	result.statistics = AggregateFunction::PropagateInputValueStats;
	AggregateFunction::WireStructStateType<STATE>(result);
	return result;
}

const vector<LogicalType> ArgMaxByTypes() {
	vector<LogicalType> types = {LogicalType::INTEGER,   LogicalType::BIGINT,       LogicalType::HUGEINT,
	                             LogicalType::DOUBLE,    LogicalType::VARCHAR,      LogicalType::DATE,
	                             LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ, LogicalType::BLOB};
	return types;
}

#if !DUCKDB_SMALLER_BINARY(arg_min_max_types)
//! The implementation for a value of any type, selected by a key of a fixed size
template <class OP, class BY_TYPE>
AggregateFunctionCallbacks GetVectorArgMinMaxCallbacks(const ArgMinMaxNullHandling null_handling) {
	using STATE = ArgMinMaxVectorState<OP::ORDER, BY_TYPE>;
	AggregateFunctionCallbacks result;
	result.state_size = AggregateFunction::StateSize<STATE>;
	result.initialize = AggregateFunction::StateInitialize<STATE, OP>;
	result.update = OP::template Update<STATE>;
	result.combine = AggregateFunction::StateCombine<STATE, OP>;
	result.finalize = AggregateFunction::StateVoidFinalize<STATE, OP>;
	result.bind = GetBindFunction<OP>(null_handling);
	result.statistics = AggregateFunction::PropagateInputValueStats;
	AggregateFunction::WireStructStateType<STATE>(result);
	return result;
}

template <class OP>
AggregateFunctionCallbacks GetVectorArgMinMaxCallbacksBy(const LogicalType &by_type,
                                                         const ArgMinMaxNullHandling null_handling) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetVectorArgMinMaxCallbacks<OP, int32_t>(null_handling);
	case PhysicalType::INT64:
		return GetVectorArgMinMaxCallbacks<OP, int64_t>(null_handling);
	case PhysicalType::INT128:
		return GetVectorArgMinMaxCallbacks<OP, hugeint_t>(null_handling);
	case PhysicalType::DOUBLE:
		return GetVectorArgMinMaxCallbacks<OP, double>(null_handling);
	case PhysicalType::VARCHAR:
		return GetVectorArgMinMaxCallbacks<OP, string_t>(null_handling);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

//! The implementation for a value and a key of a fixed size each
template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunctionCallbacks GetArgMinMaxCallbacks(const ArgMinMaxNullHandling null_handling) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto result = AggregateFunction::BinaryAggregateCallbacks<STATE, ARG_TYPE, BY_TYPE, ARG_TYPE, OP>();
	result.bind = GetBindFunction<OP>(null_handling);
	result.statistics = AggregateFunction::PropagateInputValueStats;
	return result;
}

template <class OP, class ARG_TYPE>
AggregateFunctionCallbacks GetArgMinMaxCallbacksBy(const LogicalType &by_type,
                                                   const ArgMinMaxNullHandling null_handling) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxCallbacks<OP, ARG_TYPE, int32_t>(null_handling);
	case PhysicalType::INT64:
		return GetArgMinMaxCallbacks<OP, ARG_TYPE, int64_t>(null_handling);
	case PhysicalType::INT128:
		return GetArgMinMaxCallbacks<OP, ARG_TYPE, hugeint_t>(null_handling);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxCallbacks<OP, ARG_TYPE, double>(null_handling);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxCallbacks<OP, ARG_TYPE, string_t>(null_handling);
	default:
		throw InternalException("Unimplemented arg_min/arg_max by aggregate");
	}
}

//! A DECIMAL value is selected as the integer it is stored as
template <class OP>
AggregateFunctionCallbacks GetDecimalArgMinMaxCallbacks(const LogicalType &by_type, const LogicalType &type,
                                                        ArgMinMaxNullHandling null_handling) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetArgMinMaxCallbacksBy<OP, int16_t>(by_type, null_handling);
	case PhysicalType::INT32:
		return GetArgMinMaxCallbacksBy<OP, int32_t>(by_type, null_handling);
	case PhysicalType::INT64:
		return GetArgMinMaxCallbacksBy<OP, int64_t>(by_type, null_handling);
	case PhysicalType::VARCHAR:
		// wide decimals are BIGNUM blobs
		return GetArgMinMaxCallbacksBy<OP, string_t>(by_type, null_handling);
	default:
		return GetArgMinMaxCallbacksBy<OP, hugeint_t>(by_type, null_handling);
	}
}
#endif

//! Selects the implementation for the types the function is called with: a specialized one if there is one for
//! exactly these types, the generic one over sort keys otherwise
template <class COMPARATOR, OrderType ORDER_TYPE, ArgMinMaxNullHandling NULL_HANDLING>
unique_ptr<FunctionData> BindArgMinMax(BindAggregateFunctionInput &input) {
	// we always use LessThan when using sort keys because the ORDER_TYPE takes care of selecting the lowest or highest
	using GENERIC_VECTOR_OP = VectorArgMinMaxBase<LessThan, ORDER_TYPE, GenericArgMinMaxState<ORDER_TYPE>>;
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto arg_type = arguments[0]->GetReturnType();
	auto by_type = arguments[1]->GetReturnType();

	bool specialized_by = false;
	for (auto &specialized_type : ArgMaxByTypes()) {
		specialized_by = specialized_by || specialized_type.id() == by_type.id();
	}
	auto implementation = GetGenericArgMinMaxCallbacks<GENERIC_VECTOR_OP>(NULL_HANDLING);
#if !DUCKDB_SMALLER_BINARY(arg_min_max_types)
	using OP = ArgMinMaxBase<COMPARATOR>;
	using VECTOR_OP = VectorArgMinMaxBase<COMPARATOR, ORDER_TYPE>;
	if (specialized_by) {
		switch (arg_type.id()) {
		case LogicalTypeId::INTEGER:
			implementation = GetArgMinMaxCallbacksBy<OP, int32_t>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::BIGINT:
			implementation = GetArgMinMaxCallbacksBy<OP, int64_t>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::DOUBLE:
			implementation = GetArgMinMaxCallbacksBy<OP, double>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::BLOB:
			implementation = GetArgMinMaxCallbacksBy<OP, string_t>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::DATE:
			implementation = GetArgMinMaxCallbacksBy<OP, date_t>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			implementation = GetArgMinMaxCallbacksBy<OP, timestamp_t>(by_type, NULL_HANDLING);
			break;
		case LogicalTypeId::DECIMAL:
			implementation = GetDecimalArgMinMaxCallbacks<OP>(by_type, arg_type, NULL_HANDLING);
			break;
		default:
			implementation = GetVectorArgMinMaxCallbacksBy<VECTOR_OP>(by_type, NULL_HANDLING);
			break;
		}
	}
#endif
	// the function is bound to the types already: only its implementation depends on them
	function.SetCallbacks(implementation);
	// the implementation pushes the collation of the key and creates the bind data
	return implementation.bind(input);
}

template <class COMPARATOR, OrderType ORDER_TYPE>
void AddArgMinMaxFunctions(AggregateFunctionSet &fun, const ArgMinMaxNullHandling null_handling) {
	bind_aggregate_function_t bind = BindArgMinMax<COMPARATOR, ORDER_TYPE, ArgMinMaxNullHandling::IGNORE_ANY_NULL>;
	switch (null_handling) {
	case ArgMinMaxNullHandling::IGNORE_ANY_NULL:
		break;
	case ArgMinMaxNullHandling::HANDLE_ARG_NULL:
		bind = BindArgMinMax<COMPARATOR, ORDER_TYPE, ArgMinMaxNullHandling::HANDLE_ARG_NULL>;
		break;
	case ArgMinMaxNullHandling::HANDLE_ANY_NULL:
		bind = BindArgMinMax<COMPARATOR, ORDER_TYPE, ArgMinMaxNullHandling::HANDLE_ANY_NULL>;
		break;
	}
	// the function is declared as the generic implementation over sort keys, the bind specializes it: the argument
	// is returned as it is passed, whatever the value it is selected by
	using GENERIC_VECTOR_OP = VectorArgMinMaxBase<LessThan, ORDER_TYPE, GenericArgMinMaxState<ORDER_TYPE>>;
	auto type = TypeName::VARIABLE("T");
	AggregateFunction function(FunctionSignature()
	                               .AddVariable("T")
	                               .AddVariable("BY")
	                               .AddParameter("arg", type)
	                               .AddParameter("val", TypeName::VARIABLE("BY"))
	                               .SetReturnType(type),
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, bind);
	function.SetCallbacks(GetGenericArgMinMaxCallbacks<GENERIC_VECTOR_OP>(null_handling));
	function.SetBindCallback(bind);
	function.GetProperties().SetProperty(BoundAggregateExpression::NON_NULL_SELECTOR_DOMAIN_PROPERTY,
	                                     Value::BOOLEAN(true));
	fun.AddFunction(function);
}

//------------------------------------------------------------------------------
// ArgMinMax(N) Function
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
// State
//------------------------------------------------------------------------------

template <class A, class B, class COMPARATOR>
class ArgMinMaxNState {
public:
	using VAL_TYPE = A;
	using ARG_TYPE = B;

	using V = typename VAL_TYPE::TYPE;
	using K = typename ARG_TYPE::TYPE;

	BinaryAggregateHeap<K, V, COMPARATOR> heap;

	bool is_initialized = false;
	void Initialize(ArenaAllocator &allocator, idx_t nval) {
		heap.Initialize(allocator, nval);
		is_initialized = true;
	}
};

//------------------------------------------------------------------------------
// Operation
//------------------------------------------------------------------------------
template <class STATE>
void ArgMinMaxNUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                      idx_t count) {
	D_ASSERT(aggr_input.bind_data);
	const auto &bind_data = aggr_input.bind_data->Cast<ArgMinMaxFunctionData>();

	auto &val_vector = inputs[0];
	auto &arg_vector = inputs[1];
	auto &n_vector = inputs[2];

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat arg_format;
	UnifiedVectorFormat n_format;
	UnifiedVectorFormat state_format;

	auto val_extra_state = STATE::VAL_TYPE::CreateExtraState();
	auto arg_extra_state = STATE::ARG_TYPE::CreateExtraState();

	STATE::VAL_TYPE::PrepareData(val_vector, val_extra_state, val_format, bind_data.nulls_last);
	STATE::ARG_TYPE::PrepareData(arg_vector, arg_extra_state, arg_format, bind_data.nulls_last);

	n_vector.ToUnifiedFormat(n_format);
	state_vector.ToUnifiedFormat(state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

	for (idx_t i = 0; i < count; i++) {
		const auto arg_idx = arg_format.sel->get_index(i);
		const auto val_idx = val_format.sel->get_index(i);

		if (bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL &&
		    (!arg_format.validity.RowIsValid(arg_idx) || !val_format.validity.RowIsValid(val_idx))) {
			continue;
		}
		if (bind_data.null_handling == ArgMinMaxNullHandling::HANDLE_ARG_NULL &&
		    !val_format.validity.RowIsValid(val_idx)) {
			continue;
		}

		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		// Initialize the heap if necessary and add the input to the heap
		if (!state.is_initialized) {
			const auto nidx = n_format.sel->get_index(i);
			if (!n_format.validity.RowIsValid(nidx)) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value cannot be NULL");
			}
			const auto nval = UnifiedVectorFormat::GetData<int64_t>(n_format)[nidx];
			if (nval <= 0) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value must be > 0");
			}
			if (nval >= MIN_MAX_N_MAX_VALUE) {
				throw InvalidInputException("Invalid input for arg_min/arg_max: n value must be < %d",
				                            MIN_MAX_N_MAX_VALUE);
			}
			state.Initialize(aggr_input.allocator, UnsafeNumericCast<idx_t>(nval));
		}

		// Now add the input to the heap
		auto arg_val = STATE::ARG_TYPE::Create(arg_format, arg_idx);
		auto val_val = STATE::VAL_TYPE::Create(val_format, val_idx);

		state.heap.Insert(aggr_input.allocator, arg_val, val_val);
	}
}

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
template <class VAL_TYPE, class ARG_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNFunction(BoundAggregateFunction &function) {
	using STATE = ArgMinMaxNState<VAL_TYPE, ARG_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.SetStateSizeCallback(AggregateFunction::StateSize<STATE>);
	function.SetStateInitCallback(AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>);
	function.SetStateCombineCallback(AggregateFunction::StateCombine<STATE, OP>);
	function.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, OP>);

	function.SetStateFinalizeCallback(MinMaxNOperation::Finalize<STATE>);
	function.SetStateUpdateCallback(ArgMinMaxNUpdate<STATE>);
}

template <class VAL_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNFunction(PhysicalType arg_type, BoundAggregateFunction &function) {
	switch (arg_type) {
#if !DUCKDB_SMALLER_BINARY(arg_min_max_n_types)
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxStringValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<int32_t>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<int64_t>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<float>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFixedValue<double>, COMPARATOR>(function);
		break;
#endif
	default:
		SpecializeArgMinMaxNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <class COMPARATOR>
void SpecializeArgMinMaxNFunction(PhysicalType val_type, PhysicalType arg_type, BoundAggregateFunction &function) {
	switch (val_type) {
#if !DUCKDB_SMALLER_BINARY(arg_min_max_n_types)
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNFunction<MinMaxStringValue, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<int32_t>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<int64_t>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<float>, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNFunction<MinMaxFixedValue<double>, COMPARATOR>(arg_type, function);
		break;
#endif
	default:
		SpecializeArgMinMaxNFunction<MinMaxFallbackValue, COMPARATOR>(arg_type, function);
		break;
	}
}

template <class VAL_TYPE, class ARG_TYPE, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(BoundAggregateFunction &function) {
	using STATE = ArgMinMaxNState<VAL_TYPE, ARG_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.SetStateSizeCallback(AggregateFunction::StateSize<STATE>);
	function.SetStateInitCallback(AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>);
	function.SetStateCombineCallback(AggregateFunction::StateCombine<STATE, OP>);
	function.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, OP>);
	function.SetStateFinalizeCallback(MinMaxNOperation::Finalize<STATE>);
	function.SetStateUpdateCallback(ArgMinMaxNUpdate<STATE>);
}

template <class VAL_TYPE, bool NULLS_LAST, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(PhysicalType arg_type, BoundAggregateFunction &function) {
	switch (arg_type) {
#if !DUCKDB_SMALLER_BINARY(arg_min_max_n_types)
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<float, NULLS_LAST>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFixedValueOrNull<double, NULLS_LAST>, COMPARATOR>(function);
		break;
#endif
	default:
		SpecializeArgMinMaxNullNFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <bool NULLS_LAST, class COMPARATOR>
void SpecializeArgMinMaxNullNFunction(PhysicalType val_type, PhysicalType arg_type, BoundAggregateFunction &function) {
	switch (val_type) {
#if !DUCKDB_SMALLER_BINARY(arg_min_max_n_types)
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNullNFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                      function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                      function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<float, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                    function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNullNFunction<MinMaxFixedValueOrNull<double, NULLS_LAST>, NULLS_LAST, COMPARATOR>(arg_type,
		                                                                                                     function);
		break;
#endif
	default:
		SpecializeArgMinMaxNullNFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	}
}

template <ArgMinMaxNullHandling NULL_HANDLING, bool NULLS_LAST, class COMPARATOR>
unique_ptr<FunctionData> ArgMinMaxNBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	for (auto &arg : arguments) {
		if (arg->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	ExpressionBinder::PushCollation(context, arguments[1], arguments[1]->GetReturnType());
	// the value is replaced by its comparison key, which may be of another type
	function.GetArguments()[1] = arguments[1]->GetReturnType();

	const auto val_type = arguments[0]->GetReturnType().InternalType();
	const auto arg_type = arguments[1]->GetReturnType().InternalType();
	function.SetReturnType(LogicalType::LIST(arguments[0]->GetReturnType()));

	// Specialize the function based on the input types
	auto function_data = make_uniq<ArgMinMaxFunctionData>(NULL_HANDLING, NULLS_LAST);
	if (NULL_HANDLING != ArgMinMaxNullHandling::IGNORE_ANY_NULL) {
		SpecializeArgMinMaxNullNFunction<NULLS_LAST, COMPARATOR>(val_type, arg_type, function);
	} else {
		SpecializeArgMinMaxNFunction<COMPARATOR>(val_type, arg_type, function);
	}

	return unique_ptr<FunctionData>(std::move(function_data));
}

template <ArgMinMaxNullHandling NULL_HANDLING, bool NULLS_LAST, class COMPARATOR>
void AddArgMinMaxNFunction(AggregateFunctionSet &set) {
	auto type = TypeName::VARIABLE("T");
	AggregateFunction function(FunctionSignature()
	                               .AddVariable("T")
	                               .AddVariable("BY")
	                               .AddParameter("arg", type)
	                               .AddParameter("val", TypeName::VARIABLE("BY"))
	                               .AddParameter("N", LogicalType::BIGINT)
	                               .SetReturnType(TypeName::LIST(type)),
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           ArgMinMaxNBind<NULL_HANDLING, NULLS_LAST, COMPARATOR>);
	function.SetFallible(); // the update callback rejects NULL and out-of-range n values at runtime

	set.AddFunction(function);
}

} // namespace

//------------------------------------------------------------------------------
// Function Registration
//------------------------------------------------------------------------------

AggregateFunctionSet ArgMinFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::IGNORE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::IGNORE_ANY_NULL, true, LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::IGNORE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::IGNORE_ANY_NULL, false, GreaterThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMinNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ARG_NULL);
	return fun;
}

AggregateFunctionSet ArgMaxNullFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ARG_NULL);
	return fun;
}

AggregateFunctionSet ArgMinNullsLastFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan, OrderType::ASCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::HANDLE_ANY_NULL, true, LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxNullsLastFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan, OrderType::DESCENDING>(fun, ArgMinMaxNullHandling::HANDLE_ANY_NULL);
	AddArgMinMaxNFunction<ArgMinMaxNullHandling::HANDLE_ANY_NULL, false, GreaterThan>(fun);
	return fun;
}

} // namespace duckdb
