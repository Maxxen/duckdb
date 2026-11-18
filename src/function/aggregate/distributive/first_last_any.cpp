#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/aggregate_rewrite.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace {

template <bool LAST, bool SKIP_NULLS>
unique_ptr<Expression> RewriteOrderedFirst(AggregateRewriteInput &input) {
	if (!input.aggregate.GetOrderBys()) {
		return nullptr;
	}
	auto aggregate = unique_ptr_cast<Expression, BoundAggregateExpression>(input.aggregate.Copy());

	FunctionBinder binder(input.context);
	vector<unique_ptr<Expression>> sort_children;
	for (auto &order : aggregate->GetOrderBysMutable()->orders) {
		sort_children.emplace_back(std::move(order.expression));
		sort_children.emplace_back(make_uniq<BoundConstantExpression>(Value(order.GetOrderModifier())));
	}
	aggregate->GetOrderBysMutable().reset();

	auto sort_key = binder.BindScalarFunction(Identifier::DefaultSchema(), "create_sort_key", std::move(sort_children));

	auto children = std::move(aggregate->GetChildrenMutable());
	children.emplace_back(std::move(sort_key));
	const char *function_name;
	if (LAST) {
		function_name = "arg_max_null";
	} else if (SKIP_NULLS) {
		function_name = "arg_min";
	} else {
		function_name = "arg_min_null";
	}

	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    input.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), function_name));
	vector<LogicalType> child_types;
	for (auto &child : children) {
		child_types.push_back(child->GetReturnType());
	}
	const auto &function = entry.functions.GetFunctionByArguments(input.context, child_types);
	auto result = binder.BindAggregateFunction(function, std::move(children), std::move(aggregate->GetFilterMutable()),
	                                           aggregate->GetAggregateType());
	return unique_ptr_cast<BoundAggregateExpression, Expression>(std::move(result));
}

//! The aggregate state of first/last/any_value is nullable on two levels: the state itself is NULL when no row has
//! been seen yet (is_set, the outer optional), and the recorded value can itself be NULL (value_is_valid, the inner
//! optional). Valid exported states are e.g. NULL, {'value': NULL} and {'value': 42}.
template <class T>
struct FirstState {
	static constexpr const char *STATE_NAMES[] = {"value"};
	//! The value is exported with the aggregate's return type (e.g. DATE or UUID instead of the physical type)
	using STATE_TYPE = OptionalStateType<StructStateType<OptionalStateType<StateTypedValue<T, StateReturnType>>>>;

	using VALUE_TYPE = T;
	T value;
	//! Whether the recorded value is valid (i.e. not NULL)
	bool value_is_valid;
	//! Whether the state has been set (i.e. we have seen a row)
	bool is_set;
};

//! String state variant: strings are stored in the arena allocator, re-using the previous allocation
//! when overwriting the value (relevant for last, which overwrites the value for every row).
struct FirstStringStateBase {
	string_t value;
	//! Whether the recorded value is valid (i.e. not NULL)
	bool value_is_valid;
	//! Whether the state has been set (i.e. we have seen a row)
	bool is_set;
	//! The size of the arena allocation for a non-inlined string value - not part of the exported state.
	//! Must be wide enough for NextPowerOfTwo(MAX_STRING_SIZE), which does not fit in a uint32_t.
	idx_t alloc_size;

	void Assign(string_t input, AggregateInputData &input_data) {
		if (input.IsInlined()) {
			value = input;
			alloc_size = 0;
		} else {
			auto len = input.GetSize();
			char *ptr;
			if (alloc_size >= len) {
				ptr = value.GetDataWriteable();
			} else {
				alloc_size = NextPowerOfTwo(len);
				ptr = char_ptr_cast(input_data.allocator.Allocate(alloc_size));
			}
			memcpy(ptr, input.GetData(), len);
			value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
		}
	}
};

struct FirstStringState : FirstStringStateBase {
	static constexpr const char *STATE_NAMES[] = {"value"};
	//! The value is exported with the aggregate's return type - it can be e.g. a VARCHAR, BLOB or BIT value
	using STATE_TYPE = OptionalStateType<StructStateType<OptionalStateType<StateString<StateReturnType>>>>;
};

//! State for arbitrary types - the value is stored as a binary sort key, exported as the aggregate's return type.
struct FirstSortKeyState : FirstStringStateBase {
	static constexpr const char *STATE_NAMES[] = {"value"};
	using STATE_TYPE =
	    OptionalStateType<StructStateType<OptionalStateType<StateSortKey<StateReturnType, OrderType::ASCENDING>>>>;
};

struct FirstFunctionBase {
	static bool IgnoreNull() {
		return false;
	}
};

template <bool LAST, class FUNC>
static inline void ScanClusterRange(idx_t pos, idx_t end, FUNC &&func) {
	if constexpr (LAST) {
		for (idx_t k = end; k > pos; k--) {
			if (func(k - 1)) {
				return;
			}
		}
	} else {
		for (idx_t k = pos; k < end; k++) {
			if (func(k)) {
				return;
			}
		}
	}
}

template <bool LAST, bool SKIP_NULLS>
struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			if (!unary_input.RowIsValid()) {
				if (!SKIP_NULLS) {
					state.is_set = true;
					state.value_is_valid = false;
				}
			} else {
				state.is_set = true;
				state.value_is_valid = true;
				state.value = input;
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	// Clustered early-exit: first/any_value scan forward and stop at the first valid value;
	// last scans backward and stops at the first valid value from the end.
	template <class INPUT_TYPE, class STATE_TYPE, class OP, bool ALL_VALID>
	static void ClusteredOpInternal(STATE_TYPE &state, const INPUT_TYPE *vals, const sel_t *sel,
	                                const SelectionVector &isel, const ValidityMask &validity, idx_t pos, idx_t end) {
		if (!LAST && state.is_set) {
			return;
		}
		ScanClusterRange<LAST>(pos, end, [&](idx_t k) {
			auto idx = isel.get_index(sel ? sel[k] : k);
			if (ALL_VALID || validity.RowIsValidUnsafe(idx)) {
				state.is_set = true;
				state.value_is_valid = true;
				state.value = vals[idx];
				return true;
			}
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.value_is_valid = false;
				return true;
			}
			return false;
		});
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ClusteredOp(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &input, const sel_t *sel,
	                        const SelectionVector &isel, const ValidityMask &validity, idx_t pos, idx_t end) {
		if (validity.CanHaveNull()) {
			ClusteredOpInternal<INPUT_TYPE, STATE_TYPE, OP, false>(state, vals, sel, isel, validity, pos, end);
		} else {
			ClusteredOpInternal<INPUT_TYPE, STATE_TYPE, OP, true>(state, vals, sel, isel, validity, pos, end);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.is_set) {
			target = source;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || !state.value_is_valid) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionStringBase : public FirstFunctionBase {
	template <class STATE>
	static void SetValue(STATE &state, AggregateInputData &input_data, string_t value, bool is_null) {
		if (is_null) {
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.value_is_valid = false;
			}
		} else {
			state.is_set = true;
			state.value_is_valid = true;
			state.Assign(value, input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (source.is_set && (LAST || !target.is_set)) {
			SetValue<STATE>(target, input_data, source.value, !source.value_is_valid);
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionString : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, unary_input.input, input,
			                                                                    !unary_input.RowIsValid());
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP, bool ALL_VALID>
	static void ClusteredOpInternal(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &input,
	                                const sel_t *sel, const SelectionVector &isel, const ValidityMask &validity,
	                                idx_t pos, idx_t end) {
		if (!LAST && state.is_set) {
			return;
		}
		ScanClusterRange<LAST>(pos, end, [&](idx_t k) {
			auto idx = isel.get_index(sel ? sel[k] : k);
			bool is_null = ALL_VALID ? false : !validity.RowIsValidUnsafe(idx);
			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE_TYPE>(state, input.input, vals[idx],
			                                                                         is_null);
			return state.is_set;
		});
	}

	template <class INPUT_TYPE, class STATE_TYPE, class OP>
	static void ClusteredOp(STATE_TYPE &state, const INPUT_TYPE *vals, AggregateUnaryInput &input, const sel_t *sel,
	                        const SelectionVector &isel, const ValidityMask &validity, idx_t pos, idx_t end) {
		if (validity.CanHaveNull()) {
			ClusteredOpInternal<INPUT_TYPE, STATE_TYPE, OP, false>(state, vals, input, sel, isel, validity, pos, end);
		} else {
			ClusteredOpInternal<INPUT_TYPE, STATE_TYPE, OP, true>(state, vals, input, sel, isel, validity, pos, end);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || !state.value_is_valid) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstVectorFunction : FirstFunctionStringBase<LAST, SKIP_NULLS> {
	using STATE = FirstSortKeyState;

	static void Update(Vector inputs[], AggregateInputData &input_data, idx_t, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(sdata);
		auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);

		// Gather the rows that can still be assigned before touching the input: reading the values can be expensive
		// (a shredded VARIANT has to be materialized), while in a grouped aggregate the states are typically set
		// after the first few chunks, leaving nothing to do.
		sel_t assign_sel[STANDARD_VECTOR_SIZE];
		idx_t assign_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!LAST && states[sdata.sel->get_index(i)]->is_set) {
				continue;
			}
			assign_sel[assign_count++] = NumericCast<sel_t>(i);
		}
		if (assign_count == 0) {
			// fast path - nothing to set
			return;
		}

		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(idata);

		Vector sort_key(LogicalType::BLOB);
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		// slice with a selection vector and generate sort keys
		if (assign_count == count) {
			CreateSortKeyHelpers::CreateSortKey(input, modifiers, sort_key);
		} else {
			SelectionVector sel(assign_sel, STANDARD_VECTOR_SIZE);
			Vector sliced_input(input, sel, assign_count);
			CreateSortKeyHelpers::CreateSortKey(sliced_input, modifiers, sort_key);
		}
		auto sort_key_data = FlatVector::GetData<string_t>(sort_key);

		// now assign sort keys
		for (idx_t i = 0; i < assign_count; i++) {
			const auto idx = idata.sel->get_index(assign_sel[i]);
			bool is_null = !idata.validity.RowIsValid(idx);
			if (SKIP_NULLS && is_null) {
				continue;
			}

			const auto state_idx = sdata.sel->get_index(assign_sel[i]);
			auto &state = *states[state_idx];
			if (!LAST && state.is_set) {
				continue;
			}

			FirstFunctionStringBase<LAST, SKIP_NULLS>::template SetValue<STATE>(state, input_data, sort_key_data[i],
			                                                                    is_null);
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || !state.value_is_valid) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.value, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		}
	}
};

template <class T, bool LAST, bool SKIP_NULLS>
void FirstFunctionClusterUpdate(Vector inputs[], AggregateInputData &aggregate_input_data, idx_t input_count,
                                const ClusteredAggr &clustered, idx_t count) {
	D_ASSERT(input_count == 1);
	UnifiedVectorFormat idata;
	inputs[0].ToUnifiedFormat(idata);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	AggregateUnaryInput unary_input(aggregate_input_data, idata.validity);
	for (auto &run : clustered.runs()) {
		auto &state = *reinterpret_cast<FirstState<T> *>(run.state);
		const auto *run_sel = run.sel;
		const auto run_count = run.count;
		FirstFunction<LAST, SKIP_NULLS>::template ClusteredOp<T, FirstState<T>, FirstFunction<LAST, SKIP_NULLS>>(
		    state, input_data, unary_input, run_sel, *idata.sel, idata.validity, 0, run_count);
	}
}

//! The implementation over a value of a fixed size
template <class T, bool LAST, bool SKIP_NULLS>
AggregateFunctionCallbacks GetFirstCallbacksTemplated() {
	using STATE = FirstState<T>;
	using OP = FirstFunction<LAST, SKIP_NULLS>;
	AggregateFunctionCallbacks result;
	result.state_size = AggregateFunction::StateSize<STATE>;
	result.initialize = AggregateFunction::StateInitialize<STATE, OP>;
	result.update = AggregateFunction::UnaryScatterUpdate<STATE, T, OP>;
	result.combine = AggregateFunction::StateCombine<STATE, OP>;
	result.finalize = AggregateFunction::StateFinalize<STATE, T, OP>;
	result.cluster_update = FirstFunctionClusterUpdate<T, LAST, SKIP_NULLS>;
	AggregateFunction::WireStructStateType<STATE>(result);
	return result;
}

//! The implementation for a type: it only depends on how the values of the type are stored
template <bool LAST, bool SKIP_NULLS>
AggregateFunctionCallbacks GetFirstCallbacks(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetFirstCallbacksTemplated<int8_t, LAST, SKIP_NULLS>();
	case PhysicalType::INT16:
		return GetFirstCallbacksTemplated<int16_t, LAST, SKIP_NULLS>();
	case PhysicalType::INT32:
		return GetFirstCallbacksTemplated<int32_t, LAST, SKIP_NULLS>();
	case PhysicalType::INT64:
		return GetFirstCallbacksTemplated<int64_t, LAST, SKIP_NULLS>();
	case PhysicalType::UINT8:
		return GetFirstCallbacksTemplated<uint8_t, LAST, SKIP_NULLS>();
	case PhysicalType::UINT16:
		return GetFirstCallbacksTemplated<uint16_t, LAST, SKIP_NULLS>();
	case PhysicalType::UINT32:
		return GetFirstCallbacksTemplated<uint32_t, LAST, SKIP_NULLS>();
	case PhysicalType::UINT64:
		return GetFirstCallbacksTemplated<uint64_t, LAST, SKIP_NULLS>();
	case PhysicalType::INT128:
		return GetFirstCallbacksTemplated<hugeint_t, LAST, SKIP_NULLS>();
	case PhysicalType::UINT128:
		return GetFirstCallbacksTemplated<uhugeint_t, LAST, SKIP_NULLS>();
	case PhysicalType::FLOAT:
		return GetFirstCallbacksTemplated<float, LAST, SKIP_NULLS>();
	case PhysicalType::DOUBLE:
		return GetFirstCallbacksTemplated<double, LAST, SKIP_NULLS>();
	case PhysicalType::INTERVAL:
		return GetFirstCallbacksTemplated<interval_t, LAST, SKIP_NULLS>();
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateCallbacks<FirstStringState, string_t, string_t,
		                                                  FirstFunctionString<LAST, SKIP_NULLS>>();
	default: {
		using OP = FirstVectorFunction<LAST, SKIP_NULLS>;
		using STATE = FirstSortKeyState;
		AggregateFunctionCallbacks result;
		result.state_size = AggregateFunction::StateSize<STATE>;
		result.initialize = AggregateFunction::StateInitialize<STATE, OP>;
		result.update = OP::Update;
		result.combine = AggregateFunction::StateCombine<STATE, OP>;
		result.finalize = AggregateFunction::StateVoidFinalize<STATE, OP>;
		AggregateFunction::WireStructStateType<STATE>(result);
		return result;
	}
	}
}

//! Selects the implementation for the type the function is called with
template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindFirst(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	function.SetCallbacks(GetFirstCallbacks<LAST, SKIP_NULLS>(arguments[0]->GetReturnType()));
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	function.SetDirectRewriteCallback(RewriteOrderedFirst<LAST, SKIP_NULLS>);
	function.SetSingleValueIdentity(true);
	function.SetStatisticsCallback(AggregateFunction::PropagateInputValueStats);
	return nullptr;
}

template <bool LAST, bool SKIP_NULLS>
void AddFirstOperator(AggregateFunctionSet &set) {
	// the value is returned as it is passed
	auto type = LogicalType::TEMPLATE("T");
	AggregateFunction fun(FunctionSignature().AddParameter("arg", type).SetReturnType(type), nullptr, nullptr, nullptr,
	                      nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                      BindFirst<LAST, SKIP_NULLS>);
	set.AddFunction(fun);
}

} // namespace

AggregateFunctionSet FirstFun::GetFunctions() {
	AggregateFunctionSet first("first");
	AddFirstOperator<false, false>(first);
	return first;
}

AggregateFunctionSet LastFun::GetFunctions() {
	AggregateFunctionSet last("last");
	AddFirstOperator<true, false>(last);
	return last;
}

AggregateFunctionSet AnyValueFun::GetFunctions() {
	AggregateFunctionSet any_value("any_value");
	AddFirstOperator<false, true>(any_value);
	return any_value;
}

} // namespace duckdb
