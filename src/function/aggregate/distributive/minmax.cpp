#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/minmax_n_helpers.hpp"
#include "duckdb/function/aggregate/sort_key_helpers.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

namespace {

template <class T>
struct MinMaxState {
	using value_type = T;
	//! The value is exported with the aggregate's return type (e.g. DATE or UUID instead of the physical type)
	using STATE_TYPE = OptionalStateType<StateTypedValue<T, StateReturnType>>;
	T value;
	bool is_set;
};

//! The implementation over a value of a fixed size
template <class OP>
static AggregateFunctionCallbacks GetUnaryCallbacks(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<int8_t>, int8_t, int8_t, OP>();
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<int8_t>, int8_t, int8_t, OP>();
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<int16_t>, int16_t, int16_t, OP>();
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<int32_t>, int32_t, int32_t, OP>();
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<int64_t>, int64_t, int64_t, OP>();
	case PhysicalType::UINT8:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<uint8_t>, uint8_t, uint8_t, OP>();
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<uint16_t>, uint16_t, uint16_t, OP>();
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<uint32_t>, uint32_t, uint32_t, OP>();
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<uint64_t>, uint64_t, uint64_t, OP>();
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<hugeint_t>, hugeint_t, hugeint_t, OP>();
	case PhysicalType::UINT128:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<uhugeint_t>, uhugeint_t, uhugeint_t, OP>();
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<float>, float, float, OP>();
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<double>, double, double, OP>();
	case PhysicalType::INTERVAL:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxState<interval_t>, interval_t, interval_t, OP>();
	default:
		throw InternalException("Unimplemented type for min/max aggregate");
	}
}

struct MinMaxBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (!state.is_set) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input, unary_input.input);
			state.is_set = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input, unary_input.input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class REDUCE_OP>
struct NumericMinMaxBase : public MinMaxBase, public ClusteredStateCopy {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		state.value = input;
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		if (!local.is_set) {
			local.value = input;
			local.is_set = true;
		} else {
			local.value = REDUCE_OP::template Operation<INPUT_TYPE>(local.value, input);
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		if (count != 0) {
			UpdateClusteredLocal(local, input);
		}
	}
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		state.value = REDUCE_OP::template Operation<INPUT_TYPE>(state.value, input);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set) {
			target = source;
		} else {
			using value_type = decltype(target.value);
			target.value = REDUCE_OP::template Operation<value_type>(target.value, source.value);
		}
	}

	template <class STATE, class OP>
	static void RepeatedCombine(const STATE &source, STATE &target, AggregateInputData &input, idx_t count) {
		if (count == 0) {
			return;
		}
		Combine<STATE, OP>(source, target, input);
	}
};

using MinOperation = NumericMinMaxBase<Min>;
using MaxOperation = NumericMinMaxBase<Max>;

//! Shared state layout for both MinMaxStringState and MinMaxSortKeyState.
//! Stores a string_t value (regular string or sort key blob) with power-of-two allocation reuse.
struct BaseMinMaxStringState {
	string_t value;
	bool is_set;
	//! Must be wide enough for NextPowerOfTwo(MAX_STRING_SIZE), which does not fit in a uint32_t
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

struct MinMaxStringState : BaseMinMaxStringState {
	//! The value is exported with the aggregate's return type - it can be e.g. a VARCHAR, BLOB or BIT value
	using STATE_TYPE = OptionalStateType<StateString<StateReturnType>>;
};

struct StringMinMaxBase : public MinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		state.Assign(input, input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.is_set) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.is_set) {
			// target is NULL, use source value directly
			Assign(target, source.value, input_data);
			target.is_set = true;
		} else {
			OP::template Execute<string_t, STATE>(target, source.value, input_data);
		}
	}

	template <class STATE, class OP>
	static void RepeatedCombine(const STATE &source, STATE &target, AggregateInputData &input, idx_t count) {
		if (count == 0) {
			return;
		}
		Combine<STATE, OP>(source, target, input);
	}
};

template <class COMPARE>
struct StringMinMaxOperation : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (COMPARE::template Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}
};

using MinOperationString = StringMinMaxOperation<LessThan>;
using MaxOperationString = StringMinMaxOperation<GreaterThan>;

template <OrderType ORDER_TYPE_TEMPLATED>
struct VectorMinMaxBase {
	static constexpr OrderType ORDER_TYPE = ORDER_TYPE_TEMPLATED;

	static bool IgnoreNull() {
		return true;
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		state.Assign(input, input_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (!state.is_set) {
			Assign(state, input, input_data);
			state.is_set = true;
			return;
		}
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.is_set) {
			// source is NULL, nothing to do
			return;
		}
		OP::template Execute<string_t, STATE, OP>(target, source.value, input_data);
	}

	template <class STATE, class OP>
	static void RepeatedCombine(const STATE &source, STATE &target, AggregateInputData &input, idx_t count) {
		if (count == 0) {
			return;
		}
		Combine<STATE, OP>(source, target, input);
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			CreateSortKeyHelpers::DecodeSortKey(state.value, finalize_data.result, finalize_data.result_idx,
			                                    OrderModifiers(ORDER_TYPE, OrderByNullType::NULLS_LAST));
		}
	}
};

struct MinOperationVector : VectorMinMaxBase<OrderType::ASCENDING> {};

struct MaxOperationVector : VectorMinMaxBase<OrderType::DESCENDING> {};

template <OrderType ORDER>
struct MinMaxSortKeyState : BaseMinMaxStringState {
	using STATE_TYPE = OptionalStateType<StateSortKey<StateReturnType, ORDER>>;
};

//! The implementation over the sort keys of the values, for the types that do not compare by their bytes
template <typename OP>
static AggregateFunctionCallbacks GetSortKeyCallbacks() {
	using STATE = MinMaxSortKeyState<OP::ORDER_TYPE>;
	AggregateFunctionCallbacks result;
	result.state_size = AggregateFunction::StateSize<STATE>;
	result.initialize = AggregateFunction::StateInitialize<STATE, OP>;
	result.update = AggregateSortKeyHelpers::UnaryUpdate<STATE, OP, OP::ORDER_TYPE, false>;
	result.combine = AggregateFunction::StateCombine<STATE, OP>;
	result.finalize = AggregateFunction::StateVoidFinalize<STATE, OP>;
	AggregateFunction::WireStructStateType<STATE>(result);
	return result;
}

//! The implementation for a type: it only depends on how the values of the type are stored
template <class OP, class OP_STRING, class OP_VECTOR>
static AggregateFunctionCallbacks GetMinMaxCallbacks(const LogicalType &type) {
	auto internal_type = type.InternalType();
	switch (internal_type) {
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateCallbacks<MinMaxStringState, string_t, string_t, OP_STRING>();
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		return GetSortKeyCallbacks<OP_VECTOR>();
	default:
		return GetUnaryCallbacks<OP>(internal_type);
	}
}

shared_ptr<const AggregateFunction> GetCollatedMinMaxFunction(ClientContext &context, const Identifier &name,
                                                              const vector<LogicalType> &types) {
	const auto function_name = name == "min" ? "arg_min" : "arg_max";
	QueryErrorContext error_context;
	auto func = Catalog::GetEntry<AggregateFunctionCatalogEntry>(
	    context, QualifiedName(Identifier(), Identifier(), Identifier(function_name)), OnEntryNotFound::RETURN_NULL,
	    error_context);
	if (!func) {
		throw NotImplementedException(
		    "Failure while binding function %s using collations - arg_min/arg_max do not exist in the "
		    "catalog - load the core_functions module to fix this issue",
		    name);
	}

	return func->functions.GetFunctionByArguments(context, types);
}

//! Whether min/max over the type are computed through arg_min/arg_max over the collated value
static bool MinMaxUsesCollation(ClientContext &context, const LogicalType &input_type) {
	// The generic non-VARCHAR collation path is not ready yet (see internal #8704). BIT, TIMETZ and VARIANT use
	// explicit binary-comparable keys so min/max follows the same logical order as comparisons and ORDER BY.
	const auto varchar_collation =
	    input_type.id() == LogicalTypeId::VARCHAR &&
	    (!StringType::GetCollation(input_type).empty() || !Settings::Get<DefaultCollationSetting>(context).empty());
	const auto nested_collation = StructType::IsStruct(input_type) || input_type.id() == LogicalTypeId::LIST ||
	                              input_type.id() == LogicalTypeId::ARRAY;
	return input_type.id() == LogicalTypeId::BIT || input_type.id() == LogicalTypeId::TIME_TZ ||
	       input_type.id() == LogicalTypeId::VARIANT || varchar_collation || nested_collation;
}

template <class OP, class OP_STRING, class OP_VECTOR>
unique_ptr<FunctionData> BindMinMax(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto input_type = arguments[0]->GetReturnType();

	const auto collation = MinMaxUsesCollation(context, input_type);
	auto collated_arg = collation ? arguments[0]->Copy() : nullptr;
	if (collation && ExpressionBinder::PushCollation(context, collated_arg, collated_arg->GetReturnType())) {
		// If aggr function is min/max and uses collations, replace bound_function with arg_min/arg_max
		// to make sure the result's correctness.
		vector<LogicalType> types {arguments[0]->GetReturnType(), collated_arg->GetReturnType()};
		function.ReplaceImplementation(input.GetClientContext(),
		                               *GetCollatedMinMaxFunction(context, function.GetName(), types));

		// Bind function like arg_min/arg_max: its bind selects the implementation for these types.
		arguments.push_back(std::move(collated_arg));
		auto bind_data = function.GetCallbacks().GetBindCallback()(input);
		function.SetSingleValueIdentity(true);
		return bind_data;
	}

	if (input_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	// the function is bound to the type already: only its implementation depends on it
	auto state_export_type = function.GetStateTypeCallback();
	auto implementation = GetMinMaxCallbacks<OP, OP_STRING, OP_VECTOR>(input_type);
	if (state_export_type) {
		implementation.get_state_type = state_export_type;
	}
	implementation.statistics = AggregateFunction::PropagateInputValueStats;
	function.SetCallbacks(implementation);
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	function.SetSingleValueIdentity(true);
	return nullptr;
}

//! BindMinMax replaces a collated min/max with arg_min/arg_max over the collation key, so the bound call has one
//! more argument than the definition and must be rendered under the replacement's own name
static unique_ptr<FunctionExpression> MinMaxUnbind(AggregateFunctionUnbindInput &input) {
	auto &function = input.expression.Function();
	auto name = function.GetDefinition()->GetQualifiedName();
	if (input.children.size() == function.GetLogicalArguments().size() + 1) {
		name = function.GetQualifiedName();
	} else if (input.children.size() != function.GetLogicalArguments().size()) {
		return nullptr;
	}
	return make_uniq<FunctionExpression>(name, std::move(input.children));
}

template <class OP, class OP_STRING, class OP_VECTOR>
AggregateFunction GetMinMaxOperator(const string &name) {
	auto type = TypeName::VARIABLE("T");
	AggregateFunction fun(Identifier(name),
	                      FunctionSignature().AddVariable("T").AddParameter("arg", type).SetReturnType(type), nullptr,
	                      nullptr, nullptr, nullptr, nullptr, nullptr, BindMinMax<OP, OP_STRING, OP_VECTOR>);
	fun.SetUnbindCallback(MinMaxUnbind);
	return fun;
}

} // namespace

AggregateFunction MinFunction::GetFunction() {
	return GetMinMaxOperator<MinOperation, MinOperationString, MinOperationVector>("min");
}

AggregateFunction MaxFunction::GetFunction() {
	return GetMinMaxOperator<MaxOperation, MaxOperationString, MaxOperationVector>("max");
}

//---------------------------------------------------
// MinMaxN
//---------------------------------------------------
namespace {

template <class A, class COMPARATOR>
class MinMaxNState {
public:
	using VAL_TYPE = A;
	using T = typename VAL_TYPE::TYPE;

	UnaryAggregateHeap<T, COMPARATOR> heap;
	bool is_initialized = false;

	void Initialize(ArenaAllocator &allocator, idx_t nval) {
		heap.Initialize(allocator, nval);
		is_initialized = true;
	}

	static T GetValue(T val) {
		return val;
	}
};

template <class STATE>
void MinMaxNUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                   idx_t count) {
	auto &val_vector = inputs[0];
	auto &n_vector = inputs[1];

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat n_format;
	UnifiedVectorFormat state_format;

	auto val_extra_state = STATE::VAL_TYPE::CreateExtraState();

	STATE::VAL_TYPE::PrepareData(val_vector, val_extra_state, val_format, true);

	n_vector.ToUnifiedFormat(n_format);
	state_vector.ToUnifiedFormat(state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

	for (idx_t i = 0; i < count; i++) {
		const auto val_idx = val_format.sel->get_index(i);
		if (!val_format.validity.RowIsValid(val_idx)) {
			continue;
		}
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		// Initialize the heap if necessary and add the input to the heap
		if (!state.is_initialized) {
			const auto nidx = n_format.sel->get_index(i);
			if (!n_format.validity.RowIsValid(nidx)) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value cannot be NULL");
			}
			const auto nval = UnifiedVectorFormat::GetData<int64_t>(n_format)[nidx];
			if (nval <= 0) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value must be > 0");
			}
			if (nval >= MIN_MAX_N_MAX_VALUE) {
				throw InvalidInputException("Invalid input for MIN/MAX: n value must be < %d", MIN_MAX_N_MAX_VALUE);
			}
			state.Initialize(aggr_input.allocator, UnsafeNumericCast<idx_t>(nval));
		}

		// Now add the input to the heap
		auto val_val = STATE::VAL_TYPE::Create(val_format, val_idx);
		state.heap.Insert(aggr_input.allocator, val_val);
	}
}

template <class VAL_TYPE, class COMPARATOR>
void SpecializeMinMaxNFunction(BoundAggregateFunction &function) {
	using STATE = MinMaxNState<VAL_TYPE, COMPARATOR>;
	using OP = MinMaxNOperation;

	function.GetCallbacks().SetStateSizeCallback(AggregateFunction::StateSize<STATE>);
	function.GetCallbacks().SetStateInitCallback(
	    AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>);
	function.GetCallbacks().SetStateCombineCallback(AggregateFunction::StateCombine<STATE, OP>);
	function.GetCallbacks().SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, OP>);

	function.GetCallbacks().SetStateFinalizeCallback(MinMaxNOperation::Finalize<STATE>);
	function.GetCallbacks().SetStateUpdateCallback(MinMaxNUpdate<STATE>);
}

template <class COMPARATOR>
void SpecializeMinMaxNFunction(PhysicalType arg_type, BoundAggregateFunction &function) {
	switch (arg_type) {
	case PhysicalType::VARCHAR:
		SpecializeMinMaxNFunction<MinMaxStringValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeMinMaxNFunction<MinMaxFixedValue<int32_t>, COMPARATOR>(function);
		break;
	case PhysicalType::INT64:
		SpecializeMinMaxNFunction<MinMaxFixedValue<int64_t>, COMPARATOR>(function);
		break;
	case PhysicalType::FLOAT:
		SpecializeMinMaxNFunction<MinMaxFixedValue<float>, COMPARATOR>(function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeMinMaxNFunction<MinMaxFixedValue<double>, COMPARATOR>(function);
		break;
	default:
		SpecializeMinMaxNFunction<MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <class COMPARATOR>
unique_ptr<FunctionData> MinMaxNBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	for (const auto &arg : arguments) {
		if (arg->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	if (MinMaxUsesCollation(context, arguments[0]->GetReturnType())) {
		auto collated_arg = arguments[0]->Copy();
		if (ExpressionBinder::PushCollation(context, collated_arg, collated_arg->GetReturnType())) {
			vector<LogicalType> types {arguments[0]->GetReturnType(), collated_arg->GetReturnType(),
			                           arguments[1]->GetReturnType()};
			auto collated_function = GetCollatedMinMaxFunction(context, function.GetName(), types);

			vector<unique_ptr<Expression>> collated_arguments;
			collated_arguments.reserve(3);
			collated_arguments.push_back(std::move(arguments[0]));
			collated_arguments.push_back(std::move(collated_arg));
			collated_arguments.push_back(std::move(arguments[1]));
			FunctionBinder function_binder(context);
			auto expr = function_binder.BindAggregateFunction(collated_function, std::move(collated_arguments));
			arguments = std::move(expr->GetChildrenMutable());
			function.ReplaceImplementation(expr->Function());
			return expr->BindInfo() ? expr->BindInfo()->Copy() : nullptr;
		}
	}

	const auto val_type = arguments[0]->GetReturnType().InternalType();

	// Specialize the function based on the input types
	SpecializeMinMaxNFunction<COMPARATOR>(val_type, function);
	return nullptr;
}

template <class COMPARATOR>
AggregateFunction GetMinMaxNFunction() {
	auto type = TypeName::VARIABLE("T");
	AggregateFunction fun(FunctionSignature()
	                          .AddVariable("T")
	                          .AddParameter("arg", type)
	                          .AddParameter("n", LogicalType::BIGINT)
	                          .SetReturnType(TypeName::LIST(type)),
	                      nullptr, nullptr, nullptr, nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                      nullptr, MinMaxNBind<COMPARATOR>, nullptr);
	fun.SetUnbindCallback(MinMaxUnbind);
	fun.SetFallible(); // the update callback rejects NULL and out-of-range n values at runtime
	return fun;
}

} // namespace
//---------------------------------------------------
// Function Registration
//---------------------------------------------------s
AggregateFunctionSet MinFun::GetFunctions() {
	AggregateFunctionSet min("min");
	min.AddFunction(MinFunction::GetFunction());
	min.AddFunction(GetMinMaxNFunction<LessThan>());
	return min;
}

AggregateFunctionSet MaxFun::GetFunctions() {
	AggregateFunctionSet max("max");
	max.AddFunction(MaxFunction::GetFunction());
	max.AddFunction(GetMinMaxNFunction<GreaterThan>());
	return max;
}

} // namespace duckdb
