#include "capi_v2_internal.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/common/enums/function_errors.hpp"

#include <cstdio>

// Translation between the generic (key, value) function property channel and the
// engine's FunctionProperties / AggregateFunctionProperties. Identifiers are laid
// out as 0xGGKKVV (group/key/value); see api_spec/v2/function/function.yaml. The
// C-API enum values are deliberately independent of the internal enum values:
// the switches below are the only place the two are tied together, so internal
// reordering cannot change the ABI.

namespace duckdb {
namespace {

constexpr uint32_t FUNCTION_PROPERTY_GROUP_MASK = 0xFF0000u;
constexpr uint32_t FUNCTION_PROPERTY_KEY_MASK = 0xFFFF00u;
constexpr uint32_t FUNCTION_PROPERTY_GROUP_COMMON = 0x010000u;
constexpr uint32_t FUNCTION_PROPERTY_GROUP_AGGREGATE = 0x030000u;

uint32_t AsU32(DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	return static_cast<uint32_t>(key);
}
uint32_t AsU32(DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	return static_cast<uint32_t>(value);
}

std::string ToHex(uint32_t value) {
	char buffer[16];
	std::snprintf(buffer, sizeof(buffer), "0x%06x", value);
	return std::string(buffer);
}

[[noreturn]] void ThrowUnknownValue(DUCKDB_V2_FUNCTION_PROPERTY_KEY key, DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	throw InvalidInputException("Unknown function property value " + ToHex(AsU32(value)) + " for key " +
	                            ToHex(AsU32(key)));
}
[[noreturn]] void ThrowUnknownKey(DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	throw InvalidInputException("Unknown function property key " + ToHex(AsU32(key)));
}

// A value belongs to a key iff it shares the key's high 16 bits (group + key).
void ValidateValueMatchesKey(DUCKDB_V2_FUNCTION_PROPERTY_KEY key, DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	if ((AsU32(value) & FUNCTION_PROPERTY_KEY_MASK) != AsU32(key)) {
		throw InvalidInputException("Function property value " + ToHex(AsU32(value)) + " does not belong to key " +
		                            ToHex(AsU32(key)));
	}
}

void SetCommonProperty(FunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                       DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (key) {
	case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT:
			props.SetStability(FunctionStability::CONSISTENT);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE:
			props.SetStability(FunctionStability::VOLATILE);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY:
			props.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT:
			props.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL:
			props.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE:
			props.SetErrorMode(FunctionErrors::CANNOT_ERROR);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE:
			props.SetErrorMode(FunctionErrors::CAN_THROW_RUNTIME_ERROR);
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE:
			props.SetCollationHandling(FunctionCollationHandling::PROPAGATE_COLLATIONS);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE:
			props.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE:
			props.SetCollationHandling(FunctionCollationHandling::IGNORE_COLLATIONS);
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	default:
		ThrowUnknownKey(key);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetCommonProperty(const FunctionProperties &props,
                                                    DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	switch (key) {
	case DUCKDB_V2_FUNCTION_PROPERTY_STABILITY:
		switch (props.GetStability()) {
		case FunctionStability::CONSISTENT:
			return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT;
		case FunctionStability::VOLATILE:
			return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE;
		case FunctionStability::CONSISTENT_WITHIN_QUERY:
			return DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY;
		}
		break;
	case DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING:
		switch (props.GetNullHandling()) {
		case FunctionNullHandling::DEFAULT_NULL_HANDLING:
			return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT;
		case FunctionNullHandling::SPECIAL_HANDLING:
			return DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL;
		}
		break;
	case DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY:
		switch (props.GetErrorMode()) {
		case FunctionErrors::CANNOT_ERROR:
			return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE;
		case FunctionErrors::CAN_THROW_RUNTIME_ERROR:
			return DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE;
		}
		break;
	case DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING:
		switch (props.GetCollationHandling()) {
		case FunctionCollationHandling::PROPAGATE_COLLATIONS:
			return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE;
		case FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS:
			return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE;
		case FunctionCollationHandling::IGNORE_COLLATIONS:
			return DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE;
		}
		break;
	default:
		break;
	}
	ThrowUnknownKey(key);
}

void SetAggregateProperty(AggregateFunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                          DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	switch (key) {
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES:
			props.order_dependent = AggregateOrderDependent::ORDER_DEPENDENT;
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO:
			props.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT:
		switch (value) {
		case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES:
			props.distinct_dependent = AggregateDistinctDependent::DISTINCT_DEPENDENT;
			return;
		case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO:
			props.distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
			return;
		default:
			ThrowUnknownValue(key, value);
		}
	default:
		ThrowUnknownKey(key);
	}
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetAggregateProperty(const AggregateFunctionProperties &props,
                                                       DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	switch (key) {
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT:
		switch (props.order_dependent) {
		case AggregateOrderDependent::ORDER_DEPENDENT:
			return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES;
		case AggregateOrderDependent::NOT_ORDER_DEPENDENT:
			return DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO;
		}
		break;
	case DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT:
		switch (props.distinct_dependent) {
		case AggregateDistinctDependent::DISTINCT_DEPENDENT:
			return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES;
		case AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT:
			return DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO;
		}
		break;
	default:
		break;
	}
	ThrowUnknownKey(key);
}

uint32_t GroupOf(DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	return AsU32(key) & FUNCTION_PROPERTY_GROUP_MASK;
}

} // namespace

void SetScalarFunctionProperty(FunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                               DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	ValidateValueMatchesKey(key, value);
	if (GroupOf(key) != FUNCTION_PROPERTY_GROUP_COMMON) {
		throw InvalidInputException("Function property key " + ToHex(AsU32(key)) +
		                            " is not valid for scalar functions");
	}
	SetCommonProperty(props, key, value);
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetScalarFunctionProperty(const FunctionProperties &props,
                                                            DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	if (GroupOf(key) != FUNCTION_PROPERTY_GROUP_COMMON) {
		throw InvalidInputException("Function property key " + ToHex(AsU32(key)) +
		                            " is not valid for scalar functions");
	}
	return GetCommonProperty(props, key);
}

void SetAggregateFunctionProperty(AggregateFunctionProperties &props, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                  DUCKDB_V2_FUNCTION_PROPERTY_VALUE value) {
	ValidateValueMatchesKey(key, value);
	auto group = GroupOf(key);
	if (group == FUNCTION_PROPERTY_GROUP_COMMON) {
		SetCommonProperty(props, key, value);
		return;
	}
	if (group == FUNCTION_PROPERTY_GROUP_AGGREGATE) {
		SetAggregateProperty(props, key, value);
		return;
	}
	throw InvalidInputException("Function property key " + ToHex(AsU32(key)) + " is not valid for aggregate functions");
}

DUCKDB_V2_FUNCTION_PROPERTY_VALUE GetAggregateFunctionProperty(const AggregateFunctionProperties &props,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_KEY key) {
	auto group = GroupOf(key);
	if (group == FUNCTION_PROPERTY_GROUP_COMMON) {
		return GetCommonProperty(props, key);
	}
	if (group == FUNCTION_PROPERTY_GROUP_AGGREGATE) {
		return GetAggregateProperty(props, key);
	}
	throw InvalidInputException("Function property key " + ToHex(AsU32(key)) + " is not valid for aggregate functions");
}

} // namespace duckdb
