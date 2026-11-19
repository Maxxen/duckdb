#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

// the number of entries of a MAP or LIST (both are stored as a list of entries)
static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &map = args.data[0];
	auto entries = map.Values<list_entry_t>();

	auto result_data = FlatVector::Writer<uint64_t>(result, args.size());
	for (idx_t row = 0; row < args.size(); row++) {
		auto entry = entries[row];
		if (!entry.IsValid()) {
			result_data.WriteNull();
			continue;
		}
		result_data.WriteValue(entries.GetValueUnsafe(row).length);
	}
}

ScalarFunctionSet CardinalityFun::GetFunctions() {
	ScalarFunctionSet set("cardinality");
	// the entries of a map
	ScalarFunction map_fun(FunctionSignature()
	                           .AddVariable("K")
	                           .AddVariable("V")
	                           .AddParameter("map", TypeName::MAP(TypeName::VARIABLE("K"), TypeName::VARIABLE("V")))
	                           .SetReturnType(LogicalType::UBIGINT),
	                       CardinalityFunction);
	set.AddFunction(map_fun);
	// the elements of a list - a fixed-size array is cast to a list
	ScalarFunction list_fun(FunctionSignature()
	                            .AddVariable("T")
	                            .AddParameter("list", TypeName::LIST(TypeName::VARIABLE("T")))
	                            .SetReturnType(LogicalType::UBIGINT),
	                        CardinalityFunction);
	set.AddFunction(list_fun);
	return set;
}

} // namespace duckdb
