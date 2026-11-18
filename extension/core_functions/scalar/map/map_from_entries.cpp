#include "duckdb/common/vector/map_vector.hpp"
#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void MapFromEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	MapUtil::ReinterpretMap(result, args.data[0]);
	MapVector::MapConversionVerify(result, count);
}

ScalarFunction MapFromEntriesFun::GetFunction() {
	auto key_type = TypeName::VARIABLE("K");
	auto val_type = TypeName::VARIABLE("V");
	auto map_type = TypeName::MAP(key_type, val_type);
	auto row_type = TypeName::TUPLE({key_type, val_type});

	ScalarFunction fun({}, map_type, MapFromEntriesFunction);
	fun.GetSignature().AddVariable("K").AddVariable("V").AddParameter("map", TypeName::LIST(row_type));
	fun.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);

	fun.SetFallible();
	return fun;
}

} // namespace duckdb
