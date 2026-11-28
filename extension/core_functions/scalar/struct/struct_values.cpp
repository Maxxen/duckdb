#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar/nested_functions.hpp" // VariableReturnBindData
#include "core_functions/scalar/struct_functions.hpp"

namespace duckdb {

static void StructValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const auto &input = args.data[0];
	const idx_t count = args.size();

	auto &input_children = StructVector::GetEntries(input);
	auto &result_children = StructVector::GetEntries(result);
	D_ASSERT(result_children.size() == input_children.size());

	// UnnamedStruct vector and Struct vector are actually the same underneath, so we can just reference the children
	if (input.GetType().id() == LogicalTypeId::TUPLE) {
		result.Reference(input);
		return;
	}

	// We would use result.Reference(input) also for this case,
	// but that function asserts that the logical types are the same
	for (idx_t i = 0; i < input_children.size(); i++) {
		result_children[i].Reference(input_children[i]);
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			ConstantVector::SetNull(result, count_t(count));
		}
	} else {
		// set only the struct buffer's type - do not propagate to children
		// since children reference external vectors (input children) that may have incompatible buffer types
		result.BufferMutable().SetVectorTypeOnly(VectorType::FLAT_VECTOR);

		// Make result validity to mirror input's nulls
		auto validity_entries = input.Validity();

		if (validity_entries.CanHaveNull()) {
			auto &validity = FlatVector::ValidityMutable(result);

			for (idx_t i = 0; i < count; i++) {
				if (!validity_entries.IsValid(i)) {
					validity.SetInvalid(i);
				}
			}
		}
	}
}

ScalarFunctionSet StructValuesFun::GetFunctions() {
	// a struct with names and a tuple: an overload for each, as neither is passed as the other. The values of a
	// struct are the tuple of the types of its fields
	ScalarFunctionSet set;
	set.AddFunction(
	    ScalarFunction(FunctionSignature()
	                       .AddNamedPack("KW")
	                       .AddParameter("struct", TypeName(Identifier("STRUCT"), {TypeName::NAMED_PACK("KW")}))
	                       .SetReturnType(TypeName::TUPLE({TypeName::PACK("KW")})),
	                   StructValuesFunction));
	set.AddFunction(ScalarFunction(FunctionSignature()
	                                   .AddPack("T")
	                                   .AddParameter("struct", TypeName::TUPLE({TypeName::PACK("T")}))
	                                   .SetReturnType(TypeName::TUPLE({TypeName::PACK("T")})),
	                               StructValuesFunction));
	return set;
}

} // namespace duckdb
