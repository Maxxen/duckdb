#include "duckdb/common/checked_integer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static void ListResizeFunction(DataChunk &args, ExpressionState &, Vector &result) {
	// Early-out, if the return value is a constant NULL.
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result, count_t(args.size()));
		return;
	}

	const auto &lists = args.data[0];
	const auto &new_sizes = args.data[1];
	auto row_count = args.size();
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(new_sizes.GetType().id() == LogicalTypeId::UBIGINT);

	auto list_entries = lists.Values<list_entry_t>();
	auto new_size_entries = new_sizes.Values<ubigint_t>();
	auto &child_vector = ListVector::GetChild(lists);

	// Sum up the total child capacity
	ubigint_t total_child_size(0);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		auto list_entry = list_entries[row_idx];
		auto new_size_entry = new_size_entries[row_idx];
		if (list_entry.IsValid() && new_size_entry.IsValid()) {
			total_child_size += new_size_entry.GetValue();
		}
	}

	bool has_default_vector = args.ColumnCount() == 3 && args.data[2].GetType().id() != LogicalTypeId::SQLNULL;

	ListVector::Reserve(result, total_child_size.GetValue());
	auto result_entries = FlatVector::Writer<list_entry_t>(result, row_count);

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		auto list_entry = list_entries[row_idx];
		// Set to NULL, if the list is NULL.
		if (!list_entry.IsValid()) {
			result_entries.WriteNull();
			continue;
		}

		auto new_size_entry = new_size_entries[row_idx];
		ubigint_t new_size = new_size_entry.IsValid() ? new_size_entry.GetValue() : ubigint_t(0);

		// If new_size >= length, then we copy [0, length) values.
		// If new_size < length, then we copy [0, new_size) values.
		const auto &source_list = list_entry.GetValue();
		auto copy_count = MinValue<ubigint_t>(source_list.length, new_size);

		auto list = result_entries.WriteDynamicList();
		ubigint_t source_offset = source_list.offset;
		ubigint_t source_count = source_offset + copy_count;
		list.Append(child_vector, *FlatVector::IncrementalSelectionVector(), source_count.GetValue(),
		            source_offset.GetValue(), copy_count.GetValue());

		if (copy_count >= new_size) {
			continue;
		}
		ubigint_t remaining_count = new_size - copy_count;

		// if a default value is provided fill the list with the default value
		if (has_default_vector) {
			SelectionVector sel(remaining_count.GetValue());
			for (idx_t j = 0; j < remaining_count.GetValue(); j++) {
				sel.set_index(j, row_idx);
			}
			const auto &default_vector = args.data[2];
			// the selection only has an entry for every padded row
			list.Append(default_vector, sel, remaining_count.GetValue(), 0, remaining_count.GetValue());
			continue;
		}

		// Fill the remaining space with NULL.
		list.AppendNulls(remaining_count.GetValue());
	}
}

//! The list is resized to a UBIGINT size, and padded with a value of its element type
static LogicalType ListResizeResolveTypes(ResolveReturnTypeInput &input) {
	D_ASSERT(input.GetArgumentCount() == 2 || input.GetArgumentCount() == 3);
	input.CastArgument(1, LogicalType::UBIGINT);

	// a constant NULL list is resized to NULL, whatever it is padded with
	auto list_type = input.GetArgumentType(0);
	auto constant_list = input.TryGetConstant(0);
	if (ListType::GetChildType(list_type).id() == LogicalTypeId::SQLNULL && constant_list && constant_list->IsNull()) {
		return LogicalType::SQLNULL;
	}
	if (input.GetArgumentCount() == 3 && input.GetArgumentType(2).id() != LogicalTypeId::SQLNULL) {
		input.CastArgument(2, ListType::GetChildType(list_type));
	}
	return list_type;
}

ScalarFunctionSet ListResizeFun::GetFunctions() {
	ScalarFunction simple_fun(FunctionSignature()
	                              .AddVariable("T")
	                              .AddVariable("SIZE")
	                              .AddParameter("list", TypeName::LIST(TypeName::VARIABLE("T")))
	                              .AddParameter("size", TypeName::VARIABLE("SIZE"))
	                              .SetReturnType(ReturnType::ANY(ListResizeResolveTypes)),
	                          ListResizeFunction, VariableReturnBindData::Bind);
	simple_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	simple_fun.SetFallible();
	ScalarFunction default_value_fun(FunctionSignature()
	                                     .AddVariable("T")
	                                     .AddVariable("SIZE")
	                                     .AddVariable("VALUE")
	                                     .AddParameter("list", TypeName::LIST(TypeName::VARIABLE("T")))
	                                     .AddParameter("size", TypeName::VARIABLE("SIZE"))
	                                     .AddParameter("value", TypeName::VARIABLE("VALUE"))
	                                     .SetReturnType(ReturnType::ANY(ListResizeResolveTypes)),
	                                 ListResizeFunction, VariableReturnBindData::Bind);
	default_value_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	default_value_fun.SetFallible();
	ScalarFunctionSet list_resize_set("list_resize");
	list_resize_set.AddFunction(simple_fun);
	list_resize_set.AddFunction(default_value_fun);
	return list_resize_set;
}

} // namespace duckdb
