#include "duckdb/core_functions/lambda_functions.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

static unique_ptr<FunctionData> ListHasAllBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));
	arguments[1] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[1]));

	if (bound_function.name == "<@") {
		std::swap(arguments[0], arguments[1]);
	}

	return nullptr;
}

static void ListHasAllFunction(DataChunk &args, ExpressionState &, Vector &result) {

	auto &l_vec = args.data[0];
	auto &r_vec = args.data[1];

	if (ListType::GetChildType(l_vec.GetType()) == LogicalType::SQLNULL &&
	    ListType::GetChildType(r_vec.GetType()) == LogicalType::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<bool>(result)[0] = true;
		return;
	}

	const auto l_size = ListVector::GetListSize(l_vec);
	const auto r_size = ListVector::GetListSize(r_vec);

	auto &l_child = ListVector::GetEntry(l_vec);
	auto &r_child = ListVector::GetEntry(r_vec);

	// Setup unified formats for the list elements
	UnifiedVectorFormat build_format;
	UnifiedVectorFormat probe_format;

	l_child.ToUnifiedFormat(l_size, build_format);
	r_child.ToUnifiedFormat(r_size, probe_format);

	// Create the sort keys for the list elements
	Vector l_sortkey_vec(LogicalType::BLOB, l_size);
	Vector r_sortkey_vec(LogicalType::BLOB, r_size);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(l_child, l_size, order_modifiers, l_sortkey_vec);
	CreateSortKeyHelpers::CreateSortKey(r_child, r_size, order_modifiers, r_sortkey_vec);

	const auto build_data = FlatVector::GetData<string_t>(l_sortkey_vec);
	const auto probe_data = FlatVector::GetData<string_t>(r_sortkey_vec);

	string_set_t set;

	BinaryExecutor::Execute<list_entry_t, list_entry_t, bool>(
	    l_vec, r_vec, result, args.size(), [&](const list_entry_t &build_list, const list_entry_t &probe_list) {
		    // Short circuit if the probe list is empty
		    if (probe_list.length == 0) {
			    return true;
		    }

		    // Reset the set
		    set.clear();

		    // Build the set
		    for (auto idx = build_list.offset; idx < build_list.offset + build_list.length; idx++) {
			    const auto entry_idx = build_format.sel->get_index(idx);
			    if (build_format.validity.RowIsValid(entry_idx)) {
				    set.insert(build_data[entry_idx]);
			    }
		    }

		    // Probe the set
		    for (auto idx = probe_list.offset; idx < probe_list.offset + probe_list.length; idx++) {
			    const auto entry_idx = probe_format.sel->get_index(idx);
			    if (probe_format.validity.RowIsValid(entry_idx) && set.find(probe_data[entry_idx]) == set.end()) {
				    return false;
			    }
		    }
		    return true;
	    });
}

ScalarFunction ListHasAllFun::GetFunction() {
	auto template_type = LogicalType::TEMPLATE("T");
	auto list_type = LogicalType::LIST(template_type);
	ScalarFunction fun({list_type, list_type}, LogicalType::BOOLEAN, ListHasAllFunction, ListHasAllBind);
	return fun;
}

} // namespace duckdb
