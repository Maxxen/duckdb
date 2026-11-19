#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

struct ConcatFunctionData : public FunctionData {
	ConcatFunctionData(const LogicalType &return_type_p, bool is_operator_p)
	    : return_type(return_type_p), is_operator(is_operator_p) {
	}

	LogicalType return_type;

	bool is_operator = false;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

bool ConcatFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ConcatFunctionData>();
	return return_type == other.return_type && is_operator == other.is_operator;
}

unique_ptr<FunctionData> ConcatFunctionData::Copy() const {
	return make_uniq<ConcatFunctionData>(return_type, is_operator);
}

void StringConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// iterate over the vectors to count how large the final string will be
	idx_t constant_lengths = 0;
	vector<idx_t> result_lengths(args.size(), 0);
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		const auto &input = args.data[col_idx];
		D_ASSERT(input.GetType().InternalType() == PhysicalType::VARCHAR);
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			auto input_data = ConstantVector::GetData<string_t>(input);
			constant_lengths += input_data->GetSize();
		} else {
			// now get the lengths of each of the input elements
			for (auto entry : input.Values<string_t>()) {
				if (!entry.IsValid()) {
					continue;
				}
				result_lengths[entry.GetIndex()] += entry.GetValue().GetSize();
			}
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = FlatVector::ScatterWriter<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		// allocate an empty string of the required size
		idx_t str_length = constant_lengths + result_lengths[i];
		result_data[i].EmptyString(str_length);
		// we reuse the result_lengths vector to store the currently appended size
		result_lengths[i] = 0;
	}

	// now that the empty space for the strings has been allocated, perform the concatenation
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		const auto &input = args.data[col_idx];

		// loop over the vector and concat to all results
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (ConstantVector::IsNull(input)) {
				// constant null, skip
				continue;
			}
			// append the constant vector to each of the strings
			auto input_data = ConstantVector::GetData<string_t>(input);
			auto input_ptr = input_data->GetData();
			auto input_len = input_data->GetSize();
			for (idx_t i = 0; i < args.size(); i++) {
				memcpy(result_data[i].GetDataWriteable() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		} else {
			for (auto entry : input.Values<string_t>()) {
				if (!entry.IsValid()) {
					continue;
				}
				auto &input_str = entry.GetValue();
				auto i = entry.GetIndex();
				auto input_ptr = input_str.GetData();
				auto input_len = input_str.GetSize();
				memcpy(result_data[i].GetDataWriteable() + result_lengths[i], input_ptr, input_len);
				result_lengths[i] += input_len;
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].Finalize();
	}
}

void ConcatOperator(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, [&](string_t a, string_t b) {
		    auto a_data = a.GetData();
		    auto b_data = b.GetData();
		    auto a_length = a.GetSize();
		    auto b_length = b.GetSize();

		    auto target_length = a_length + b_length;
		    auto target = StringVector::EmptyString(result, target_length);
		    auto target_data = target.GetDataWriteable();

		    memcpy(target_data, a_data, a_length);
		    memcpy(target_data + a_length, b_data, b_length);
		    target.Finalize();
		    return target;
	    });
}

struct ListConcatInputData {
	ListConcatInputData(const Vector &input, idx_t size)
	    : input(input), child_vec(ListVector::GetChild(input)), list_data(input.Values<list_entry_t>()) {
	}

	const Vector &input;
	const Vector &child_vec;
	VectorIterator<list_entry_t> list_data;
};

void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result, bool is_operator) {
	auto count = args.size();

	vector<ListConcatInputData> input_data;
	for (const auto &input : args.data) {
		if (!is_operator && input.GetType().id() == LogicalTypeId::SQLNULL) {
			// LIST_CONCAT ignores NULL values
			continue;
		}
		input_data.emplace_back(input, count);
	}

	// the || operator yields NULL whenever any input is NULL, while list_concat skips NULLs
	vector<bool> row_invalid(count, false);
	if (is_operator) {
		for (auto &input : input_data) {
			for (idx_t r = 0; r < count; r++) {
				if (!input.list_data[r].IsValid()) {
					row_invalid[r] = true;
				}
			}
		}
	}

	auto result_writer = FlatVector::Writer<list_entry_t>(result, count);
	for (idx_t r = 0; r < count; r++) {
		if (row_invalid[r]) {
			result_writer.WriteNull();
			continue;
		}
		auto list = result_writer.WriteDynamicList();
		for (auto &input : input_data) {
			auto list_val = input.list_data[r];
			if (!list_val.IsValid()) {
				continue;
			}
			const auto &list_entry = list_val.GetValue();
			list.Append(input.child_vec, *FlatVector::IncrementalSelectionVector(),
			            list_entry.offset + list_entry.length, list_entry.offset, list_entry.length);
		}
	}
}

void ConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<ConcatFunctionData>();
	if (info.return_type.id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result, count_t(args.size()));
		return;
	}
	if (info.return_type.id() == LogicalTypeId::LIST) {
		return ListConcatFunction(args, state, result, info.is_operator);
	}
	if (info.is_operator) {
		return ConcatOperator(args, state, result);
	}
	return StringConcatFunction(args, state, result);
}

//! The list type that lists and arrays are concatenated as - NULL if all of them are NULL
LogicalType GetListConcatType(ResolveReturnTypeInput &input) {
	auto &context = input.GetClientContext();
	LogicalType child_type = LogicalType::SQLNULL;
	bool all_null = true;
	for (idx_t i = 0; i < input.GetArgumentCount(); i++) {
		auto &return_type = input.GetArgumentType(i);
		if (return_type == LogicalTypeId::SQLNULL) {
			// we mimic postgres behaviour: list_concat(NULL, my_list) = my_list
			continue;
		}
		all_null = false;
		LogicalType next_type = LogicalTypeId::INVALID;
		switch (return_type.id()) {
		case LogicalTypeId::UNKNOWN:
			throw ParameterNotResolvedException();
		case LogicalTypeId::LIST:
			next_type = ListType::GetChildType(return_type);
			break;
		case LogicalTypeId::ARRAY:
			next_type = ArrayType::GetChildType(return_type);
			break;
		default: {
			string type_list;
			for (idx_t arg_idx = 0; arg_idx < input.GetArgumentCount(); arg_idx++) {
				if (!type_list.empty()) {
					if (arg_idx + 1 == input.GetArgumentCount()) {
						// last argument
						type_list += " and ";
					} else {
						type_list += ", ";
					}
				}
				type_list += input.GetArgumentType(arg_idx).ToString();
			}
			throw BinderException(input.GetArgumentLocation(i),
			                      "Cannot concatenate types %s - an explicit cast is required", type_list);
		}
		}
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, next_type, child_type)) {
			throw BinderException(input.GetArgumentLocation(i),
			                      "Cannot concatenate lists of types %s[] and %s[] - an explicit cast is required",
			                      child_type.ToString(), next_type.ToString());
		}
	}
	if (all_null) {
		return LogicalType::SQLNULL;
	}
	return LogicalType::LIST(child_type);
}

enum class ConcatKind : uint8_t { CONCAT, LIST_CONCAT, CONCAT_OPERATOR };

//! The type the arguments are concatenated as, which is the type that is returned
template <ConcatKind KIND>
LogicalType GetConcatType(ResolveReturnTypeInput &input) {
	bool list_concat = KIND == ConcatKind::LIST_CONCAT;
	bool all_null = true;
	// blob concat is only supported for the concat operator - regular concat converts to varchar
	bool all_blob = KIND == ConcatKind::CONCAT_OPERATOR;
	for (idx_t i = 0; i < input.GetArgumentCount(); i++) {
		auto &arg_type = input.GetArgumentType(i);
		if (arg_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (arg_type.id() == LogicalTypeId::LIST || arg_type.id() == LogicalTypeId::ARRAY) {
			list_concat = true;
		}
		if (arg_type.id() != LogicalTypeId::BLOB) {
			all_blob = false;
		}
		if (arg_type.id() != LogicalTypeId::SQLNULL) {
			all_null = false;
		}
	}
	if (all_null) {
		return KIND == ConcatKind::CONCAT ? LogicalType::VARCHAR : LogicalType::SQLNULL;
	}
	if (list_concat) {
		return GetListConcatType(input);
	}
	// we can now assume that the input is a string or castable to a string
	return all_blob ? LogicalType::BLOB : LogicalType::VARCHAR;
}

//! Casts the arguments to the type they are concatenated as
template <ConcatKind KIND>
LogicalType ConcatResolveTypes(ResolveReturnTypeInput &input) {
	auto concat_type = GetConcatType<KIND>(input);
	if (concat_type.id() != LogicalTypeId::SQLNULL) {
		for (idx_t i = 0; i < input.GetArgumentCount(); i++) {
			input.CastArgument(i, concat_type);
		}
	}
	return concat_type;
}

unique_ptr<FunctionData> BindConcatFunction(BindScalarFunctionInput &input) {
	return make_uniq<ConcatFunctionData>(input.GetBoundFunction().GetReturnType(), false);
}

unique_ptr<FunctionData> BindConcatOperator(BindScalarFunctionInput &input) {
	return make_uniq<ConcatFunctionData>(input.GetBoundFunction().GetReturnType(), true);
}

unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto stats = child_stats[0].ToUnique();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		stats->Merge(child_stats[i]);
	}
	// NULL lists are skipped, so the result length is the sum of the input lengths
	idx_t min_length = 0;
	optional_idx max_length = 0;
	for (auto &child : child_stats) {
		auto child_min = ListStats::MinListLength(child);
		auto child_max = ListStats::MaxListLength(child);
		if (!child.CanHaveNull() && child_min.IsValid()) {
			min_length += child_min.GetIndex();
		}
		if (!max_length.IsValid() || !child_max.IsValid() ||
		    child_max.GetIndex() > NumericLimits<idx_t>::Maximum() - max_length.GetIndex()) {
			max_length = optional_idx();
		} else {
			max_length = max_length.GetIndex() + child_max.GetIndex();
		}
	}
	ListStats::SetLengthBounds(*stats, min_length, max_length);
	return stats;
}

} // namespace

ScalarFunction ListConcatFun::GetFunction() {
	// The arguments and return types are set in the binder function.
	auto fun = ScalarFunction(FunctionSignature()
	                              .AddArgs("args", LogicalType::LIST(LogicalType::ANY))
	                              .SetReturnType(ReturnType::ANY(ConcatResolveTypes<ConcatKind::LIST_CONCAT>)),
	                          ConcatFunction, BindConcatFunction, ListConcatStats);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

// the concat operator and concat function have different behavior regarding NULLs
// this is strange but seems consistent with postgresql and mysql
// (sqlite does not support the concat function, only the concat operator)

// the concat operator behaves as one would expect: any NULL value present results in a NULL
// i.e. NULL || 'hello' = NULL
// the concat function, however, treats NULL values as an empty string
// i.e. concat(NULL, 'hello') = 'hello'
ScalarFunction ConcatFun::GetFunction() {
	ScalarFunction concat("concat",
	                      FunctionSignature()
	                          .AddVariable("T")
	                          .AddParameter("value", TypeName::VARIABLE("T"))
	                          .AddArgs("args", LogicalType::ANY)
	                          .SetReturnType(ReturnType::ANY(ConcatResolveTypes<ConcatKind::CONCAT>)),
	                      ConcatFunction, BindConcatFunction);
	concat.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return concat;
}

ScalarFunction ConcatOperatorFun::GetFunction() {
	ScalarFunction concat_op("||",
	                         FunctionSignature()
	                             .AddVariable("T")
	                             .AddVariable("U")
	                             .AddParameter("arg1", TypeName::VARIABLE("T"))
	                             .AddParameter("arg2", TypeName::VARIABLE("U"))
	                             .SetReturnType(ReturnType::ANY(ConcatResolveTypes<ConcatKind::CONCAT_OPERATOR>)),
	                         ConcatFunction, BindConcatOperator);
	return concat_op;
}

} // namespace duckdb
