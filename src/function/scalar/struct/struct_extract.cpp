#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

static void StructExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<StructExtractBindData>();

	// this should be guaranteed by the binder
	const auto &vec = args.data[0];

	vec.Verify();
	const auto &children = StructVector::GetEntries(vec);
	D_ASSERT(info.index < children.size());
	auto &struct_child = children[info.index];
	result.Reference(struct_child);
	result.Verify();
}

enum class StructExtractKey : uint8_t { NAME, INDEX };

//! Looks up the entry with the given name, returns its index and sets the type of the entry
static idx_t ResolveStructEntryByName(const LogicalType &struct_type, const string &key_str, LogicalType &entry_type) {
	auto &struct_children = StructType::GetChildTypes(struct_type);
	if (key_str.empty()) {
		throw BinderException("Key name for struct_extract must not be empty");
	}
	auto key = Identifier(key_str);
	for (idx_t i = 0; i < struct_children.size(); i++) {
		if (struct_children[i].first == key) {
			entry_type = struct_children[i].second;
			return i;
		}
	}
	vector<string> candidates;
	candidates.reserve(struct_children.size());
	for (auto &struct_child : struct_children) {
		candidates.emplace_back(struct_child.first);
	}
	auto closest_settings = StringUtil::TopNJaroWinkler(candidates, key);
	auto message = StringUtil::CandidatesMessage(closest_settings, "Candidate Entries");
	throw BinderException("Could not find key %s in struct\n%s", key, message);
}

//! Looks up the entry with the given (1-based) index, returns its (0-based) index and sets the type of the entry
static idx_t ResolveStructEntryByIndex(const LogicalType &struct_type, int64_t index, LogicalType &entry_type) {
	auto &struct_children = StructType::GetChildTypes(struct_type);
	if (index <= 0 || idx_t(index) > struct_children.size()) {
		throw BinderException("Key index %lld for struct_extract out of range - expected an index between 1 and %llu",
		                      index, struct_children.size());
	}
	entry_type = struct_children[NumericCast<idx_t>(index - 1)].second;
	return NumericCast<idx_t>(index - 1);
}

//! Validates the struct argument, and resolves the type of the entry that a constant key extracts
template <StructExtractKey KEY>
static LogicalType StructExtractReturnType(const ResolveReturnTypeInput &input) {
	auto &child_type = input.GetArgumentType(0);
	if (!StructType::IsStruct(child_type)) {
		throw BinderException("struct_extract can only be used on a STRUCT, not on %s", child_type.ToString());
	}
	if (StructType::GetChildTypes(child_type).empty()) {
		throw BinderException("Can't extract something from an empty struct");
	}
	// the type is resolved here, so that it is also known (and the key is validated) for NULL structs
	auto key_val = input.TryGetConstant(1);
	if (!key_val || key_val->IsNull()) {
		// the bind reports a key that is not a constant
		return LogicalType::ANY;
	}
	LogicalType entry_type;
	if (KEY == StructExtractKey::NAME) {
		ResolveStructEntryByName(child_type, key_val->GetValue<string>(), entry_type);
	} else {
		ResolveStructEntryByIndex(child_type, key_val->GetValue<int64_t>(), entry_type);
	}
	return entry_type;
}

static unique_ptr<FunctionData> StructExtractBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto key_val = input.GetNonNullConstant(1);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);

	LogicalType return_type;
	auto key_index = ResolveStructEntryByName(bound_function.GetArguments()[0], StringValue::Get(key_val), return_type);
	return StructExtractAtFun::GetBindData(key_index);
}

static unique_ptr<FunctionData> StructExtractBindIndex(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	Value key_val = input.GetConstant(1);

	LogicalType return_type;
	auto key_index =
	    ResolveStructEntryByIndex(bound_function.GetArguments()[0], key_val.GetValue<int64_t>(), return_type);
	return StructExtractAtFun::GetBindData(key_index);
}

static unique_ptr<BaseStatistics> PropagateStructExtractStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;

	auto &info = bind_data->Cast<StructExtractBindData>();
	auto struct_child_stats = StructStats::GetChildStats(child_stats[0]);
	return struct_child_stats[info.index].ToUnique();
}

unique_ptr<FunctionData> StructExtractAtFun::GetBindData(idx_t index) {
	return make_uniq<StructExtractBindData>(index);
}

//! A struct with names: its fields are the pack KV
static TypeName NamedStructPattern() {
	return TypeName(Identifier("STRUCT"), {TypeName::NAMED_PACK("KV")});
}

//! A tuple: its types are the pack T
static TypeName TuplePattern() {
	return TypeName::TUPLE({TypeName::PACK("T")});
}

ScalarFunction GetKeyExtractFunction() {
	ScalarFunction fun("struct_extract",
	                   FunctionSignature()
	                       .AddNamedPack("KV")
	                       .AddParameter("struct", NamedStructPattern())
	                       .AddParameter("entry", LogicalType::VARCHAR)
	                       .SetReturnType(ReturnType::ANY(StructExtractReturnType<StructExtractKey::NAME>)),
	                   StructExtractFunction, StructExtractBind, PropagateStructExtractStats);
	return fun;
}

ScalarFunction GetIndexExtractFunction() {
	ScalarFunction fun("struct_extract",
	                   FunctionSignature()
	                       .AddPack("T")
	                       .AddParameter("tuple", TuplePattern())
	                       .AddParameter("index", LogicalType::BIGINT)
	                       .SetReturnType(ReturnType::ANY(StructExtractReturnType<StructExtractKey::INDEX>)),
	                   StructExtractFunction, StructExtractBindIndex, PropagateStructExtractStats);
	return fun;
}

//! struct_extract_at takes a struct with names and a tuple: an overload for each, as neither is passed as the other
static ScalarFunction GetExtractAtFunction(FunctionSignature signature) {
	ScalarFunction fun("struct_extract_at",
	                   signature.AddParameter("index", LogicalType::BIGINT)
	                       .SetReturnType(ReturnType::ANY(StructExtractReturnType<StructExtractKey::INDEX>)),
	                   StructExtractFunction, StructExtractBindIndex, PropagateStructExtractStats);
	return fun;
}

static ScalarFunction GetNamedExtractAtFunction() {
	return GetExtractAtFunction(FunctionSignature().AddNamedPack("KV").AddParameter("struct", NamedStructPattern()));
}

static ScalarFunction GetTupleExtractAtFunction() {
	return GetExtractAtFunction(FunctionSignature().AddPack("T").AddParameter("struct", TuplePattern()));
}

ScalarFunction GetExtractAtFunction(const LogicalType &struct_type) {
	return struct_type.id() == LogicalTypeId::TUPLE ? GetTupleExtractAtFunction() : GetNamedExtractAtFunction();
}

ScalarFunctionSet StructExtractFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunctionSet struct_extract_set("struct_extract");
	struct_extract_set.AddFunction(GetKeyExtractFunction());
	struct_extract_set.AddFunction(GetIndexExtractFunction());
	return struct_extract_set;
}

ScalarFunctionSet StructExtractAtFun::GetFunctions() {
	ScalarFunctionSet struct_extract_at_set("struct_extract_at");
	struct_extract_at_set.AddFunction(GetNamedExtractAtFunction());
	struct_extract_at_set.AddFunction(GetTupleExtractAtFunction());
	return struct_extract_at_set;
}

} // namespace duckdb
