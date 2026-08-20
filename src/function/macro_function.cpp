#include "duckdb/function/macro_function.hpp"

#include "duckdb/common/sql_identifier.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

MacroFunction::MacroFunction(MacroType type) : type(type) {
}

string FormatMacroFunction(const MacroFunction &function, const Identifier &name) {
	auto result = name + "(";
	string parameters;
	for (idx_t param_idx = 0; param_idx < function.parameters.size(); param_idx++) {
		if (!parameters.empty()) {
			parameters += ", ";
		}
		const auto &param_name = function.parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
		parameters += param_name;

		if (param_idx < function.types.size() && function.types[param_idx]) {
			parameters += " " + function.types[param_idx]->ToString();
		}

		auto it = function.default_parameters.find(param_name);
		if (it != function.default_parameters.end()) {
			parameters += " := ";
			parameters += it->second->ToString();
		}
	}
	result += parameters + ")";
	return result;
}

MacroBindResult MacroFunction::BindMacroFunction(
    Binder &binder, const vector<unique_ptr<MacroFunction>> &functions, const Identifier &name,
    FunctionExpression &function_expr, vector<unique_ptr<ParsedExpression>> &positional_arguments,
    InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> &named_arguments,
    idx_t depth) {
	ExpressionBinder expr_binder(binder, binder.context);
	expr_binder.lambda_bindings = binder.lambda_bindings;

	// Figure out whether we even need to bind arguments
	bool requires_bind = false;
	for (auto &function : functions) {
		if (function->HasTypedParameters()) {
			requires_bind = true;
			break;
		}
	}

	// Find argument types and separate positional and default arguments
	vector<LogicalType> positional_arg_types;
	InsertionOrderPreservingMap<LogicalType, Identifier, identifier_map_t<idx_t>> named_arg_types;
	for (auto &arg : function_expr.GetArgumentsMutable()) {
		auto arg_copy = arg.GetExpression().Copy();
		LogicalType arg_type = LogicalType::UNKNOWN;
		if (requires_bind) {
			const auto arg_bind_result = expr_binder.BindExpression(arg_copy, depth + 1);
			arg_type = arg_bind_result.HasError() ? LogicalType::UNKNOWN : arg_bind_result.expression->GetReturnType();
		}
		if (!arg.GetExpression().GetAlias().empty()) {
			// Default argument
			if (named_arguments.find(arg.GetExpression().GetAlias()) != named_arguments.end()) {
				return MacroBindResult(StringUtil::Format("Macro %s() has named argument repeated '%s'", name,
				                                          arg.GetExpression().GetAlias()));
			}
			named_arg_types.insert(arg.GetExpression().GetAlias(), std::move(arg_type));
			named_arguments[arg.GetExpression().GetAlias()] = std::move(arg.GetExpressionMutable());
		} else if (!named_arguments.empty()) {
			return MacroBindResult(
			    StringUtil::Format("Macro %s() has positional argument following named argument", name));
		} else {
			// Positional argument
			positional_arguments.push_back(std::move(arg.GetExpressionMutable()));
			positional_arg_types.push_back(std::move(arg_type));
		}
	}

	// Check for each macro function if it matches the number of positional arguments
	auto lowest_cost = NumericLimits<idx_t>::Maximum();
	vector<idx_t> result_indices;
	for (idx_t function_idx = 0; function_idx < functions.size(); function_idx++) {
		auto &function = functions[function_idx];

		auto parameter_types = function->GetParameterTypes(binder.context);

		// Check if we can exclude the match based on argument count (also avoids out-of-bounds below)
		if (positional_arguments.size() > function->parameters.size() ||
		    positional_arguments.size() + named_arguments.size() > function->parameters.size()) {
			continue;
		}

		// Also check if we can exclude the match based on the supplied named arguments
		bool bail = false;
		for (auto &kv : named_arguments) {
			bool found = false;
			for (const auto &parameter : function->parameters) {
				if (kv.first == parameter->Cast<ColumnRefExpression>().GetColumnName()) {
					found = true;
					break;
				}
			}
			if (!found) {
				bail = true;
				break;
			}
		}
		if (bail) {
			continue; // One of the supplied named arguments is not present in the macro definition
		}

		// Loop through arguments, positionals first, then named, summing up cost for best matching macro as we go
		idx_t param_idx = 0;
		idx_t macro_cost = 0;

		// Figure out best function fit, bail if any positional arguments are duplicated in named arguments
		for (; param_idx < positional_arguments.size(); param_idx++) {
			const auto &param_name = function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
			if (named_arguments.find(param_name) != named_arguments.end()) {
				bail = true;
				break;
			}
			const auto &param_type = parameter_types[param_idx];
			if (param_type == LogicalType::UNKNOWN) {
				macro_cost += 1000000;
			} else {
				const auto cast_cost =
				    CastFunctionSet::ImplicitCastCost(binder.context, positional_arg_types[param_idx], param_type);
				if (cast_cost < 0) {
					bail = true;
					break;
				}
				macro_cost += NumericCast<idx_t>(cast_cost);
			}
		}
		if (bail) {
			continue; // Couldn't find one of the supplied named arguments, or one of the casts is not possible
		}

		// Match remaining arguments with named/defaults
		for (; param_idx < function->parameters.size(); param_idx++) {
			const auto &param_name = function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
			const auto arg_it = named_arguments.find(param_name);
			const auto default_it = function->default_parameters.find(param_name);
			if (arg_it == named_arguments.end() && default_it == function->default_parameters.end()) {
				break; // The parameter has no argument (supplied/default)!
			}
			if (arg_it == named_arguments.end()) {
				continue; // Using default, no cost
			}

			// Supplied arg, add cost
			const auto &param_type = parameter_types[param_idx];
			if (param_type == LogicalType::UNKNOWN) {
				macro_cost += 1000000;
			} else {
				const auto cast_cost =
				    CastFunctionSet::ImplicitCastCost(binder.context, named_arg_types[param_name], param_type);
				if (cast_cost < 0) {
					break; // No cast possible
				}
				macro_cost += NumericCast<idx_t>(cast_cost);
			}
		}

		if (param_idx == function->parameters.size() && macro_cost <= lowest_cost) {
			// Found a matching function
			if (macro_cost < lowest_cost) {
				lowest_cost = macro_cost;
				result_indices.clear();
			}
			result_indices.push_back(function_idx);
		}
	}

	if (result_indices.size() != 1) {
		string error;
		if (result_indices.empty()) {
			// No matching function found
			error = StringUtil::Format("Macro %s() does not support the supplied arguments.", name);
			error += " You might need to add explicit type casts.\n";
			error += "Candidate macros:";
			for (auto &function : functions) {
				error += "\n\t" + FormatMacroFunction(*function, name);
			}
		} else {
			// Multiple matching functions found
			error = StringUtil::Format("Macro %s() has multiple overloads that match the supplied arguments.\n", name);
			error += "In order to select one, please supply all arguments by name, and/or add explicit type casts.\n";
			error += "Candidate macros:";
			for (const auto &result_idx : result_indices) {
				error += "\n\t" + FormatMacroFunction(*functions[result_idx], name);
			}
		}

		return MacroBindResult(error);
	}

	const auto &macro_idx = result_indices[0];
	const auto &macro_def = *functions[macro_idx];

	// Cast positionals to proper types
	// the cast target is a parsed expression, so the description is turned back into one here
	auto cast_target = [&macro_def](idx_t idx) -> unique_ptr<TypeExpression> {
		if (idx >= macro_def.types.size() || !macro_def.types[idx]) {
			return nullptr;
		}
		return macro_def.types[idx]->ToTypeExpression();
	};
	idx_t param_idx = 0;
	for (; param_idx < positional_arguments.size(); param_idx++) {
		if (auto target = cast_target(param_idx)) {
			// This macro parameter is typed, add a cast
			auto &positional_arg = positional_arguments[param_idx];
			positional_arg = make_uniq<CastExpression>(std::move(target), std::move(positional_arg));
		}
	}

	// Add the default values for parameters that have defaults, for which no argument was supplied
	for (; param_idx < macro_def.parameters.size(); param_idx++) {
		const auto &param_name = macro_def.parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
		auto named_arg_it = named_arguments.find(param_name);
		if (named_arg_it != named_arguments.end()) {
			// The user has supplied an argument for this parameter
			if (auto target = cast_target(param_idx)) {
				// This macro parameter is typed, add a cast
				auto &named_arg = named_arg_it->second;
				named_arg = make_uniq<CastExpression>(std::move(target), std::move(named_arg));
			}
			continue;
		}

		const auto default_it = macro_def.default_parameters.find(param_name);
		D_ASSERT(default_it != macro_def.default_parameters.end());
		auto &named_arg = named_arguments[param_name];
		named_arg = default_it->second->Copy();
		if (auto target = cast_target(param_idx)) {
			// This macro parameter is typed, add a cast
			named_arg = make_uniq<CastExpression>(std::move(target), std::move(named_arg));
		}
	}

	return MacroBindResult(macro_idx);
}

unique_ptr<DummyBinding> MacroFunction::CreateDummyBinding(
    ClientContext &context, const MacroFunction &macro_def, const Identifier &name,
    vector<unique_ptr<ParsedExpression>> &positional_arguments,
    InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> &named_arguments) {
	// create a MacroBinding to bind this macro's parameters to its arguments
	auto types = macro_def.GetParameterTypes(context);
	vector<Identifier> names;
	for (idx_t i = 0; i < positional_arguments.size(); i++) {
		names.emplace_back(macro_def.parameters[i]->Cast<ColumnRefExpression>().GetColumnName());
	}
	for (auto &kv : named_arguments) {
		names.emplace_back(kv.first);
		positional_arguments.push_back(std::move(kv.second)); // push defaults into positionals
	}

	auto res = make_uniq<DummyBinding>(types, names, name.GetIdentifierName());
	res->arguments = &positional_arguments;
	return res;
}

bool MacroFunction::HasTypedParameters() const {
	for (auto &type : types) {
		if (type) {
			return true;
		}
	}
	for (auto &type : parsed_types) {
		if (type) {
			return true;
		}
	}
	return false;
}

vector<LogicalType> MacroFunction::GetParameterTypes(ClientContext &context) const {
	vector<LogicalType> result;
	result.reserve(parameters.size());
	for (idx_t i = 0; i < parameters.size(); i++) {
		if (i >= types.size() || !types[i]) {
			// an untyped parameter
			result.emplace_back(LogicalType::UNKNOWN);
			continue;
		}
		auto binder = Binder::CreateBinder(context);
		result.push_back(binder->BindTypeDescriptor(*types[i]));
	}
	return result;
}

void MacroFunction::CopyProperties(MacroFunction &other) const {
	other.type = type;
	for (auto &param : parameters) {
		other.parameters.push_back(param->Copy());
	}
	for (auto &kv : default_parameters) {
		other.default_parameters[kv.first] = kv.second->Copy();
	}
	for (auto &type : types) {
		other.types.push_back(type ? make_uniq<TypeDescriptor>(*type) : nullptr);
	}
}

vector<unique_ptr<ParsedExpression>>
MacroFunction::GetPositionalParametersForSerialization(Serializer &serializer) const {
	vector<unique_ptr<ParsedExpression>> result;
	if (serializer.ShouldSerialize(StorageVersion::V1_4_0)) {
		// We serialize all positional parameters as-is
		for (auto &param : parameters) {
			result.push_back(param->Copy());
		}
		return result;
	}
	// Serializing targeting an older version - delete all named parameters from the list of positional parameters
	for (auto &param : parameters) {
		auto &colref = param->Cast<ColumnRefExpression>();
		if (default_parameters.find(Identifier(colref.GetName())) != default_parameters.end()) {
			// This is a default parameter - do not serialize
			continue;
		}
		result.push_back(param->Copy());
	}
	return result;
}

void MacroFunction::FinalizeDeserialization() {
	// In older versions of DuckDB, parameters with defaults were not stored in "parameters", this adds them
	for (auto &kv : default_parameters) {
		bool found = false;
		for (const auto &parameter : parameters) {
			if (kv.first == parameter->Cast<ColumnRefExpression>().GetColumnName()) {
				found = true;
				break;
			}
		}
		if (!found) {
			parameters.push_back(make_uniq<ColumnRefExpression>(kv.first));
		}
	}
	// In older versions of DuckDB macros were always untyped
	if (parameters.size() != types.size()) {
		types.resize(parameters.size());
	}
}

string MacroFunction::ToSQL() const {
	vector<string> param_strings;
	for (idx_t param_idx = 0; param_idx < parameters.size(); param_idx++) {
		const auto &param_name = parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
		auto param_string = SQLIdentifier::ToString(param_name.GetIdentifierName());
		if (types[param_idx]) {
			param_string += " " + types[param_idx]->ToString();
		}
		auto it = default_parameters.find(param_name);
		if (it != default_parameters.end()) {
			param_string += " := " + it->second->ToString();
		}
		param_strings.emplace_back(std::move(param_string));
	}
	return StringUtil::Format("(%s) AS ", StringUtil::Join(param_strings, ", "));
}

void MacroFunction::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<MacroType>(100, "type", type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(
	    101, "parameters", GetPositionalParametersForSerialization(serializer));
	serializer.WritePropertyWithDefault<
	    InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>>>(
	    102, "default_parameters", default_parameters);
	if (serializer.ShouldSerialize(StorageVersion::V1_4_0) && !serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		// older versions store the declared types as resolved LogicalTypes. The stored expressions are normalized,
		// so they name only built-in types and resolve without a ClientContext.
		vector<LogicalType> logical_types;
		logical_types.reserve(types.size());
		for (auto &type : types) {
			logical_types.push_back(type ? type->DefaultBind() : LogicalType::UNKNOWN);
		}
		serializer.WritePropertyWithDefault<vector<LogicalType>>(103, "types", logical_types, vector<LogicalType>());
	}
	if (serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		serializer.WriteProperty<vector<unique_ptr<TypeDescriptor>>>(104, "type_descriptors", types);
	}
}

unique_ptr<MacroFunction> MacroFunction::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<MacroType>(100, "type");
	auto parameters = deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(101, "parameters");
	auto default_parameters = deserializer.ReadPropertyWithDefault<
	    InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>>>(
	    102, "default_parameters");
	auto logical_types =
	    deserializer.ReadPropertyWithExplicitDefault<vector<LogicalType>>(103, "types", vector<LogicalType>());
	vector<unique_ptr<TypeDescriptor>> type_descriptors;
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<TypeDescriptor>>>(104, "type_descriptors", type_descriptors);
	unique_ptr<MacroFunction> result;
	switch (type) {
	case MacroType::SCALAR_MACRO:
		result = ScalarMacroFunction::Deserialize(deserializer);
		break;
	case MacroType::TABLE_MACRO:
		result = TableMacroFunction::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of MacroFunction!");
	}
	result->parameters = std::move(parameters);
	result->default_parameters = std::move(default_parameters);
	if (!type_descriptors.empty()) {
		result->types = std::move(type_descriptors);
	} else {
		// written by a version that stored the declared types as resolved LogicalTypes
		for (auto &logical_type : logical_types) {
			result->types.push_back(logical_type.id() == LogicalTypeId::UNKNOWN
			                            ? nullptr
			                            : make_uniq<TypeDescriptor>(TypeDescriptor::FromLogicalType(logical_type)));
		}
	}
	result->FinalizeDeserialization();
	return result;
}

} // namespace duckdb
