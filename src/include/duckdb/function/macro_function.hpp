//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/type_descriptor.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class ScalarMacroFunction;

enum class MacroType : uint8_t { VOID_MACRO = 0, TABLE_MACRO = 1, SCALAR_MACRO = 2 };

struct MacroBindResult {
	explicit MacroBindResult(string error_p) : error(std::move(error_p)) {
	}
	explicit MacroBindResult(idx_t function_idx) : function_idx(function_idx) {
	}

	optional_idx function_idx;
	string error;
};

class MacroFunction {
public:
	explicit MacroFunction(MacroType type);

	//! The type
	MacroType type;
	//! The parameters (ColumnRefExpression)
	vector<unique_ptr<ParsedExpression>> parameters;
	//! The default values of the parameters
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> default_parameters;
	//! The declared types of the parameters, as folded, unbound descriptions with any referenced user type
	//! inlined. A null entry is an untyped parameter.
	vector<unique_ptr<TypeDescriptor>> types;
	//! The declared types as written, set by the parser. The binder folds these into `types` and clears them,
	//! so nothing downstream of the binder ever sees them set.
	vector<unique_ptr<TypeExpression>> parsed_types;

public:
	virtual ~MacroFunction() {
	}

	void CopyProperties(MacroFunction &other) const;

	virtual unique_ptr<MacroFunction> Copy() const = 0;

	vector<unique_ptr<ParsedExpression>> GetPositionalParametersForSerialization(Serializer &serializer) const;
	void FinalizeDeserialization();

	static MacroBindResult BindMacroFunction(
	    Binder &binder, const vector<unique_ptr<MacroFunction>> &macro_functions, const Identifier &name,
	    FunctionExpression &function_expr, vector<unique_ptr<ParsedExpression>> &positional_arguments,
	    InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> &named_arguments,
	    idx_t depth);
	//! The declared parameter types, resolved, padded to the parameter count with UNKNOWN for untyped ones
	vector<LogicalType> GetParameterTypes(ClientContext &context) const;
	//! Whether any parameter is typed
	bool HasTypedParameters() const;

	static unique_ptr<DummyBinding>
	CreateDummyBinding(ClientContext &context, const MacroFunction &macro_def, const Identifier &name,
	                   vector<unique_ptr<ParsedExpression>> &positional_arguments,
	                   InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>>
	                       &named_arguments);

	virtual string ToSQL() const;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<MacroFunction> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast macro to type - macro type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast macro to type - macro type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
