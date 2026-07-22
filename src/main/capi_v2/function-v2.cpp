#include "capi_v2_internal.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

// ---------------------------------------------------------------------------
// Bind-time argument list accessors, shared by scalar and aggregate function
// bind callbacks (api_spec/v2/function/function.yaml). The handle is backed by
// BindArgumentsV2 (capi_v2_internal.hpp): it borrows the argument expressions
// and the bound function's argument-type list, kept in sync so both the scalar
// binder (mutates the argument expressions directly) and the aggregate binder
// (resizes the argument expressions to the argument-type list) land on a valid
// post-bind state. Arguments surface as types and folded values only, never as
// expressions, so the interface generalizes to table functions (whose
// arguments are constant values engine-side). Bodies wrap in WithErrorHandler
// so a thrown DuckDB exception becomes a V2 error code.
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_get_count(duckdb_v2_bind_arguments_handle args, idx_t *out_count,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_get_count");
		}
		*out_count = duckdb::ToBindArguments(args)->arguments->size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_get_type(duckdb_v2_bind_arguments_handle args, idx_t index,
                                                  duckdb_v2_logical_type_handle *out_type,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_type) {
			*out_type = nullptr;
		}
		if (!args || !out_type) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_get_type");
		}
		auto &arguments = *duckdb::ToBindArguments(args)->arguments;
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		*out_type =
		    reinterpret_cast<_duckdb_v2_logical_type *>(new duckdb::LogicalType(arguments[index]->GetReturnType()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_fold(duckdb_v2_bind_arguments_handle args, duckdb_v2_context_handle ctx,
                                              idx_t index, duckdb_v2_value_handle *out_value,
                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_value) {
			*out_value = nullptr;
		}
		if (!args || !ctx || !out_value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_fold");
		}
		auto &arguments = *duckdb::ToBindArguments(args)->arguments;
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		auto &expr = *arguments[index];
		if (!expr.IsFoldable()) {
			throw duckdb::InvalidInputException("duckdb_v2_bind_arguments_fold: argument is not constant-foldable");
		}
		// The context arrives with the lock held and a transaction active. A
		// runtime error while evaluating (e.g. divide by zero) propagates.
		auto value = duckdb::ExpressionExecutor::EvaluateScalar(*duckdb::ToContext(ctx), expr);
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(value)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_set_constant(duckdb_v2_bind_arguments_handle args, idx_t index,
                                                      duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args || !value) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_set_constant");
		}
		auto &arguments = *duckdb::ToBindArguments(args)->arguments;
		if (index >= arguments.size()) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		arguments[index] = duckdb::make_uniq<duckdb::BoundConstantExpression>(*duckdb::ToValue(value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_truncate(duckdb_v2_bind_arguments_handle args, idx_t count,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_truncate");
		}
		auto &bind_args = *duckdb::ToBindArguments(args);
		auto &arguments = *bind_args.arguments;
		if (count > arguments.size()) {
			throw duckdb::InvalidInputException("bind_arguments_truncate: count exceeds the current argument count");
		}
		arguments.resize(count);
		// Keep the bound function's argument-type list in sync (shrink only).
		if (bind_args.argument_types && count < bind_args.argument_types->size()) {
			bind_args.argument_types->resize(count);
		}
	});
}
