#include "capi_v2_internal.hpp"

#include "duckdb/execution/expression_executor.hpp"

// ---------------------------------------------------------------------------
// Bind-time argument list accessors, shared by scalar, aggregate, and table
// function bind callbacks (api_spec/v2/function/function.yaml). The handle is
// backed by BindArgumentsV2 (capi_v2_internal.hpp), which borrows either the
// argument expressions (scalar / aggregate) or pre-assembled constant values
// (table); the accessors below switch on which backing is set. The list is
// read-only: arguments surface as types, folded values, and names, never as
// expressions. Bodies wrap in WithErrorHandler so a thrown DuckDB exception
// becomes a V2 error code.
// ---------------------------------------------------------------------------

namespace duckdb {
namespace {

idx_t BindArgumentsCount(const BindArgumentsV2 &args) {
	return args.values ? args.values->size() : args.arguments->size();
}

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_get_count(duckdb_v2_bind_arguments_handle args, idx_t *out_count,
                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_get_count");
		}
		*out_count = duckdb::BindArgumentsCount(*duckdb::ToBindArguments(args));
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
		auto &bind_args = *duckdb::ToBindArguments(args);
		if (index >= duckdb::BindArgumentsCount(bind_args)) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		const auto &type =
		    bind_args.values ? (*bind_args.values)[index].type() : (*bind_args.arguments)[index]->GetReturnType();
		*out_type = reinterpret_cast<_duckdb_v2_logical_type *>(new duckdb::LogicalType(type));
	});
}

DUCKDB_V2_ERROR duckdb_v2_bind_arguments_get_name(duckdb_v2_bind_arguments_handle args, idx_t index,
                                                  duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (out_name) {
			*out_name = duckdb_v2_identifier_t {nullptr, 0};
		}
		if (!args || !out_name) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_bind_arguments_get_name");
		}
		auto &bind_args = *duckdb::ToBindArguments(args);
		if (index >= duckdb::BindArgumentsCount(bind_args)) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		const duckdb::Identifier *name = nullptr;
		if (bind_args.values) {
			name = &(*bind_args.value_names)[index];
		} else if (bind_args.argument_names) {
			name = &(*bind_args.argument_names)[index];
		}
		// An unnamed slot (an unnamed vararg) reports the empty view.
		if (name && !name->empty()) {
			*out_name = duckdb::ToStr(*name);
		}
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
		auto &bind_args = *duckdb::ToBindArguments(args);
		if (index >= duckdb::BindArgumentsCount(bind_args)) {
			throw duckdb::InvalidInputException("bind argument index out of range");
		}
		// Table arguments are constant values engine-side; folding is a copy.
		if (bind_args.values) {
			*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value((*bind_args.values)[index]));
			return;
		}
		auto &expr = *(*bind_args.arguments)[index];
		if (!expr.IsFoldable()) {
			throw duckdb::InvalidInputException("duckdb_v2_bind_arguments_fold: argument is not constant-foldable");
		}
		// The context arrives with the lock held and a transaction active. A
		// runtime error while evaluating (e.g. divide by zero) propagates.
		auto value = duckdb::ExpressionExecutor::EvaluateScalar(*duckdb::ToContext(ctx), expr);
		*out_value = reinterpret_cast<_duckdb_v2_value *>(new duckdb::Value(std::move(value)));
	});
}
