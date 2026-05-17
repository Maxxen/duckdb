#include "capi_v2_internal.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace {

struct ScalarFunctionBindDataV2 final : public FunctionData {
	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<ScalarFunctionBindDataV2>();
		copy->user_data = user_data_copy_cb ? user_data_copy_cb(user_data) : user_data;
		copy->user_data_destructor_cb = user_data_destructor_cb;
		copy->user_data_copy_cb = user_data_copy_cb;
		copy->user_data_equals_cb = user_data_equals_cb;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		if (user_data_equals_cb) {
			auto &other_bind_data = other.Cast<ScalarFunctionBindDataV2>();
			return user_data_equals_cb(user_data, other_bind_data.user_data);
		}
		return user_data == other.Cast<ScalarFunctionBindDataV2>().user_data;
	}

	~ScalarFunctionBindDataV2() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
		user_data = nullptr;
		user_data_destructor_cb = nullptr;
	}

	void *user_data;

	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
	duckdb_v2_user_data_copy_cb user_data_copy_cb = nullptr;
	duckdb_v2_user_data_equals_cb user_data_equals_cb = nullptr;
};

struct ScalarFunctionBindInfoV2 {
	unique_ptr<ScalarFunctionBindDataV2> bind_data = nullptr;
};

struct ScalarFunctionStateDataV2 final : public FunctionLocalState {
	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
};

struct ScalarFunctionStateInfoV2 {
	optional_ptr<ScalarFunctionBindDataV2> bind_data = nullptr;
	unique_ptr<ScalarFunctionStateDataV2> local_state = nullptr;
};

struct ScalarFunctionInvokeInfoV2 {
	optional_ptr<ScalarFunctionBindDataV2> bind_data = nullptr;
	optional_ptr<ScalarFunctionStateDataV2> local_state = nullptr;

	/* TODO */
};

struct ScalarFunctionV2 {
	struct RuntimeInfo final : public ScalarFunctionInfo {
		duckdb_v2_scalar_function_bind_cb bind_cb = nullptr;
		duckdb_v2_scalar_function_state_cb state_cb = nullptr;
		duckdb_v2_scalar_function_invoke_cb invoke_cb = nullptr;

		duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
		void *user_data = nullptr;

		~RuntimeInfo() override {
			if (user_data && user_data_destructor_cb) {
				user_data_destructor_cb(user_data);
			}
		}
	};

	RuntimeInfo info;
	string name;

	static auto BindCallback(BindScalarFunctionInput &input) -> unique_ptr<FunctionData> {
		const auto &info = input.GetBoundFunction().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.bind_cb);

		ErrorInfoV2 error_info;
		ScalarFunctionBindInfoV2 bind_args;

		const auto arg_ptr = reinterpret_cast<duckdb_v2_scalar_function_bind_info_ptr>(&bind_args);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&input.GetClientContext());
		const auto err_ptr = reinterpret_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.bind_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw BinderException(error_info.message);
		}

		// If the user set the bind data, move it out here
		if (bind_args.bind_data) {
			return std::move(bind_args.bind_data);
		}

		return nullptr;
	}

	static auto StateCallback(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
	    -> unique_ptr<FunctionLocalState> {
		const auto &info = expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.state_cb);

		ErrorInfoV2 error_info;
		ScalarFunctionStateInfoV2 state_args;

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.bind_info.get()) {
			state_args.bind_data = &bind_ptr->Cast<ScalarFunctionBindDataV2>();
		}

		const auto arg_ptr = reinterpret_cast<duckdb_v2_scalar_function_state_info_ptr>(&state_args);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&state.GetContext());
		const auto err_ptr = reinterpret_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.state_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw InvalidInputException(error_info.message);
		}

		// If the user set the local state, move it out here
		if (state_args.local_state) {
			return std::move(state_args.local_state);
		}

		return nullptr;
	}

	static auto InvokeCallback(DataChunk &args, ExpressionState &state, Vector &result) -> void {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = func_expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.invoke_cb);

		ErrorInfoV2 error_info;
		ScalarFunctionInvokeInfoV2 invoke_args;

		// Setup bind data (if provided)
		if (auto bind_ptr = func_expr.bind_info.get()) {
			invoke_args.bind_data = &bind_ptr->Cast<ScalarFunctionBindDataV2>();
		}

		// Setup local state (if provided)
		if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
			invoke_args.local_state = &state_ptr->Cast<ScalarFunctionStateDataV2>();
		}

		// Setup arguments to pass to the user-provided invoke callback and call it
		const auto arg_ptr = reinterpret_cast<duckdb_v2_error_info_ptr>(&invoke_args);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&state.GetContext());
		const auto err_ptr = reinterpret_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.invoke_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw InvalidInputException(error_info.message);
		}

		// TODO: populate result vector based on invoke_args
		ConstantVector::Reference(result, Value::INTEGER(42), count_t(args.size()));
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_create(duckdb_v2_context_ptr ctx, duckdb_v2_error_info_ptr err,
                                                      duckdb_v2_scalar_function_ptr *out) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}

		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}

		*out = static_cast<duckdb_v2_scalar_function_ptr>(new duckdb::ScalarFunctionV2());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_name(duckdb_v2_scalar_function_ptr func, const char *name) {
	if (!func) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	static_cast<duckdb::ScalarFunctionV2 *>(func)->name = name;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_bind_callback(duckdb_v2_scalar_function_ptr func,
                                                                 duckdb_v2_scalar_function_bind_cb callback) {
	if (!func) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	static_cast<duckdb::ScalarFunctionV2 *>(func)->info.bind_cb = callback;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_bind_set_data(duckdb_v2_scalar_function_bind_info_ptr args, void *data,
                                                             duckdb_v2_user_data_copy_cb copy,
                                                             duckdb_v2_user_data_equals_cb equals,
                                                             duckdb_v2_user_data_destroy_cb destroy) {
	if (!args) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}

	auto &bind_info = *static_cast<duckdb::ScalarFunctionBindInfoV2 *>(args);

	bind_info.bind_data = duckdb::make_uniq<duckdb::ScalarFunctionBindDataV2>();
	bind_info.bind_data->user_data = data;
	bind_info.bind_data->user_data_copy_cb = copy;
	bind_info.bind_data->user_data_equals_cb = equals;
	bind_info.bind_data->user_data_destructor_cb = destroy;

	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_state_callback(duckdb_v2_scalar_function_ptr func,
                                                                  duckdb_v2_scalar_function_state_cb callback) {
	if (!func) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	static_cast<duckdb::ScalarFunctionV2 *>(func)->info.state_cb = callback;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_state_set_data(duckdb_v2_scalar_function_state_info_ptr args, void *data,
                                                              duckdb_v2_user_data_destroy_cb destroy) {
	if (!args) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}

	auto &state_info = *static_cast<duckdb::ScalarFunctionStateInfoV2 *>(args);

	state_info.local_state = duckdb::make_uniq<duckdb::ScalarFunctionStateDataV2>();
	state_info.local_state->user_data = data;
	state_info.local_state->user_data_destructor_cb = destroy;

	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_invoke_callback(duckdb_v2_scalar_function_ptr func,
                                                                   duckdb_v2_scalar_function_invoke_cb callback) {
	if (!func) {
		return DUCKDB_V2_ERROR_INVALID_INPUT;
	}
	static_cast<duckdb::ScalarFunctionV2 *>(func)->info.invoke_cb = callback;
	return DUCKDB_V2_ERROR_NONE;
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_state_get_bind_data(duckdb_v2_scalar_function_state_info_ptr args,
                                                                   duckdb_v2_error_info_ptr err, void **out_data) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("State info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &info = *static_cast<duckdb::ScalarFunctionStateInfoV2 *>(args);

		if (!info.bind_data) {
			throw duckdb::InvalidInputException("No bind data found for this function invocation.");
		}

		*out_data = info.bind_data->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_invoke_get_bind_data(duckdb_v2_scalar_function_invoke_info_ptr args,
                                                                    duckdb_v2_error_info_ptr err, void **out_data) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("Invoke info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &info = *static_cast<duckdb::ScalarFunctionInvokeInfoV2 *>(args);

		if (!info.bind_data) {
			throw duckdb::InvalidInputException("No bind data found for this function invocation.");
		}

		*out_data = info.bind_data->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_invoke_get_state_data(duckdb_v2_scalar_function_invoke_info_ptr args,
                                                                     duckdb_v2_error_info_ptr err, void **out_data) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("Invoke info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &info = *static_cast<duckdb::ScalarFunctionInvokeInfoV2 *>(args);

		if (!info.local_state) {
			throw duckdb::InvalidInputException("No local state found for this function invocation.");
		}

		*out_data = info.local_state->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_register(duckdb_v2_context_ptr ctx, duckdb_v2_scalar_function_ptr func,
                                                        duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &builder = *static_cast<duckdb::ScalarFunctionV2 *>(func);
		auto &context = *static_cast<duckdb::ClientContext *>(ctx);

		if (builder.name.empty()) {
			throw duckdb::InvalidInputException("Function name cannot be empty.");
		}

		if (builder.info.invoke_cb == nullptr) {
			throw duckdb::InvalidInputException("Invoke callback must be set for the function.");
		}

		duckdb::ScalarFunction function(builder.name, {}, duckdb::LogicalType::INTEGER,
		                                duckdb::ScalarFunctionV2::InvokeCallback);

		if (builder.info.bind_cb) {
			function.SetBindCallback(duckdb::ScalarFunctionV2::BindCallback);
		}

		if (builder.info.invoke_cb) {
			function.SetInitStateCallback(duckdb::ScalarFunctionV2::StateCallback);
		}

		function.SetExtraFunctionInfo<duckdb::ScalarFunctionV2::RuntimeInfo>(builder.info);

		// Also verify signature so that function parameters make sense
		function.GetSignature().Verify();

		auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
		duckdb::CreateScalarFunctionInfo sf_info(function);
		sf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(context, sf_info);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_destroy(duckdb_v2_scalar_function_ptr func) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->~ScalarFunctionV2();
	});
}
