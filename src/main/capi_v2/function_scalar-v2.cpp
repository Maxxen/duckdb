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

struct ScalarFunctionStateDataV2 final : public FunctionLocalState {
	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_cb user_data_destructor_cb = nullptr;
};

enum class ScalarFunctionCallbackTypeV2 : uint8_t { BIND, INIT, EXEC };

struct ScalarFunctionCallbackInfoV2 {
	ScalarFunctionCallbackInfoV2(ScalarFunctionCallbackTypeV2 type_p, const string &name_p)
	    : type(type_p), name(name_p) {
	}

	ScalarFunctionCallbackTypeV2 type;
	const string &name;

	// Extra info
	void *user_data = nullptr;

	unique_ptr<ScalarFunctionBindDataV2> set_bind_data = nullptr;
	optional_ptr<ScalarFunctionBindDataV2> get_bind_data = nullptr;

	unique_ptr<ScalarFunctionStateDataV2> set_local_state = nullptr;
	optional_ptr<ScalarFunctionStateDataV2> get_local_state = nullptr;
};

struct ScalarFunctionV2 {
	struct RuntimeInfo final : public ScalarFunctionInfo {
		duckdb_v2_scalar_function_callback_cb bind_cb = nullptr;
		duckdb_v2_scalar_function_callback_cb init_cb = nullptr;
		duckdb_v2_scalar_function_callback_cb exec_cb = nullptr;

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
		ScalarFunctionCallbackInfoV2 cb_info(ScalarFunctionCallbackTypeV2::BIND, input.GetBoundFunction().GetName());

		cb_info.user_data = info.user_data;

		const auto arg_ptr = static_cast<duckdb_v2_scalar_function_info_ptr>(&cb_info);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&input.GetClientContext());
		const auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.bind_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw BinderException(error_info.message);
		}

		// If the user set the bind data, move it out here
		if (cb_info.set_bind_data) {
			return std::move(cb_info.set_bind_data);
		}

		return nullptr;
	}

	static auto InitCallback(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
	    -> unique_ptr<FunctionLocalState> {
		const auto &info = expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.init_cb);

		ErrorInfoV2 error_info;
		ScalarFunctionCallbackInfoV2 cb_info(ScalarFunctionCallbackTypeV2::INIT, expr.function.GetName());

		cb_info.user_data = info.user_data;

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.bind_info.get()) {
			cb_info.get_bind_data = &bind_ptr->Cast<ScalarFunctionBindDataV2>();
		}

		const auto arg_ptr = static_cast<duckdb_v2_scalar_function_info_ptr>(&cb_info);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&state.GetContext());
		const auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.init_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw InvalidInputException(error_info.message);
		}

		// If the user set the local state, move it out here
		if (cb_info.set_local_state) {
			return std::move(cb_info.set_local_state);
		}

		return nullptr;
	}

	static auto ExecCallback(DataChunk &args, ExpressionState &state, Vector &result) -> void {
		auto &expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.exec_cb);

		ErrorInfoV2 error_info;
		ScalarFunctionCallbackInfoV2 cb_info(ScalarFunctionCallbackTypeV2::EXEC, expr.function.GetName());

		cb_info.user_data = info.user_data;

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.bind_info.get()) {
			cb_info.get_bind_data = &bind_ptr->Cast<ScalarFunctionBindDataV2>();
		}

		// Setup local state (if provided)
		if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
			cb_info.get_local_state = &state_ptr->Cast<ScalarFunctionStateDataV2>();
		}

		// Setup arguments to pass to the user-provided exec callback and call it
		const auto arg_ptr = static_cast<duckdb_v2_scalar_function_info_ptr>(&cb_info);
		const auto ctx_ptr = static_cast<duckdb_v2_context_ptr>(&state.GetContext());
		const auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&error_info);

		info.exec_cb(arg_ptr, ctx_ptr, err_ptr);

		if (error_info.code != DUCKDB_V2_ERROR_NONE) {
			throw InvalidInputException(error_info.message);
		}

		// TODO: populate result vector based on exec args
		ConstantVector::Reference(result, Value::INTEGER(42), count_t(args.size()));
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_create(duckdb_v2_context_ptr ctx,
                                                              duckdb_v2_scalar_function_builder_ptr *out,
                                                              duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}

		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}

		*out = static_cast<duckdb_v2_scalar_function_builder_ptr>(new duckdb::ScalarFunctionV2());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_name(duckdb_v2_scalar_function_builder_ptr func,
                                                                const char *name, duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!name) {
			throw duckdb::InvalidInputException("Function name cannot be null.");
		}
		if (strlen(name) == 0) {
			throw duckdb::InvalidInputException("Function name cannot be empty.");
		}

		static_cast<duckdb::ScalarFunctionV2 *>(func)->name = name;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_bind_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                                         duckdb_v2_scalar_function_callback_cb callback,
                                                                         duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.bind_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_init_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                                         duckdb_v2_scalar_function_callback_cb callback,
                                                                         duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.init_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_exec_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                                         duckdb_v2_scalar_function_callback_cb callback,
                                                                         duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.exec_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_bind_data(duckdb_v2_scalar_function_info_ptr args, void *data,
                                                             duckdb_v2_user_data_copy_cb copy,
                                                             duckdb_v2_user_data_equals_cb equals,
                                                             duckdb_v2_user_data_destroy_cb destroy,
                                                             duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("Bind info pointer cannot be null.");
		}

		auto &cb_info = *static_cast<duckdb::ScalarFunctionCallbackInfoV2 *>(args);

		// Can only set bind data from the bind callback
		if (cb_info.type != duckdb::ScalarFunctionCallbackTypeV2::BIND) {
			throw duckdb::InvalidInputException("Bind data can only be set from the bind callback.");
		}

		cb_info.set_bind_data = duckdb::make_uniq<duckdb::ScalarFunctionBindDataV2>();
		cb_info.set_bind_data->user_data = data;
		cb_info.set_bind_data->user_data_copy_cb = copy;
		cb_info.set_bind_data->user_data_equals_cb = equals;
		cb_info.set_bind_data->user_data_destructor_cb = destroy;

		cb_info.get_bind_data = cb_info.set_bind_data.get();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_get_bind_data(duckdb_v2_scalar_function_info_ptr args, void **out_data,
                                                             duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("State info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &info = *static_cast<duckdb::ScalarFunctionCallbackInfoV2 *>(args);

		if (!info.get_bind_data) {
			throw duckdb::InvalidInputException("No bind data found for this function invocation.");
		}

		*out_data = info.get_bind_data->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_set_init_data(duckdb_v2_scalar_function_info_ptr args, void *data,
                                                             duckdb_v2_user_data_destroy_cb destroy,
                                                             duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("State info pointer cannot be null.");
		}

		auto &cb_info = *static_cast<duckdb::ScalarFunctionCallbackInfoV2 *>(args);

		// Can only set init data from the init callback
		if (cb_info.type != duckdb::ScalarFunctionCallbackTypeV2::INIT) {
			throw duckdb::InvalidInputException("Init data can only be set from the init callback.");
		}

		cb_info.set_local_state = duckdb::make_uniq<duckdb::ScalarFunctionStateDataV2>();
		cb_info.set_local_state->user_data = data;
		cb_info.set_local_state->user_data_destructor_cb = destroy;

		cb_info.get_local_state = cb_info.set_local_state.get();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_get_init_data(duckdb_v2_scalar_function_info_ptr args, void **out_data,
                                                             duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!args) {
			throw duckdb::InvalidInputException("Invoke info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &cb_info = *static_cast<duckdb::ScalarFunctionCallbackInfoV2 *>(args);

		if (!cb_info.get_local_state) {
			throw duckdb::InvalidInputException("No local state found for this function invocation.");
		}

		*out_data = cb_info.get_local_state->user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_user_data(duckdb_v2_scalar_function_builder_ptr func,
                                                                     void *data, duckdb_v2_user_data_destroy_cb destroy,
                                                                     duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &function = *static_cast<duckdb::ScalarFunctionV2 *>(func);
		function.info.user_data = data;
		function.info.user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_get_user_data(duckdb_v2_scalar_function_info_ptr func, void **out_data,
                                                             duckdb_v2_error_info_ptr err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function info pointer cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output data pointer cannot be null.");
		}

		const auto &info = *static_cast<duckdb::ScalarFunctionCallbackInfoV2 *>(func);
		*out_data = info.user_data;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_register(duckdb_v2_context_ptr ctx,
                                                                duckdb_v2_scalar_function_builder_ptr func,
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

		if (builder.info.exec_cb == nullptr) {
			throw duckdb::InvalidInputException("Exec callback must be set for the function.");
		}

		duckdb::ScalarFunction function(builder.name, {}, duckdb::LogicalType::INTEGER,
		                                duckdb::ScalarFunctionV2::ExecCallback);

		if (builder.info.bind_cb) {
			function.SetBindCallback(duckdb::ScalarFunctionV2::BindCallback);
		}

		if (builder.info.exec_cb) {
			function.SetInitStateCallback(duckdb::ScalarFunctionV2::InitCallback);
		}

		function.SetExtraFunctionInfo<duckdb::ScalarFunctionV2::RuntimeInfo>(builder.info);
		builder.info.user_data =
		    nullptr; // Clear user data from builder since it's now owned by the function's extra info
		builder.info.user_data_destructor_cb = nullptr; // Clear user data destructor from builder for the same reason

		// Also verify signature so that function parameters make sense
		function.GetSignature().Verify();

		auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
		duckdb::CreateScalarFunctionInfo sf_info(function);
		sf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(context, sf_info);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_destroy(duckdb_v2_scalar_function_builder_ptr *func) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!func) {
			return;
		}
		if (!*func) {
			return;
		}
		static_cast<duckdb::ScalarFunctionV2 *>(*func)->~ScalarFunctionV2();
		*func = nullptr;
	});
}
