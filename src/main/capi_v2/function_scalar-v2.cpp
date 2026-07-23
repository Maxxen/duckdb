#include "capi_v2_internal.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
namespace {

struct ScalarFunctionBindDataV2 final : public FunctionData {
	shared_ptr<OpaqueDataHandle> user_data = nullptr;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<ScalarFunctionBindDataV2>();
		copy->user_data = user_data;
		return std::move(copy);
	}

	auto Equals(const FunctionData &other) const -> bool override {
		const auto &other_data = other.Cast<ScalarFunctionBindDataV2>();
		return user_data && other_data.user_data && user_data->Equals(*other_data.user_data);
	}
};

struct ScalarFunctionStateDataV2 final : public FunctionLocalState {
	OpaqueDataHandle user_data;
};

// --- Callback info structs (passed to user callbacks as opaque handles) ------

struct ScalarFunctionBindInfoV2 {
	duckdb_v2_identifier_t function_name = {};
	void *user_data = nullptr;
	BindArgumentsV2 *arguments = nullptr;
	duckdb_v2_opaque out_bind_data = {};
};

struct ScalarFunctionInitInfoV2 {
	duckdb_v2_identifier_t function_name = {};
	void *user_data = nullptr;
	void *bind_data = nullptr;
	duckdb_v2_opaque out_init_data = {};
};

struct ScalarFunctionExecInfoV2 {
	duckdb_v2_identifier_t function_name = {};
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	DataChunk *input = nullptr;
	Vector *result = nullptr;
};

struct ScalarFunctionV2 {
	struct RuntimeInfo final : public ScalarFunctionInfo {
		duckdb_v2_scalar_function_bind_callback_fn bind_cb = nullptr;
		duckdb_v2_scalar_function_init_callback_fn init_cb = nullptr;
		duckdb_v2_scalar_function_exec_callback_fn exec_cb = nullptr;
		shared_ptr<OpaqueDataHandle> user_data = nullptr;
	};

	RuntimeInfo info;
	Identifier name;

	vector<pair<Identifier, LogicalType>> parameters;
	//! Varargs type (INVALID when the function is not variadic).
	LogicalType varargs;
	LogicalType return_type;
	FunctionProperties properties;

	static auto BindCallback(BindScalarFunctionInput &input) -> unique_ptr<FunctionData> {
		const auto &info = input.GetBoundFunction().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.bind_cb);

		// Scalar binds mutate the argument expressions directly.
		BindArgumentsV2 bind_args;
		bind_args.arguments = &input.GetArguments();

		ScalarFunctionBindInfoV2 cb_info;
		cb_info.function_name = ToStr(input.GetBoundFunction().GetName());
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.arguments = &bind_args;

		auto info_handle = reinterpret_cast<duckdb_v2_scalar_function_bind_info_handle>(&cb_info);
		// Binding always runs under a client context.
		auto context = reinterpret_cast<duckdb_v2_context_handle>(&input.GetClientContext());
		InvokeWithErrorSlot<BinderException>(
		    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(info_handle, context, err); });

		// If the user set the bind data, move it out here
		if (cb_info.out_bind_data.ptr) {
			auto result = make_uniq<ScalarFunctionBindDataV2>();
			result->user_data = make_shared_ptr<OpaqueDataHandle>(
			    cb_info.out_bind_data.ptr, cb_info.out_bind_data.destroy, cb_info.out_bind_data.equals);
			return std::move(result);
		}
		return nullptr;
	}

	static auto InitCallback(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
	    -> unique_ptr<FunctionLocalState> {
		const auto &info = expr.Function().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.init_cb);

		auto user_bind_data = bind_data ? bind_data->Cast<ScalarFunctionBindDataV2>().user_data : nullptr;

		ScalarFunctionInitInfoV2 cb_info;
		cb_info.function_name = ToStr(expr.Function().GetName());
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.bind_data = user_bind_data ? user_bind_data->GetData() : nullptr;

		auto info_handle = reinterpret_cast<duckdb_v2_scalar_function_init_info_handle>(&cb_info);
		// Null when initialized by a context-free ExpressionExecutor (e.g. an index expression).
		auto context = state.HasContext() ? reinterpret_cast<duckdb_v2_context_handle>(&state.GetContext()) : nullptr;
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.init_cb(info_handle, context, err); });

		// If the user set the local state, move it out here
		if (cb_info.out_init_data.ptr) {
			auto result = make_uniq<ScalarFunctionStateDataV2>();
			result->user_data = OpaqueDataHandle(cb_info.out_init_data.ptr, cb_info.out_init_data.destroy);
			return std::move(result);
		}

		return nullptr;
	}

	static auto ExecCallback(DataChunk &input, ExpressionState &state, Vector &result) -> void {
		auto &expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = expr.Function().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.exec_cb);

		ScalarFunctionExecInfoV2 cb_info;
		cb_info.function_name = ToStr(expr.Function().GetName());
		cb_info.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		cb_info.input = &input;
		cb_info.result = &result;

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.BindInfo().get()) {
			const auto &bind_data = bind_ptr->Cast<ScalarFunctionBindDataV2>();
			cb_info.bind_data = bind_data.user_data ? bind_data.user_data->GetData() : nullptr;
		}

		// Setup local state (if provided)
		if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
			const auto &state_data = state_ptr->Cast<ScalarFunctionStateDataV2>();
			cb_info.init_data = state_data.user_data.GetData();
		}

		auto info_handle = reinterpret_cast<duckdb_v2_scalar_function_exec_info_handle>(&cb_info);
		// Null for invocations that evaluate the function without a client context (e.g. an index expression).
		auto context = state.HasContext() ? reinterpret_cast<duckdb_v2_context_handle>(&state.GetContext()) : nullptr;
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { info.exec_cb(info_handle, context, err_ptr); });
	}

	// See ThrowFunctionNotSerializable for why these throw.
	static auto SerializeCallback(Serializer &, const optional_ptr<FunctionData>, const BoundScalarFunction &function)
	    -> void {
		ThrowFunctionNotSerializable(function.GetName());
	}

	static auto DeserializeCallback(Deserializer &, BoundScalarFunction &function) -> unique_ptr<FunctionData> {
		ThrowFunctionNotSerializable(function.GetName());
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_create(duckdb_v2_context_handle ctx,
                                                         duckdb_v2_scalar_function_builder_handle *out,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}

		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}

		*out = reinterpret_cast<_duckdb_v2_scalar_function_builder *>(new duckdb::ScalarFunctionV2());
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_set_name(duckdb_v2_scalar_function_builder_handle func,
                                                           duckdb_v2_identifier_t name,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Function name cannot be null.");
		}
		if (name.len == 0) {
			throw duckdb::InvalidInputException("Function name cannot be empty.");
		}

		reinterpret_cast<duckdb::ScalarFunctionV2 *>(func)->name = duckdb::ToIdentifier(name);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_add_parameter(duckdb_v2_scalar_function_builder_handle func,
                                                                duckdb_v2_identifier_t name,
                                                                duckdb_v2_logical_type_handle type,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!name.ptr && name.len > 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be null.");
		}
		if (name.len == 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be empty.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Parameter type pointer cannot be null.");
		}

		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Parameter type cannot be invalid.");
		}

		auto &builder = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		builder.parameters.emplace_back(duckdb::ToString(name), ltype);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_set_varargs(duckdb_v2_scalar_function_builder_handle func,
                                                              duckdb_v2_logical_type_handle type,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Varargs type pointer cannot be null.");
		}
		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Varargs type cannot be invalid.");
		}
		reinterpret_cast<duckdb::ScalarFunctionV2 *>(func)->varargs = ltype;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_set_return_type(duckdb_v2_scalar_function_builder_handle func,
                                                                  duckdb_v2_logical_type_handle type,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Return type pointer cannot be null.");
		}
		const auto &ltype = *reinterpret_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Return type cannot be invalid.");
		}
		auto &builder = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		builder.return_type = ltype;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_scalar_function_builder_set_bind_callback(duckdb_v2_scalar_function_builder_handle func,
                                                    duckdb_v2_scalar_function_bind_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::ScalarFunctionV2 *>(func)->info.bind_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_scalar_function_builder_set_init_callback(duckdb_v2_scalar_function_builder_handle func,
                                                    duckdb_v2_scalar_function_init_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::ScalarFunctionV2 *>(func)->info.init_cb = callback;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_scalar_function_builder_set_exec_callback(duckdb_v2_scalar_function_builder_handle func,
                                                    duckdb_v2_scalar_function_exec_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::ScalarFunctionV2 *>(func)->info.exec_cb = callback;
	});
}
DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_set_user_data(duckdb_v2_scalar_function_builder_handle func,
                                                                duckdb_v2_opaque data,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &function = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		function.info.user_data =
		    duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_register(duckdb_v2_context_handle ctx,
                                                           duckdb_v2_scalar_function_builder_handle func,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!ctx) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &builder = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		auto &context = *reinterpret_cast<duckdb::ClientContext *>(ctx);

		if (builder.name.empty()) {
			throw duckdb::InvalidInputException("Function name cannot be empty.");
		}

		if (builder.info.exec_cb == nullptr) {
			throw duckdb::InvalidInputException("Exec callback must be set for the function.");
		}

		if (builder.return_type.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Return type must be set for the function.");
		}

		if (!builder.return_type.IsComplete()) {
			throw duckdb::InvalidInputException("Return type must be a fully defined concrete type");
		}

		duckdb::ScalarFunction function(builder.name, {}, builder.return_type, duckdb::ScalarFunctionV2::ExecCallback);

		// Set the signature
		for (const auto &[param_name, param_type] : builder.parameters) {
			function.GetSignature().AddParameter(param_name, param_type);
		}

		// Wire the varargs type (if any) before the signature is verified so the
		// engine expands trailing arguments to it during binding.
		if (builder.varargs.id() != duckdb::LogicalTypeId::INVALID) {
			function.SetVarArgs(builder.varargs);
		}

		if (builder.info.bind_cb) {
			function.SetBindCallback(duckdb::ScalarFunctionV2::BindCallback);
		}

		if (builder.info.init_cb) {
			function.SetInitStateCallback(duckdb::ScalarFunctionV2::InitCallback);
		}

		function.SetSerializeCallback(duckdb::ScalarFunctionV2::SerializeCallback);
		function.SetDeserializeCallback(duckdb::ScalarFunctionV2::DeserializeCallback);

		function.SetExtraFunctionInfo<duckdb::ScalarFunctionV2::RuntimeInfo>(builder.info);

		function.SetProperties(builder.properties);

		// Also verify signature so that function parameters make sense
		function.GetSignature().Verify();

		auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
		duckdb::CreateScalarFunctionInfo sf_info(function);
		sf_info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		catalog.CreateFunction(context, sf_info);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_set_property(duckdb_v2_scalar_function_builder_handle func,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_VALUE value,
                                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		duckdb::SetScalarFunctionProperty(builder.properties, key, value);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_get_property(duckdb_v2_scalar_function_builder_handle func,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
                                                               DUCKDB_V2_FUNCTION_PROPERTY_VALUE *out_value,
                                                               duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::ScalarFunctionV2 *>(func);
		*out_value = duckdb::GetScalarFunctionProperty(builder.properties, key);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_builder_destroy(duckdb_v2_scalar_function_builder_handle *func) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!func) {
			return;
		}
		if (!*func) {
			return;
		}
		delete reinterpret_cast<duckdb::ScalarFunctionV2 *>(*func);
		*func = nullptr;
	});
}

// --- Bind callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_function_name(duckdb_v2_scalar_function_bind_info_handle info,
                                                                 duckdb_v2_identifier_t *out_name,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_name = reinterpret_cast<duckdb::ScalarFunctionBindInfoV2 *>(info)->function_name;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_user_data(duckdb_v2_scalar_function_bind_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionBindInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_get_arguments(duckdb_v2_scalar_function_bind_info_handle info,
                                                             duckdb_v2_bind_arguments_handle *out_arguments,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_arguments) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::ScalarFunctionBindInfoV2 *>(info);
		*out_arguments = reinterpret_cast<duckdb_v2_bind_arguments_handle>(cb_info.arguments);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_bind_set_bind_data(duckdb_v2_scalar_function_bind_info_handle info,
                                                             duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		reinterpret_cast<duckdb::ScalarFunctionBindInfoV2 *>(info)->out_bind_data = data;
	});
}

// --- Init callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_get_function_name(duckdb_v2_scalar_function_init_info_handle info,
                                                                 duckdb_v2_identifier_t *out_name,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_name = reinterpret_cast<duckdb::ScalarFunctionInitInfoV2 *>(info)->function_name;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_get_user_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionInitInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_get_bind_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionInitInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_init_set_init_data(duckdb_v2_scalar_function_init_info_handle info,
                                                             duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		reinterpret_cast<duckdb::ScalarFunctionInitInfoV2 *>(info)->out_init_data = data;
	});
}

// --- Exec callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_function_name(duckdb_v2_scalar_function_exec_info_handle info,
                                                                 duckdb_v2_identifier_t *out_name,
                                                                 duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_name = reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info)->function_name;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_user_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_bind_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info)->bind_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_init_data(duckdb_v2_scalar_function_exec_info_handle info,
                                                             void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info)->init_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_input(duckdb_v2_scalar_function_exec_info_handle info,
                                                         duckdb_v2_data_chunk_handle *out_input,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_input) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info);
		*out_input = reinterpret_cast<duckdb_v2_data_chunk_handle>(cb_info.input);
	});
}

DUCKDB_V2_ERROR duckdb_v2_scalar_function_exec_get_result(duckdb_v2_scalar_function_exec_info_handle info,
                                                          duckdb_v2_vector_handle *out_result,
                                                          duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_result) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::ScalarFunctionExecInfoV2 *>(info);
		*out_result = reinterpret_cast<duckdb_v2_vector_handle>(cb_info.result);
	});
}
