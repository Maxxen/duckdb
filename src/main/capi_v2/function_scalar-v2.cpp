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

		duckdb_v2_scalar_function_bind_args args = {};
		args.struct_size = sizeof(args);
		args.function_name = ToStr(input.GetBoundFunction().GetName());
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;

		// Scalar binds mutate the argument expressions directly; expose both the
		// children and the bound argument-type list so truncation keeps them in sync.
		BindArgumentsV2 bind_args;
		bind_args.arguments = &input.GetArguments();
		bind_args.argument_types = &input.GetBoundFunction().GetArguments();
		args.arguments = reinterpret_cast<duckdb_v2_bind_arguments_handle>(&bind_args);

		// Binding always runs under a client context.
		auto context = reinterpret_cast<duckdb_v2_context_handle>(&input.GetClientContext());
		InvokeWithErrorSlot<BinderException>(
		    [&](duckdb_v2_error_info_handle *err) { info.bind_cb(&args, context, err); });

		// If the user set the bind data, move it out here

		if (args.out_bind_data.ptr) {
			auto result = make_uniq<ScalarFunctionBindDataV2>();

			result->user_data = make_shared_ptr<OpaqueDataHandle>(args.out_bind_data.ptr, args.out_bind_data.destroy,
			                                                      args.out_bind_data.equals);

			return std::move(result);
		}
		return nullptr;
	}

	static auto InitCallback(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
	    -> unique_ptr<FunctionLocalState> {
		const auto &info = expr.Function().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.init_cb);

		duckdb_v2_scalar_function_init_args args = {};
		args.struct_size = sizeof(args);
		args.function_name = ToStr(expr.Function().GetName());
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;

		auto user_bind_data = bind_data ? bind_data->Cast<ScalarFunctionBindDataV2>().user_data : nullptr;
		args.bind_data = user_bind_data ? user_bind_data->GetData() : nullptr;

		// Null when initialized by a context-free ExpressionExecutor (e.g. an index expression).
		auto context = state.HasContext() ? reinterpret_cast<duckdb_v2_context_handle>(&state.GetContext()) : nullptr;
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err) { info.init_cb(&args, context, err); });

		// If the user set the local state, move it out here
		if (args.out_init_data.ptr) {
			auto result = make_uniq<ScalarFunctionStateDataV2>();
			result->user_data = OpaqueDataHandle(args.out_init_data.ptr, args.out_init_data.destroy);

			return std::move(result);
		}

		return nullptr;
	}

	static auto ExecCallback(DataChunk &input, ExpressionState &state, Vector &result) -> void {
		auto &expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = expr.Function().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.exec_cb);

		duckdb_v2_scalar_function_exec_args args = {};
		args.struct_size = sizeof(args);
		args.function_name = ToStr(expr.Function().GetName());
		args.user_data = info.user_data ? info.user_data->GetData() : nullptr;
		args.input = reinterpret_cast<_duckdb_v2_data_chunk *>(&input);
		args.result = reinterpret_cast<_duckdb_v2_vector *>(&result);

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.BindInfo().get()) {
			const auto &bind_data = bind_ptr->Cast<ScalarFunctionBindDataV2>();
			args.bind_data = bind_data.user_data ? bind_data.user_data->GetData() : nullptr;
		}

		// Setup local state (if provided)
		if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
			const auto &state_data = state_ptr->Cast<ScalarFunctionStateDataV2>();
			args.init_data = state_data.user_data.GetData();
		}

		// Null for invocations that evaluate the function without a client context (e.g. an index expression).
		auto context = state.HasContext() ? reinterpret_cast<duckdb_v2_context_handle>(&state.GetContext()) : nullptr;
		InvokeWithErrorSlot<InvalidInputException>(
		    [&](duckdb_v2_error_info_handle *err_ptr) { info.exec_cb(&args, context, err_ptr); });
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_create(duckdb_v2_context_handle ctx,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_name(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_add_parameter(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_varargs(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_return_type(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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

DUCKDB_V2_API_CALL_t
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
DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_user_data(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_register(duckdb_v2_context_handle ctx,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_property(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_get_property(duckdb_v2_scalar_function_builder_handle func,
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_destroy(duckdb_v2_scalar_function_builder_handle *func) {
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
