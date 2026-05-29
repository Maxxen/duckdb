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

struct ScalarFunctionV2 {
	struct RuntimeInfo final : public ScalarFunctionInfo {
		duckdb_v2_scalar_function_bind_callback_cb bind_cb = nullptr;
		duckdb_v2_scalar_function_init_callback_cb init_cb = nullptr;
		duckdb_v2_scalar_function_exec_callback_cb exec_cb = nullptr;

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

	vector<pair<string, LogicalType>> parameters;
	LogicalType return_type;

	static auto BindCallback(BindScalarFunctionInput &input) -> unique_ptr<FunctionData> {
		const auto &info = input.GetBoundFunction().GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.bind_cb);

		duckdb_v2_scalar_function_bind_args args = {};
		args.struct_size = sizeof(args);
		args.context = static_cast<duckdb_v2_context_ptr>(&input.GetClientContext());
		args.function_name = input.GetBoundFunction().GetName().c_str();
		args.user_data = info.user_data;

		ErrorInfoV2 err {};
		auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&err);

		info.bind_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}

		// If the user set the bind data, move it out here

		if (args.out_bind_data) {
			auto result = make_uniq<ScalarFunctionBindDataV2>();

			result->user_data = args.out_bind_data;
			result->user_data_copy_cb = args.out_bind_data_copy;
			result->user_data_equals_cb = args.out_bind_data_equality;
			result->user_data_destructor_cb = args.out_bind_data_destructor;

			return std::move(result);
		}

		return nullptr;
	}

	static auto InitCallback(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data)
	    -> unique_ptr<FunctionLocalState> {
		const auto &info = expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.init_cb);

		duckdb_v2_scalar_function_init_args args = {};
		args.struct_size = sizeof(args);
		args.context = static_cast<duckdb_v2_context_ptr>(&state.GetContext());
		args.function_name = expr.function.GetName().c_str();
		args.user_data = info.user_data;
		args.bind_data = bind_data ? bind_data->Cast<ScalarFunctionBindDataV2>().user_data : nullptr;

		ErrorInfoV2 err {};
		auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&err);

		info.init_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}

		// If the user set the local state, move it out here
		if (args.out_init_data) {
			auto result = make_uniq<ScalarFunctionStateDataV2>();
			result->user_data = args.out_init_data;
			result->user_data_destructor_cb = info.user_data_destructor_cb;

			return std::move(result);
		}

		return nullptr;
	}

	static auto ExecCallback(DataChunk &input, ExpressionState &state, Vector &result) -> void {
		auto &expr = state.expr.Cast<BoundFunctionExpression>();
		const auto &info = expr.function.GetExtraFunctionInfo().Cast<RuntimeInfo>();

		D_ASSERT(info.exec_cb);

		duckdb_v2_scalar_function_exec_args args = {};
		args.struct_size = sizeof(args);
		args.function_name = expr.function.GetName().c_str();
		args.user_data = info.user_data;
		args.input = static_cast<duckdb_v2_data_chunk_ptr>(&input);
		args.result = static_cast<duckdb_v2_vector_ptr>(&result);

		// Setup bind data (if provided)
		if (auto bind_ptr = expr.bind_info.get()) {
			args.bind_data = bind_ptr->Cast<ScalarFunctionBindDataV2>().user_data;
		}

		// Setup local state (if provided)
		if (auto state_ptr = ExecuteFunctionState::GetFunctionState(state)) {
			args.init_data = state_ptr->Cast<ScalarFunctionStateDataV2>().user_data;
		}

		ErrorInfoV2 err {};
		auto err_ptr = static_cast<duckdb_v2_error_info_ptr>(&err);

		info.exec_cb(&args, &err_ptr);

		if (err.HasError()) {
			err.ThrowAsException();
		}
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_create(duckdb_v2_context_ptr ctx,
                                                              duckdb_v2_scalar_function_builder_ptr *out,
                                                              duckdb_v2_error_info_ptr *err) {
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
                                                                const char *name, duckdb_v2_error_info_ptr *err) {
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

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_add_parameter(duckdb_v2_scalar_function_builder_ptr func,
                                                                     const char *name, duckdb_v2_logical_type_ptr type,
                                                                     duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!name) {
			throw duckdb::InvalidInputException("Parameter name cannot be null.");
		}
		if (strlen(name) == 0) {
			throw duckdb::InvalidInputException("Parameter name cannot be empty.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Parameter type pointer cannot be null.");
		}

		const auto &ltype = *static_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Parameter type cannot be invalid.");
		}

		auto &builder = *static_cast<duckdb::ScalarFunctionV2 *>(func);
		builder.parameters.emplace_back(name, ltype);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_return_type(duckdb_v2_scalar_function_builder_ptr func,
                                                                       duckdb_v2_logical_type_ptr type,
                                                                       duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Return type pointer cannot be null.");
		}
		const auto &ltype = *static_cast<duckdb::LogicalType *>(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Return type cannot be invalid.");
		}
		auto &builder = *static_cast<duckdb::ScalarFunctionV2 *>(func);
		builder.return_type = ltype;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_scalar_function_builder_set_bind_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                    duckdb_v2_scalar_function_bind_callback_cb callback,
                                                    duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.bind_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_scalar_function_builder_set_init_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                    duckdb_v2_scalar_function_init_callback_cb callback,
                                                    duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.init_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_scalar_function_builder_set_exec_callback(duckdb_v2_scalar_function_builder_ptr func,
                                                    duckdb_v2_scalar_function_exec_callback_cb callback,
                                                    duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		static_cast<duckdb::ScalarFunctionV2 *>(func)->info.exec_cb = callback;
	});
}
DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_set_user_data(duckdb_v2_scalar_function_builder_ptr func,
                                                                     void *data, duckdb_v2_user_data_destroy_cb destroy,
                                                                     duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &function = *static_cast<duckdb::ScalarFunctionV2 *>(func);
		function.info.user_data = data;
		function.info.user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_scalar_function_builder_register(duckdb_v2_context_ptr ctx,
                                                                duckdb_v2_scalar_function_builder_ptr func,
                                                                duckdb_v2_error_info_ptr *err) {
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
		delete static_cast<duckdb::ScalarFunctionV2 *>(*func);
		*func = nullptr;
	});
}
