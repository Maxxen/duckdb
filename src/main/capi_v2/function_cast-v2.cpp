#include "capi_v2_internal.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {
namespace {

struct CastFunctionUserDataV2 {
	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_fn user_data_destructor_cb = nullptr;

	~CastFunctionUserDataV2() {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}
};

struct CastFunctionV2 {
	unique_ptr<LogicalType> source_type;
	unique_ptr<LogicalType> target_type;
	int64_t implicit_cast_cost = -1;

	duckdb_v2_cast_function_exec_callback_fn exec_cb = nullptr;

	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_fn user_data_destructor_cb = nullptr;
};

struct CastFunctionBoundDataV2 final : public BoundCastData {
	CastFunctionBoundDataV2(duckdb_v2_cast_function_exec_callback_fn exec_cb_p,
	                        shared_ptr<CastFunctionUserDataV2> user_data_p)
	    : exec_cb(exec_cb_p), user_data(std::move(user_data_p)) {
	}

	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<CastFunctionBoundDataV2>(exec_cb, user_data);
	}

	duckdb_v2_cast_function_exec_callback_fn exec_cb;
	shared_ptr<CastFunctionUserDataV2> user_data;
};

bool CastFunctionExec(Vector &input, Vector &output, idx_t count, CastParameters &parameters) {
	auto &bound_data = parameters.cast_data->Cast<CastFunctionBoundDataV2>();

	duckdb_v2_cast_function_exec_args args = {};
	args.struct_size = sizeof(args);
	args.user_data = bound_data.user_data ? bound_data.user_data->user_data : nullptr;
	args.input = reinterpret_cast<duckdb_v2_vector_handle>(&input);
	args.output = reinterpret_cast<duckdb_v2_vector_handle>(&output);
	args.count = count;
	args.mode = parameters.error_message == nullptr ? DUCKDB_V2_CAST_MODE_NORMAL : DUCKDB_V2_CAST_MODE_TRY;

	ErrorInfoV2 err {};
	auto err_ptr = reinterpret_cast<duckdb_v2_error_info_handle>(&err);
	bound_data.exec_cb(&args, &err_ptr);

	const auto success = err.code == DUCKDB_V2_ERROR_NONE;
	if (!success) {
		HandleCastError::AssignError(err.message, parameters);
	}

	return success;
}

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_create(duckdb_v2_context_handle context,
                                                            duckdb_v2_cast_function_builder_handle *out,
                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = reinterpret_cast<duckdb_v2_cast_function_builder_handle>(new duckdb::CastFunctionV2());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_set_source_type(duckdb_v2_cast_function_builder_handle func,
                                                                     duckdb_v2_logical_type_handle type,
                                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Source type pointer cannot be null.");
		}
		const auto &ltype = *duckdb::ToLogicalType(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Source type cannot be invalid.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		builder.source_type = duckdb::make_uniq<duckdb::LogicalType>(ltype);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_set_target_type(duckdb_v2_cast_function_builder_handle func,
                                                                     duckdb_v2_logical_type_handle type,
                                                                     duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Target type pointer cannot be null.");
		}
		const auto &ltype = *duckdb::ToLogicalType(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Target type cannot be invalid.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		builder.target_type = duckdb::make_uniq<duckdb::LogicalType>(ltype);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_set_implicit_cast_cost(duckdb_v2_cast_function_builder_handle func,
                                                                            int64_t cost,
                                                                            duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::CastFunctionV2 *>(func)->implicit_cast_cost = cost;
	});
}

DUCKDB_V2_API_CALL_t
duckdb_v2_cast_function_builder_set_exec_callback(duckdb_v2_cast_function_builder_handle func,
                                                  duckdb_v2_cast_function_exec_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::CastFunctionV2 *>(func)->exec_cb = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_set_user_data(duckdb_v2_cast_function_builder_handle func,
                                                                   void *data, duckdb_v2_user_data_destroy_fn destroy,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		builder.user_data = data;
		builder.user_data_destructor_cb = destroy;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_register(duckdb_v2_context_handle context,
                                                              duckdb_v2_cast_function_builder_handle func,
                                                              duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}

		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);

		if (!builder.source_type) {
			throw duckdb::InvalidInputException("Source type must be set for the cast function.");
		}
		if (!builder.target_type) {
			throw duckdb::InvalidInputException("Target type must be set for the cast function.");
		}
		if (!builder.exec_cb) {
			throw duckdb::InvalidInputException("Exec callback must be set for the cast function.");
		}

		const auto &source_type = *builder.source_type;
		const auto &target_type = *builder.target_type;

		// ANY / INVALID types make no sense as concrete cast endpoints.
		if (duckdb::TypeVisitor::Contains(source_type, duckdb::LogicalTypeId::INVALID) ||
		    duckdb::TypeVisitor::Contains(source_type, duckdb::LogicalTypeId::ANY)) {
			throw duckdb::InvalidInputException("Source type must be a fully defined concrete type.");
		}
		if (duckdb::TypeVisitor::Contains(target_type, duckdb::LogicalTypeId::INVALID) ||
		    duckdb::TypeVisitor::Contains(target_type, duckdb::LogicalTypeId::ANY)) {
			throw duckdb::InvalidInputException("Target type must be a fully defined concrete type.");
		}

		// Transfer ownership of the user data to the (shared) bound data so destroying the builder afterwards
		// does not free it; the destructor now runs when the last bound-data copy is released.
		auto user_data = duckdb::make_shared_ptr<duckdb::CastFunctionUserDataV2>();
		user_data->user_data = builder.user_data;
		user_data->user_data_destructor_cb = builder.user_data_destructor_cb;
		builder.user_data = nullptr;
		builder.user_data_destructor_cb = nullptr;

		auto bound_data = duckdb::make_uniq<duckdb::CastFunctionBoundDataV2>(builder.exec_cb, std::move(user_data));
		duckdb::BoundCastInfo cast_info(duckdb::CastFunctionExec, std::move(bound_data));

		// We're already running inside the active context's transaction (e.g. via connection_execute_with_context),
		// so register directly rather than nesting another transaction.
		auto &casts = duckdb::CastFunctionSet::Get(ctx);
		casts.RegisterCastFunction(source_type, target_type, std::move(cast_info), builder.implicit_cast_cost);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_cast_function_builder_destroy(duckdb_v2_cast_function_builder_handle *func) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!func || !*func) {
			return;
		}
		delete reinterpret_cast<duckdb::CastFunctionV2 *>(*func);
		*func = nullptr;
	});
}
