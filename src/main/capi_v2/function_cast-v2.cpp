#include "capi_v2_internal.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {
namespace {

struct CastFunctionV2 {
	unique_ptr<LogicalType> source_type;
	unique_ptr<LogicalType> target_type;
	int64_t implicit_cast_cost = -1;

	duckdb_v2_cast_function_exec_callback_fn exec_cb = nullptr;
	shared_ptr<OpaqueDataHandle> user_data = nullptr;
};

struct CastFunctionBoundDataV2 final : public BoundCastData {
	CastFunctionBoundDataV2(duckdb_v2_cast_function_exec_callback_fn exec_cb_p,
	                        shared_ptr<OpaqueDataHandle> user_data_p)
	    : exec_cb(exec_cb_p), user_data(std::move(user_data_p)) {
	}

	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<CastFunctionBoundDataV2>(exec_cb, user_data);
	}

	duckdb_v2_cast_function_exec_callback_fn exec_cb;
	shared_ptr<OpaqueDataHandle> user_data;
};

// --- Callback info struct (passed to the user callback as an opaque handle) --

struct CastFunctionExecInfoV2 {
	void *user_data = nullptr;
	Vector *input = nullptr;
	Vector *output = nullptr;
	idx_t count = 0;
	DUCKDB_V2_CAST_MODE mode = DUCKDB_V2_CAST_MODE_NORMAL;
};

bool CastFunctionExec(Vector &input, Vector &output, idx_t count, CastParameters &parameters) {
	auto &bound_data = parameters.cast_data->Cast<CastFunctionBoundDataV2>();

	CastFunctionExecInfoV2 cb_info;
	cb_info.user_data = bound_data.user_data ? bound_data.user_data->GetData() : nullptr;
	cb_info.input = &input;
	cb_info.output = &output;
	cb_info.count = count;
	cb_info.mode = parameters.error_message == nullptr ? DUCKDB_V2_CAST_MODE_NORMAL : DUCKDB_V2_CAST_MODE_TRY;

	auto info_handle = reinterpret_cast<duckdb_v2_cast_function_exec_info_handle>(&cb_info);

	ErrorInfoV2 err {};
	auto err_ptr = reinterpret_cast<duckdb_v2_error_info_handle>(&err);
	bound_data.exec_cb(info_handle, &err_ptr);

	const auto success = err.code == DUCKDB_V2_ERROR_NONE;
	if (!success) {
		HandleCastError::AssignError(err.message, parameters);
	}

	return success;
}

} // namespace
} // namespace duckdb

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_create(duckdb_v2_cast_function_builder_handle *out,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = reinterpret_cast<duckdb_v2_cast_function_builder_handle>(new duckdb::CastFunctionV2());
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_set_source_type(duckdb_v2_cast_function_builder_handle func,
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

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_set_target_type(duckdb_v2_cast_function_builder_handle func,
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

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_set_implicit_cast_cost(duckdb_v2_cast_function_builder_handle func,
                                                                       int64_t cost, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::CastFunctionV2 *>(func)->implicit_cast_cost = cost;
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_set_exec_callback(duckdb_v2_cast_function_builder_handle func,
                                                                  duckdb_v2_cast_function_exec_callback_fn callback,
                                                                  duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		reinterpret_cast<duckdb::CastFunctionV2 *>(func)->exec_cb = callback;
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_set_user_data(duckdb_v2_cast_function_builder_handle func,
                                                              duckdb_v2_opaque data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		builder.user_data = duckdb::make_shared_ptr<duckdb::OpaqueDataHandle>(data.ptr, data.destroy, data.equals);
	});
}

static void RegisterCastFunctionV2(duckdb::CastFunctionSet &casts, duckdb::CastFunctionV2 &builder) {
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

	auto bound_data = duckdb::make_uniq<duckdb::CastFunctionBoundDataV2>(builder.exec_cb, builder.user_data);
	duckdb::BoundCastInfo cast_info(duckdb::CastFunctionExec, std::move(bound_data));

	casts.RegisterCastFunction(source_type, target_type, std::move(cast_info), builder.implicit_cast_cost);
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_register_with_connection(duckdb_v2_connection_handle conn,
                                                                         duckdb_v2_cast_function_builder_handle func,
                                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		auto &ctx = *duckdb::ToConn(conn)->context;
		ctx.RunFunctionInTransaction([&]() { RegisterCastFunctionV2(duckdb::CastFunctionSet::Get(ctx), builder); });
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_register_with_extension(duckdb_v2_extension_handle extension,
                                                                        duckdb_v2_cast_function_builder_handle func,
                                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!extension) {
			throw duckdb::InvalidInputException("Extension pointer cannot be null.");
		}
		if (!func) {
			throw duckdb::InvalidInputException("Function pointer cannot be null.");
		}
		auto &builder = *reinterpret_cast<duckdb::CastFunctionV2 *>(func);
		auto &db = duckdb::ToExtension(extension)->GetDatabaseInstance();
		RegisterCastFunctionV2(duckdb::CastFunctionSet::Get(db), builder);
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_builder_destroy(duckdb_v2_cast_function_builder_handle *func) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!func || !*func) {
			return;
		}
		delete reinterpret_cast<duckdb::CastFunctionV2 *>(*func);
		*func = nullptr;
	});
}

// --- Exec callback accessors -------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_user_data(duckdb_v2_cast_function_exec_info_handle info,
                                                           void **out_data, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_data) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_data = reinterpret_cast<duckdb::CastFunctionExecInfoV2 *>(info)->user_data;
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_input(duckdb_v2_cast_function_exec_info_handle info,
                                                       duckdb_v2_vector_handle *out_input,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_input) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::CastFunctionExecInfoV2 *>(info);
		*out_input = reinterpret_cast<duckdb_v2_vector_handle>(cb_info.input);
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_output(duckdb_v2_cast_function_exec_info_handle info,
                                                        duckdb_v2_vector_handle *out_output,
                                                        duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_output) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &cb_info = *reinterpret_cast<duckdb::CastFunctionExecInfoV2 *>(info);
		*out_output = reinterpret_cast<duckdb_v2_vector_handle>(cb_info.output);
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_count(duckdb_v2_cast_function_exec_info_handle info, idx_t *out_count,
                                                       duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_count = reinterpret_cast<duckdb::CastFunctionExecInfoV2 *>(info)->count;
	});
}

DUCKDB_V2_ERROR duckdb_v2_cast_function_exec_get_mode(duckdb_v2_cast_function_exec_info_handle info,
                                                      DUCKDB_V2_CAST_MODE *out_mode, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!info) {
			throw duckdb::InvalidInputException("Info handle cannot be null.");
		}
		if (!out_mode) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out_mode = reinterpret_cast<duckdb::CastFunctionExecInfoV2 *>(info)->mode;
	});
}
