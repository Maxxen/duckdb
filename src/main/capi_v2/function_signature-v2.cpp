#include "capi_v2_internal.hpp"

// The function_signature handle is a bare heap-allocated duckdb::FunctionSignature
// (see ToFunctionSignature). Structural validation (unique names, trailing
// defaults) runs at registration through FunctionSignature::Verify(), not here,
// so a partially-built signature is always inspectable.

DUCKDB_V2_ERROR duckdb_v2_function_signature_create(duckdb_v2_function_signature_handle *out_sig,
                                                    duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_sig) {
			throw duckdb::InvalidInputException("Output signature pointer cannot be null.");
		}
		*out_sig = reinterpret_cast<duckdb_v2_function_signature_handle>(new duckdb::FunctionSignature());
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_destroy(duckdb_v2_function_signature_handle *sig) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (!sig || !*sig) {
			return;
		}
		delete duckdb::ToFunctionSignature(*sig);
		*sig = nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_add_parameter(duckdb_v2_function_signature_handle sig,
                                                           duckdb_v2_identifier_t name,
                                                           duckdb_v2_logical_type_handle type,
                                                           duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
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
		const auto &ltype = *duckdb::ToLogicalType(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Parameter type cannot be invalid.");
		}
		duckdb::ToFunctionSignature(sig)->AddParameter(duckdb::ToIdentifier(name), ltype);
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_add_parameter_default(duckdb_v2_function_signature_handle sig,
                                                                   duckdb_v2_identifier_t name,
                                                                   duckdb_v2_logical_type_handle type,
                                                                   duckdb_v2_value_handle value,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
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
		if (!value) {
			throw duckdb::InvalidInputException("Default value pointer cannot be null.");
		}
		const auto &ltype = *duckdb::ToLogicalType(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Parameter type cannot be invalid.");
		}
		const auto &default_value = *duckdb::ToValue(value);
		// Cast the default to a concrete parameter type eagerly so bind callbacks
		// observe the declared type and registration cannot produce call-time cast
		// surprises. ANY (and any other incomplete type) stores the value as-is.
		duckdb::Value stored;
		if (ltype.IsComplete()) {
			std::string cast_error;
			if (!default_value.DefaultTryCastAs(ltype, stored, &cast_error)) {
				throw duckdb::InvalidInputException("Default value for parameter '%s' is not castable to its type: %s",
				                                    duckdb::ToString(name), cast_error);
			}
		} else {
			stored = default_value;
		}
		duckdb::ToFunctionSignature(sig)->AddParameter(duckdb::ToIdentifier(name), ltype, std::move(stored));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_set_varargs(duckdb_v2_function_signature_handle sig,
                                                         duckdb_v2_logical_type_handle type,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Varargs type pointer cannot be null.");
		}
		const auto &ltype = *duckdb::ToLogicalType(type);
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Varargs type cannot be invalid.");
		}
		duckdb::ToFunctionSignature(sig)->SetVarArgs(ltype);
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_set_return_type(duckdb_v2_function_signature_handle sig,
                                                             duckdb_v2_logical_type_handle type,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!type) {
			throw duckdb::InvalidInputException("Return type pointer cannot be null.");
		}
		const auto &ltype = *duckdb::ToLogicalType(type);
		// INVALID doubles as the unset sentinel; accepting it would report success
		// while has_return_type stays false.
		if (ltype.id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Return type cannot be invalid.");
		}
		duckdb::ToFunctionSignature(sig)->SetReturnType(ltype);
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_parameter_count(duckdb_v2_function_signature_handle sig,
                                                                 idx_t *out_count, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_count) {
			throw duckdb::InvalidInputException("Output count pointer cannot be null.");
		}
		*out_count = duckdb::ToFunctionSignature(sig)->GetParameterCount();
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_parameter_name(duckdb_v2_function_signature_handle sig, idx_t index,
                                                                duckdb_v2_identifier_t *out_name,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_name) {
			throw duckdb::InvalidInputException("Output name pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (index >= signature.GetParameterCount()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    signature.GetParameterCount());
		}
		*out_name = duckdb::ToStr(signature.GetParameter(index).GetName());
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_parameter_type(duckdb_v2_function_signature_handle sig, idx_t index,
                                                                duckdb_v2_logical_type_handle *out_type,
                                                                duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_type) {
			throw duckdb::InvalidInputException("Output type pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (index >= signature.GetParameterCount()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    signature.GetParameterCount());
		}
		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(
		    new duckdb::LogicalType(signature.GetParameter(index).GetType()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_parameter_has_default(duckdb_v2_function_signature_handle sig, idx_t index,
                                                                   bool *out, duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (index >= signature.GetParameterCount()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    signature.GetParameterCount());
		}
		*out = signature.GetParameter(index).HasDefaultValue();
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_parameter_default(duckdb_v2_function_signature_handle sig, idx_t index,
                                                                   duckdb_v2_value_handle *out_value,
                                                                   duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_value) {
			throw duckdb::InvalidInputException("Output value pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (index >= signature.GetParameterCount()) {
			throw duckdb::InvalidInputException("Parameter index %llu out of range (have %llu).", index,
			                                    signature.GetParameterCount());
		}
		auto default_value = signature.GetParameter(index).GetDefaultValue();
		if (!default_value) {
			throw duckdb::InvalidInputException("Parameter at index %llu has no default value.", index);
		}
		*out_value = reinterpret_cast<duckdb_v2_value_handle>(new duckdb::Value(*default_value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_has_varargs(duckdb_v2_function_signature_handle sig, bool *out,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = duckdb::ToFunctionSignature(sig)->HasVarArgs();
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_varargs(duckdb_v2_function_signature_handle sig,
                                                         duckdb_v2_logical_type_handle *out_type,
                                                         duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_type) {
			throw duckdb::InvalidInputException("Output type pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (!signature.HasVarArgs()) {
			throw duckdb::InvalidInputException("Signature has no variadic tail.");
		}
		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(new duckdb::LogicalType(signature.GetVarArgs()));
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_has_return_type(duckdb_v2_function_signature_handle sig, bool *out,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out) {
			throw duckdb::InvalidInputException("Output pointer cannot be null.");
		}
		*out = duckdb::ToFunctionSignature(sig)->GetReturnType().id() != duckdb::LogicalTypeId::INVALID;
	});
}

DUCKDB_V2_ERROR duckdb_v2_function_signature_get_return_type(duckdb_v2_function_signature_handle sig,
                                                             duckdb_v2_logical_type_handle *out_type,
                                                             duckdb_v2_error_info_handle *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!sig) {
			throw duckdb::InvalidInputException("Signature pointer cannot be null.");
		}
		if (!out_type) {
			throw duckdb::InvalidInputException("Output type pointer cannot be null.");
		}
		auto &signature = *duckdb::ToFunctionSignature(sig);
		if (signature.GetReturnType().id() == duckdb::LogicalTypeId::INVALID) {
			throw duckdb::InvalidInputException("Signature has no return type.");
		}
		*out_type = reinterpret_cast<duckdb_v2_logical_type_handle>(new duckdb::LogicalType(signature.GetReturnType()));
	});
}
