#include "duckdb_v2.h"
#include "capi_v2_internal.hpp"

//----------------------------------------------------------------------------------------------------------------------
// Log Storage
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
namespace {

class CAPILogStorage final : public LogStorage {
public:
	~CAPILogStorage() override {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}

	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override {
		if (log_callback == nullptr) {
			return;
		}

		InvokeWithErrorSlot<InvalidInputException>([&](duckdb_v2_error_info_handle *err) {
			log_callback(user_data, timestamp.value, static_cast<DUCKDB_V2_LOG_LEVEL>(level), log_type.c_str(),
			             log_message.c_str(), err);
		});
	};

	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override {};

	void Flush(LoggingTargetTable table) override {};

	void FlushAll() override {};

	bool IsEnabled(LoggingTargetTable table) override {
		return true;
	}

	const string GetStorageName() override {
		return name;
	}

public:
	string name;
	duckdb_v2_log_callback_fn log_callback = nullptr;
	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_fn user_data_destructor_cb = nullptr;
};

class LogStorageBuilder {
public:
	string name;
	duckdb_v2_log_callback_fn log_callback = nullptr;
	void *user_data = nullptr;
	duckdb_v2_user_data_destroy_fn user_data_destructor_cb = nullptr;

	~LogStorageBuilder() {
		if (user_data && user_data_destructor_cb) {
			user_data_destructor_cb(user_data);
		}
	}
};

} // namespace
} // namespace duckdb

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_create(duckdb_v2_context_handle context,
                                                          duckdb_v2_log_storage_builder_handle *out_builder,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}
		if (!out_builder) {
			throw duckdb::InvalidInputException("Output builder pointer cannot be null.");
		}
		*out_builder = reinterpret_cast<duckdb_v2_log_storage_builder_handle>(new duckdb::LogStorageBuilder());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_set_name(duckdb_v2_log_storage_builder_handle builder,
                                                            const char *name, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		if (!name) {
			throw duckdb::InvalidInputException("Name pointer cannot be null.");
		}
		if (strlen(name) == 0) {
			throw duckdb::InvalidInputException("Name cannot be empty.");
		}
		reinterpret_cast<duckdb::LogStorageBuilder *>(builder)->name = name;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_set_user_data(duckdb_v2_log_storage_builder_handle builder,
                                                                 void *user_data,
                                                                 duckdb_v2_user_data_destroy_fn destructor,
                                                                 duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &b = *reinterpret_cast<duckdb::LogStorageBuilder *>(builder);
		if (b.user_data && b.user_data_destructor_cb) {
			b.user_data_destructor_cb(b.user_data);
		}

		b.user_data = user_data;
		b.user_data_destructor_cb = destructor;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_set_log_callback(duckdb_v2_log_storage_builder_handle builder,
                                                                    duckdb_v2_log_callback_fn callback,
                                                                    duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}
		reinterpret_cast<duckdb::LogStorageBuilder *>(builder)->log_callback = callback;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_register(duckdb_v2_context_handle context,
                                                            duckdb_v2_log_storage_builder_handle builder,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}

		if (!builder) {
			throw duckdb::InvalidInputException("Builder pointer cannot be null.");
		}

		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);
		auto &b = *reinterpret_cast<duckdb::LogStorageBuilder *>(builder);

		if (b.name.empty()) {
			throw duckdb::InvalidInputException("Log storage name cannot be empty.");
		}
		if (!b.log_callback) {
			throw duckdb::InvalidInputException("Log callback cannot be null.");
		}

		const auto &db = ctx.db;
		const auto log_storage = duckdb::make_shared_ptr<duckdb::CAPILogStorage>();
		duckdb::shared_ptr<duckdb::LogStorage> log_storage_ptr = log_storage;

		const auto success = db->GetLogManager().RegisterLogStorage(b.name, log_storage_ptr);

		if (!success) {
			throw duckdb::InvalidInputException("A log storage with the name '" + b.name + "' is already registered.");
		}

		// Initialize after successful registration to ensure proper cleanup if registration fails
		log_storage->name = b.name;
		log_storage->log_callback = b.log_callback;
		log_storage->user_data = b.user_data;
		log_storage->user_data_destructor_cb = b.user_data_destructor_cb;

		// "Move out" the user data from the builder
		b.user_data = nullptr;
		b.user_data_destructor_cb = nullptr;
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_log_storage_builder_destroy(duckdb_v2_log_storage_builder_handle *builder) {
	return WithErrorHandler(nullptr, [&]() {
		if (builder && *builder) {
			delete reinterpret_cast<duckdb::LogStorageBuilder *>(*builder);
			*builder = nullptr;
		}
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Log emission
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_API_CALL_t duckdb_v2_connection_log(duckdb_v2_connection_handle conn, DUCKDB_V2_LOG_LEVEL level,
                                              const char *message, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		auto &connection = *duckdb::ToConn(conn);
		auto &ctx = *connection.connection->context;
		duckdb::Logger::Get(ctx).WriteLog("", static_cast<duckdb::LogLevel>(level), message);
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_context_log(duckdb_v2_context_handle context, DUCKDB_V2_LOG_LEVEL level,
                                           const char *message, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		auto &ctx = *reinterpret_cast<duckdb::ClientContext *>(context);
		duckdb::Logger::Get(ctx).WriteLog("", static_cast<duckdb::LogLevel>(level), message);
	});
}
