#include "capi_v2_internal.hpp"

DUCKDB_V2_API_CALL_t duckdb_v2_file_system_get_from_connection(duckdb_v2_connection_ptr connection,
                                                               duckdb_v2_file_system_ptr *out_file_system,
                                                               duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_file_system) {
			throw duckdb::InvalidInputException("Output file system pointer cannot be null.");
		}
		*out_file_system = nullptr; // Ensure output is null on subsequent errors

		if (!connection) {
			throw duckdb::InvalidInputException("Connection pointer cannot be null.");
		}

		// Return a reference to the context's file system.
		auto &ctx = *static_cast<duckdb::ConnectionWrapperV2 *>(connection)->connection->context;
		*out_file_system = static_cast<duckdb_v2_file_system_ptr>(&duckdb::FileSystem::GetFileSystem(ctx));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_system_get_from_context(duckdb_v2_context_ptr context,
                                                            duckdb_v2_file_system_ptr *out_file_system,
                                                            duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_file_system) {
			throw duckdb::InvalidInputException("Output file system pointer cannot be null.");
		}
		*out_file_system = nullptr; // Ensure output is null on subsequent errors

		if (!context) {
			throw duckdb::InvalidInputException("Context pointer cannot be null.");
		}

		// Return a reference to the context's file system.
		auto &ctx = *static_cast<duckdb::ClientContext *>(context);
		*out_file_system = static_cast<duckdb_v2_file_system_ptr>(&duckdb::FileSystem::GetFileSystem(ctx));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_system_open(duckdb_v2_file_system_ptr file_system, const char *file_path,
                                                uint64_t file_flags, duckdb_v2_file_handle_ptr *out_file_handle,
                                                duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!out_file_handle) {
			throw duckdb::InvalidInputException("Output file handle pointer cannot be null.");
		}
		*out_file_handle = nullptr; // Ensure output is null on subsequent errors

		if (!file_system) {
			throw duckdb::InvalidInputException("File system pointer cannot be null.");
		}
		if (!file_path) {
			throw duckdb::InvalidInputException("File path pointer cannot be null.");
		}
		if (!file_flags) {
			throw duckdb::InvalidInputException("File flags cannot be zero.");
		}

		// Inspect and convert flags
		duckdb::FileOpenFlags open_flags;
		if (file_flags & DUCKDB_V2_FILE_FLAG_READ) {
			open_flags |= duckdb::FileOpenFlags::FILE_FLAGS_READ;
		}
		if (file_flags & DUCKDB_V2_FILE_FLAG_WRITE) {
			open_flags |= duckdb::FileOpenFlags::FILE_FLAGS_WRITE;
		}
		if (file_flags & DUCKDB_V2_FILE_FLAG_CREATE) {
			open_flags |= duckdb::FileOpenFlags::FILE_FLAGS_FILE_CREATE;
		}
		if (file_flags & DUCKDB_V2_FILE_FLAG_CREATE_NEW) {
			open_flags |= duckdb::FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
		}
		if (file_flags & DUCKDB_V2_FILE_FLAG_APPEND) {
			open_flags |= duckdb::FileOpenFlags::FILE_FLAGS_APPEND;
		}

		auto &fs = *static_cast<duckdb::FileSystem *>(file_system);

		auto handle = fs.OpenFile(file_path, open_flags);
		if (!handle) {
			throw duckdb::IOException("Failed to open file: %s", file_path);
		}

		*out_file_handle = static_cast<duckdb_v2_file_handle_ptr>(handle.release());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_read(duckdb_v2_file_handle_ptr file_handle, void *buffer,
                                                int64_t buffer_size, int64_t *bytes_read,
                                                duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		if (!buffer) {
			throw duckdb::InvalidInputException("Buffer pointer cannot be null.");
		}
		if (buffer_size < 0) {
			throw duckdb::InvalidInputException("Buffer size cannot be negative.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);

		const auto buffer_size_unsigned = duckdb::UnsafeNumericCast<idx_t>(buffer_size);

		*bytes_read = duckdb::UnsafeNumericCast<int64_t>(handle.Read(buffer, buffer_size_unsigned));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_write(duckdb_v2_file_handle_ptr file_handle, void *buffer,
                                                 int64_t buffer_size, int64_t *bytes_written,
                                                 duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		if (!buffer) {
			throw duckdb::InvalidInputException("Buffer pointer cannot be null.");
		}
		if (buffer_size < 0) {
			throw duckdb::InvalidInputException("Buffer size cannot be negative.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);

		const auto buffer_size_unsigned = duckdb::UnsafeNumericCast<idx_t>(buffer_size);

		*bytes_written = duckdb::UnsafeNumericCast<int64_t>(handle.Write(buffer, buffer_size_unsigned));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_tell(duckdb_v2_file_handle_ptr file_handle, int64_t *position,
                                                duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		if (!position) {
			throw duckdb::InvalidInputException("Position output pointer cannot be null.");
		}
		if (*position < 0) {
			throw duckdb::InvalidInputException("Position cannot be negative.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);
		*position = duckdb::UnsafeNumericCast<int64_t>(handle.SeekPosition());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_size(duckdb_v2_file_handle_ptr file_handle, int64_t *size,
                                                duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		if (!size) {
			throw duckdb::InvalidInputException("Size output pointer cannot be null.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);
		*size = duckdb::UnsafeNumericCast<int64_t>(handle.GetFileSize());
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_seek(duckdb_v2_file_handle_ptr file_handle, int64_t position,
                                                duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		if (position < 0) {
			throw duckdb::InvalidInputException("Position cannot be negative.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);
		handle.Seek(duckdb::UnsafeNumericCast<idx_t>(position));
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_sync(duckdb_v2_file_handle_ptr file_handle, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);
		handle.Sync();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_close(duckdb_v2_file_handle_ptr file_handle, duckdb_v2_error_info_ptr *err) {
	return duckdb::WithErrorHandler(err, [&]() {
		if (!file_handle) {
			throw duckdb::InvalidInputException("File handle pointer cannot be null.");
		}
		auto &handle = *static_cast<duckdb::FileHandle *>(file_handle);
		handle.Close();
	});
}

DUCKDB_V2_API_CALL_t duckdb_v2_file_handle_destroy(duckdb_v2_file_handle_ptr *file_handle) {
	return duckdb::WithErrorHandler(nullptr, [&]() {
		if (file_handle && *file_handle) {
			delete static_cast<duckdb::FileHandle *>(*file_handle);
			*file_handle = nullptr;
		}
	});
}
