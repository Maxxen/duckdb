#include "duckdb/common/zip_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "miniz.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Zip Utilities
//------------------------------------------------------------------------------

// Split a zip path into the path to the archive and the path within the archive
static pair<string, string> SplitArchivePath(const string &path) {
	const string suffix = ".zip";

	const auto zip_path = std::search(path.begin(), path.end(), suffix.begin(), suffix.end());

	if (zip_path == path.end()) {
		throw IOException("Could not find a '.zip' archive to open in: '%s'", path);
	}

	const auto suffix_path = zip_path + UnsafeNumericCast<int64_t>(suffix.size());
	if (suffix_path == path.end()) {
		return {path, ""};
	}

	if (*suffix_path == '/') {
		// If there is a slash after the last .zip, we need to remove everything after that
		auto archive_path = string(path.begin(), suffix_path);
		auto file_path = string(suffix_path + 1, path.end());
		return {archive_path, file_path};
	}

	// Else, this is not a raw .zip, e.g. .zippy or .zipperino
	throw IOException("Could not find a '.zip' archive to open in: '%s'.", path);
}

//------------------------------------------------------------------------------
// Zip File Handle
//------------------------------------------------------------------------------

class ZipFileHandle final : public FileHandle {
	friend class ZipFileSystem;

public:
	ZipFileHandle(FileSystem &file_system, const string &path, unique_ptr<FileHandle> archive_handle_p)
		: FileHandle(file_system, path), inner_handle(std::move(archive_handle_p)) {
	}

	void Close() override;
private:
	unique_ptr<FileHandle> inner_handle;
	idx_t start_offset;
	idx_t end_offset;
};

void ZipFileHandle::Close() {
	inner_handle->Close();
}

//------------------------------------------------------------------------------
// Zip File System
//------------------------------------------------------------------------------

bool ZipFileSystem::CanHandleFile(const string &fpath) {
	// TODO: Check that we can seek into the file
	return fpath.size() > 6 && fpath.substr(0, 6) == "zip://";
}

unique_ptr<FileHandle> ZipFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                               optional_ptr<FileOpener> opener) {

	// Get the path to the zip file
	const auto paths = SplitArchivePath(path.substr(6));
	const auto &zip_path = paths.first;
	const auto &file_path = paths.second;

	// Now we need to find the file within the zip file and return out file handle
	auto &fs = FileSystem::GetFileSystem(*opener->TryGetClientContext());
	auto handle = fs.OpenFile(zip_path, flags);

	if (file_path.empty()) {
		return handle;
	}

	throw IOException("Failed to find file: %s", file_path);
}

int64_t ZipFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	// Dont read past the end of the file
	auto position = zip_handle.inner_handle->SeekPosition();
	if (position >= zip_handle.end_offset) {
		return 0;
	}
	auto remaining_bytes = zip_handle.end_offset - position;
	auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
	return zip_handle.inner_handle->Read(buffer, to_read);
}

int64_t ZipFileSystem::GetFileSize(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	return UnsafeNumericCast<int64_t>(zip_handle.end_offset - zip_handle.start_offset);
}

void ZipFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	zip_handle.inner_handle->Seek(zip_handle.start_offset + location);
}

void ZipFileSystem::Reset(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	zip_handle.inner_handle->Reset();
	zip_handle.inner_handle->Seek(zip_handle.start_offset);
}

idx_t ZipFileSystem::SeekPosition(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	return zip_handle.inner_handle->SeekPosition() - zip_handle.start_offset;
}

bool ZipFileSystem::CanSeek() {
	return true;
}

time_t ZipFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	auto &inner_handle = *zip_handle.inner_handle;
	return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType ZipFileSystem::GetFileType(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	auto &inner_handle = *zip_handle.inner_handle;
	return inner_handle.file_system.GetFileType(inner_handle);
}

bool ZipFileSystem::OnDiskFile(FileHandle &handle) {
	auto &zip_handle = handle.Cast<ZipFileHandle>();
	return zip_handle.inner_handle->OnDiskFile();
}

vector<string> ZipFileSystem::Glob(const string &path, FileOpener *opener) {
	// Remove the "zip://" prefix
	const auto parts = SplitArchivePath(path.substr(6));
	auto &zip_path = parts.first;
	auto &file_path = parts.second;

	if (HasGlob(zip_path)) {
		throw NotImplementedException("Cannot glob multiple zip files");
	}

	if (!HasGlob(file_path)) {
		// No glob pattern in the file path, just return the file path
		return {path};
	}

	const auto pattern_parts = StringUtil::Split(file_path, '/');
	for (auto &part : pattern_parts) {
		if (part == "zip:" || StringUtil::EndsWith(part, ".zip")) {
			// We can not glob into nested zip files
			throw NotImplementedException("Globbing into nested zip files is not supported");
		}
	}

	// Given the path to the zip file, open it
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	if(!context) {
		throw IOException("Cannot glob zip archives without a client context");
	}
	auto &fs = FileSystem::GetFileSystem(*context);

	auto archive_handle = fs.OpenFile(zip_path, FileFlags::FILE_FLAGS_READ);
	if (!archive_handle) {
		throw IOException("Failed to open file: %s", zip_path);
	}

	duckdb_miniz::mz_zip_archive zip_archive;
	duckdb_miniz::mz_zip_zero_struct(&zip_archive);

	// Pass the userdata oparquet pointer to the file handle
	zip_archive.m_pIO_opaque = archive_handle.get();

	// Setup the read callback
	zip_archive.m_pRead = [](void *user_data, duckdb_miniz::mz_uint64 file_offset, void *buffer, size_t n) {
		const auto handle = static_cast<FileHandle *>(user_data);
		handle->Seek(file_offset);
		const auto bytes = handle->Read(buffer, n);
		return static_cast<size_t>(bytes);
	};

	// Initialize the archive
	if (!mz_zip_reader_init(&zip_archive, archive_handle->GetFileSize(), duckdb_miniz::MZ_ZIP_FLAG_COMPRESSED_DATA)) {
		const auto error = mz_zip_get_last_error(&zip_archive);
		const auto error_str = mz_zip_get_error_string(error);
		duckdb_miniz::mz_zip_reader_end(&zip_archive);
		throw IOException("Failed to initialize zip archive: %s", error_str);
	}

	// Now read the zip archive and find the files that match the pattern
	vector<string> result;
	const auto file_count = duckdb_miniz::mz_zip_reader_get_num_files(&zip_archive);
	for(duckdb_miniz::mz_uint i = 0; i < file_count; i++) {
		duckdb_miniz::mz_zip_archive_file_stat file_stat;
		const auto ok = duckdb_miniz::mz_zip_reader_file_stat(&zip_archive, i, &file_stat);
		if(!ok) {
			duckdb_miniz::mz_zip_reader_end(&zip_archive);
			throw IOException("Failed to read file stat from zip archive");
		}

		// Dont add directories and unsupported entries (like encrypted files) to the result
		if(!file_stat.m_is_directory && file_stat.m_is_supported) {
			auto entry_path = "zip://" + zip_path + "/" + file_stat.m_filename;
			result.push_back(std::move(entry_path));
		}
	}

	duckdb_miniz::mz_zip_reader_end(&zip_archive);
	return result;

	/*

	optional_ptr<ObjectCache> cache;
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	if (context) {
		if (ObjectCache::ObjectCacheEnabled(*context)) {
			cache = ObjectCache::GetObjectCache(*context);
		}
	}

	// Given the path to the zip file, open it
	auto &fs = FileSystem::GetFileSystem(*context);;

	auto archive_handle = fs.OpenFile(zip_path, FileFlags::FILE_FLAGS_READ);
	if (!archive_handle) {
		throw IOException("Failed to open file: %s", zip_path);
	}

	auto last_modified = archive_handle->file_system.GetLastModifiedTime(*archive_handle);
	const auto is_uncompressed = archive_handle->GetFileCompressionType() == FileCompressionType::UNCOMPRESSED;

	vector<string> result;
	for (auto &entry : TarBlockIterator::Scan(*archive_handle)) {

		auto entry_name = entry.file_name;
		auto entry_parts = StringUtil::Split(entry_name, '/');

		if (entry_parts.size() < pattern_parts.size()) {
			// This entry is not deep enough to match the pattern
			continue;
		}

		// Check if the pattern matches the entry
		bool match = true;
		for (idx_t i = 0; i < pattern_parts.size(); i++) {
			const auto &pp = pattern_parts[i];
			const auto &ep = entry_parts[i];

			if (pp == "**") {
				// We only allow crawl's to be at the end of the pattern
				if (i != pattern_parts.size() - 1) {
					throw NotImplementedException(
					    "Recursive globs are only supported at the end of zip file path patterns");
				}
				// Otherwise, everything else is a match
				match = true;
				break;
			}

			if (!LikeFun::Glob(ep.c_str(), ep.size(), pp.c_str(), pp.size())) {
				// Not a match
				match = false;
				break;
			}

			if (i == pattern_parts.size() - 1 && entry_parts.size() > pattern_parts.size()) {
				// If the entry is deeper than the pattern (and we havent hit a **), then it is not a match
				match = false;
				break;
			}
		}

		if (match) {
			auto entry_path = "zip://" + zip_path + "/" + entry_name;
			// Cache the offset and size for this file (if it is uncompressed)
			if (cache && is_uncompressed) {
				auto offset = entry.file_offset;
				auto size = entry.header->GetFileSize();
				auto cache_entry = make_shared_ptr<TarArchiveFileMetadataCache>(last_modified, offset, size);
				cache->Put(entry_path, std::move(cache_entry));
			}
			result.push_back(entry_path);
		}
	}

	return result;
	*/
}

} // namespace duckdb