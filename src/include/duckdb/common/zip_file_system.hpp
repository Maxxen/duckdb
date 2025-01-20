#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"

namespace duckdb {


class ZipFileSystem final : public FileSystem {
public:
	explicit ZipFileSystem() : FileSystem() { //parent_file_system(parent_p) {
	}

	time_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	std::string GetName() const override { return "ZipFileSystem"; }
	vector<string> Glob(const string &path, FileOpener *opener) override;

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override;
	bool CanSeek() override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;

private:
	//FileSystem &parent_file_system;
};

} // namespace duckdb