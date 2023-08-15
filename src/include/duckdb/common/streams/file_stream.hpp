#pragma once

#include "duckdb/common/streams/stream_traits.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

#define FILE_BUFFER_SIZE 4096

//------------------------------------------------------------------------------
// File Stream
//------------------------------------------------------------------------------
class FileStream {
private:
    FileSystem &fs;
	string path;
	unsafe_unique_array<data_t> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

public:
    static constexpr uint8_t DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;

    explicit FileStream(FileSystem &fs, const string &path, uint8_t open_flags = DEFAULT_OPEN_FLAGS);

    idx_t Read(data_ptr_t destination, idx_t num_bytes);
    idx_t Write(const_data_ptr_t source, idx_t num_bytes);
    idx_t Seek(SeekOrigin origin, idx_t offset);
    idx_t Position();
    idx_t Length();
    void Flush();

    ~FileStream();

    idx_t GetTotalBytesWritten() const;
    // Flush the buffer to disk and sync the file to ensure writing is completed
    void Sync();
    // Truncate the file to the specified size, must be smaller or equal to the current size
    void Truncate(idx_t size);
    // Rewind the file to the start
    void Rewind() { Seek(SeekOrigin::START, 0); }
};

static_assert(is_write_stream<FileStream>::value, "FileStream should be writable");
static_assert(is_read_stream<FileStream>::value, "FileStream should be readable");
static_assert(is_seek_stream<FileStream>::value, "FileStream should be seekable");


}