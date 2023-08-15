#include "duckdb/common/streams/file_stream.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>

namespace duckdb {

//------------------------------------------------------------------------------
// FileStream
//------------------------------------------------------------------------------

FileStream::FileStream(FileSystem &fs_p, const string &path_p, uint8_t open_flags) : 
    fs(fs_p), 
    path(path_p), 
    data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), 
    offset(0), 
    total_written(0) {
	handle = fs.OpenFile(path, open_flags, FileLockType::WRITE_LOCK);
}

idx_t FileStream::Read(data_ptr_t destination, idx_t num_bytes) {
    // first copy anything we can from the buffer
    /*
	data_ptr_t end_ptr = destination + num_bytes;
	while (true) {
		idx_t to_read = MinValue<idx_t>(end_ptr - destination, read_data - offset);
		if (to_read > 0) {
			memcpy(destination, data.get() + offset, to_read);
			offset += to_read;
			destination += to_read;
		}
		if (destination < end_ptr) {
			D_ASSERT(offset == read_data);
			total_read += read_data;
			// did not finish reading yet but exhausted buffer
			// read data into buffer
			offset = 0;
			read_data = fs.Read(*handle, data.get(), FILE_BUFFER_SIZE);
			if (read_data == 0) {
				throw SerializationException("not enough data in file to deserialize result");
			}
		} else {
			return;
		}
	}
    */

   throw NotImplementedException("FileStream::Read");
}

idx_t FileStream::Write(const_data_ptr_t source, idx_t num_bytes) {
    // first copy anything we can from the buffer
	const_data_ptr_t end_ptr = source + num_bytes;
	while (source < end_ptr) {
		idx_t to_write = MinValue<idx_t>((end_ptr - source), FILE_BUFFER_SIZE - offset);
		D_ASSERT(to_write > 0);
		memcpy(data.get() + offset, source, to_write);
		offset += to_write;
		source += to_write;
		if (offset == FILE_BUFFER_SIZE) {
			Flush();
		}
	}
    return num_bytes;
}

idx_t FileStream::Seek(SeekOrigin origin, idx_t offset) {
    switch (origin) {
    case SeekOrigin::START:
        if (offset < 0) {
            throw SerializationException("Cannot seek before the beginning of the file!");
        }
        this->offset = offset;
        break;
    case SeekOrigin::CURRENT:
        if (this->offset + offset < 0) {
            throw SerializationException("Cannot seek before the beginning of the file!");
        }
        this->offset += offset;
        break;
    case SeekOrigin::END:
        throw SerializationException("Cannot seek from the end of the file!");
    }
    return this->offset;

    /* 
        D_ASSERT(location <= file_size);
        handle->Seek(location);
        total_read = location;
        read_data = offset = 0;
    */
}

void FileStream::Flush() {
    if (offset == 0) {
		return;
	}
	fs.Write(*handle, data.get(), offset);
	total_written += offset;
	offset = 0;
}

void FileStream::Sync() {
	Flush();
	handle->Sync();
}

idx_t FileStream::Position() {
	return total_written + offset;
}


idx_t FileStream::Length() {
    return fs.GetFileSize(*handle) + offset;
}

idx_t FileStream::GetTotalBytesWritten() const {
    return total_written + offset;
}



}