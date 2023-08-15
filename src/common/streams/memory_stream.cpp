#include "duckdb/common/streams/memory_stream.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

//------------------------------------------------------------------------------
// MemoryStream
//------------------------------------------------------------------------------

MemoryStream::MemoryStream(data_ptr_t buffer_p, idx_t capacity_p) 
    : buffer(buffer_p), capacity(capacity_p), length(0), position(0), is_owning(false) {
}

MemoryStream::MemoryStream(idx_t capacity_p) 
    : buffer(new data_t[capacity_p]), capacity(capacity_p), length(0), position(0), is_owning(true) {
}

void MemoryStream::TryResize(idx_t new_capacity) {
    if(!is_owning) {
        throw SerializationException("Cannot resize non-owning MemoryStream");
    }
    data_ptr_t new_buffer = new data_t[new_capacity];
    memcpy(new_buffer, buffer, capacity);
    delete[] buffer;
    buffer = new_buffer;
    capacity = new_capacity;
}

idx_t MemoryStream::Read(data_ptr_t destination, idx_t num_bytes) {
    idx_t bytes_to_read = std::min(num_bytes, length - position);
    memcpy(destination, buffer + position, bytes_to_read);
    position += bytes_to_read;
    return bytes_to_read;
}

idx_t MemoryStream::Write(const_data_ptr_t source, idx_t num_bytes) {
    if(position + num_bytes > capacity) {
        TryResize(std::max(capacity * 2, position + num_bytes));
    }

    memcpy(buffer + position, source, num_bytes);
    position += num_bytes;
    length = std::max(length, position);
    return num_bytes;
}

idx_t MemoryStream::Seek(SeekOrigin origin, int64_t offset) {
    int64_t new_position = 0;
    switch (origin) {
        case SeekOrigin::START:
            new_position = offset;
            break;
        case SeekOrigin::CURRENT:
            new_position = position + offset;
            break;
        case SeekOrigin::END:
            new_position = length + offset;
            break;
    }
    if(new_position < 0) {
        throw SerializationException("Seeking before start of MemoryStream");
    }
    idx_t new_position_unsigned = static_cast<idx_t>(new_position);
    if(new_position_unsigned > length) {
        throw SerializationException("Seeking past end of MemoryStream");
    }

    position = new_position_unsigned;

    return position;
}

idx_t MemoryStream::Position() { return position; }

idx_t MemoryStream::Length() { return length; }

void MemoryStream::Rewind() { position = 0; }

void MemoryStream::SetLength(idx_t new_length) { 
    if (new_length > length) {
        if(new_length > capacity) {
            TryResize(new_length);
        }
        length = new_length;
    }
    else {
        // Truncate, but make sure to move the position if it's out of bounds
        if(position > new_length) {
            position = new_length;
        }
        length = new_length;
    }
}

void MemoryStream::Flush() {}

MemoryStream::~MemoryStream() {
    if (is_owning) {
        delete[] buffer;
    }
}

bool MemoryStream::IsOwning() { return is_owning; }
data_ptr_t MemoryStream::Buffer() { return buffer; }
idx_t MemoryStream::Capacity() { return capacity; }

}