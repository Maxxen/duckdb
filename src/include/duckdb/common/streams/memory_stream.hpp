#pragma once

#include "duckdb/common/streams/stream_traits.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Memory Stream
//------------------------------------------------------------------------------
class MemoryStream {
private:
    data_ptr_t buffer;
    idx_t capacity;
    idx_t length;
    idx_t position;
    bool is_owning;

    // Throws if the stream is not resizeable
    void TryResize(idx_t new_capacity);

public:
    // Create a non-owning MemoryStream over the specified buffer
    explicit MemoryStream(data_ptr_t buffer, idx_t capacity);

    // Create a resizeable owning MemoryStream with the specified initial capacity
    explicit MemoryStream(idx_t capacity = 4096);

    ~MemoryStream();

    // Returns true if the backing buffer is owned by this stream (and thus resizable)
    bool IsOwning();

    // Returns a pointer to the backing buffer
    data_ptr_t Buffer();

    // Returns the current size of the backing buffer
    idx_t Capacity();

public:
    // Read from the the stream into the specified buffer
    // This will never throw, but might not read all the bytes requested if the 
    // end of the stream is reached (position >= length)
    idx_t Read(data_ptr_t destination, idx_t num_bytes);

    // Write the specified buffer into the stream
    // This can throw if the stream is not resizeable and the write would exceed the capacity,
    // otherwise the backing buffer will be resized as necessary
    idx_t Write(const_data_ptr_t source, idx_t num_bytes);

    // Seek to the specified position
    // This can throw if the position is outside the bounds of the stream
    idx_t Seek(SeekOrigin origin, int64_t offset);

    // Rewinds the stream to the beginning
    void Rewind();    

    // Returns the current position in the stream
    idx_t Position();

    // Returns the length of the stream
    idx_t Length();
    
    // Sets the length of the stream. 
    // If the new length is larger than the current capacity, and the backing buffer is resizeable, 
    // the buffer will be resized and the memory between the capacity and new length left undefined.
    // If the new length is larger than the current capacity, and the backing buffer is not resizeable, an exception will be thrown
    // If the new length is smaller than the current length, sets the new length and moves the position to the end of the new length if necessary
    void SetLength(idx_t new_length);

    // Flushes the stream
    void Flush();
};

static_assert(is_write_stream<MemoryStream>::value, "MemoryStream should be writable");
static_assert(is_read_stream<MemoryStream>::value, "MemoryStream should be readable");
static_assert(is_seek_stream<MemoryStream>::value, "MemoryStream should be seekable");

}