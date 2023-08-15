#pragma once

#include "duckdb/common/streams/stream_traits.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Base Stream Interface
//------------------------------------------------------------------------------

class Stream {
public:
    Stream(const Stream &other) = delete;
    Stream(Stream &&other) = delete;
    Stream &operator=(const Stream &other) = delete;
    Stream &operator=(Stream &&other) = delete;
    
    virtual bool IsReadable() = 0;
    virtual bool IsWritable() = 0;
    virtual bool IsSeekable() = 0;

    virtual idx_t Read(data_ptr_t destination, idx_t num_bytes) {
        D_ASSERT(!IsReadable());
        throw InternalException("Read not supported for this stream!");
    }

    virtual idx_t Write(data_ptr_t source, idx_t num_bytes) {
        D_ASSERT(!IsWritable());
        throw InternalException("Write not supported for this stream!");
    }

    virtual idx_t Seek(SeekOrigin origin, int64_t offset) {
        D_ASSERT(!IsSeekable());
        throw InternalException("Seek not supported for this stream!");
    }

    void Rewind() {
        Seek(SeekOrigin::START, 0);
    }

    virtual idx_t Position() {
        D_ASSERT(!IsSeekable());
        throw InternalException("Position not supported for this stream!");
    }

    virtual idx_t Length() {
        D_ASSERT(!IsSeekable());
        throw InternalException("Length not supported for this stream!");
    }

    virtual void SetLength(idx_t new_length) {
        D_ASSERT(!IsSeekable() && !IsWritable());
        throw InternalException("SetLength not supported for this stream!");
    }

    virtual void Flush(bool sync) {
        D_ASSERT(!IsWritable());
        throw InternalException("Flush not supported for this stream!");
    }

    virtual ~Stream() {
    }
};

//------------------------------------------------------------------------------
// Memory Stream
//------------------------------------------------------------------------------

class MemoryStream final : public Stream {
private:
    data_ptr_t buffer;
    idx_t capacity;
    idx_t position;
    idx_t length;
    bool is_owning;
public:
    explicit MemoryStream(data_ptr_t buffer, idx_t capacity);
    explicit MemoryStream(idx_t capacity);

    bool IsOwning() {
        return is_owning;
    }

    data_ptr_t GetBuffer() {
        return buffer;
    }

    idx_t GetCapacity() {
        return capacity;
    }

public:

    bool IsReadable() final {
        return true;
    }

    bool IsWritable() final {
        return true;
    }

    bool IsSeekable() final {
        return true;
    }

    idx_t Read(data_ptr_t destination, idx_t num_bytes) final;
    idx_t Write(data_ptr_t source, idx_t num_bytes) final;
    idx_t Seek(SeekOrigin origin, int64_t offset) final;
    idx_t Position() final;
    idx_t Length() final;
    void SetLength(idx_t new_length) final;
    void Flush(bool sync) final;

    ~MemoryStream() final;
};


} // namespace duckdb