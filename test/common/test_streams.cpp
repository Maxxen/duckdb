#include "catch.hpp"
#include "duckdb/common/streams/memory_stream.hpp"

#include <string.h>

namespace duckdb {

#define NUM_INTS 10

TEST_CASE("Memory stream basics", "[streams]") {
    MemoryStream stream(10);

    REQUIRE(stream.Position() == 0);
    REQUIRE(stream.Length() == 0);
    REQUIRE(stream.Capacity() == 10);

    stream.Write(const_data_ptr_cast("Hello"), 5);
    REQUIRE(stream.Position() == 5);

    stream.Rewind();
    REQUIRE(stream.Position() == 0);

    char buffer[10];
    stream.Read(data_ptr_cast(buffer), 5);

    REQUIRE(strncmp(buffer, "Hello", 5) == 0);
    REQUIRE(stream.Position() == 5);

    stream.Write(const_data_ptr_cast("World"), 5);
    REQUIRE(stream.Position() == 10);

    stream.Rewind();
    REQUIRE(stream.Position() == 0);

    stream.Read(data_ptr_cast(buffer), 10);
    REQUIRE(strncmp(buffer, "HelloWorld", 10) == 0);
    
    // Try seeking out of bounds
    REQUIRE_THROWS(stream.Seek(SeekOrigin::START, 11));

    // Try seeking past the end
    REQUIRE_THROWS(stream.Seek(SeekOrigin::END, 1));

    // Try seeking from the end
    stream.Seek(SeekOrigin::END, -10);
    REQUIRE(stream.Position() == 0);

    // Try seeking from the current position
    stream.Seek(SeekOrigin::CURRENT, 5);
    REQUIRE(stream.Position() == 5);
}

TEST_CASE("Memory stream growable", "[streams]") {
    MemoryStream stream(5);

    REQUIRE(stream.Position() == 0);
    REQUIRE(stream.Length() == 0);
    REQUIRE(stream.Capacity() == 5);

    stream.Write(const_data_ptr_cast("Hello"), 5);
    REQUIRE(stream.Position() == 5);

    stream.Write(const_data_ptr_cast("World"), 5);
    REQUIRE(stream.Position() == 10);
    REQUIRE(stream.Length() == 10);

    stream.Rewind();
    REQUIRE(stream.Position() == 0);
    REQUIRE(stream.Length() == 10);

    char buffer[20];
    stream.Read(data_ptr_cast(buffer), 10);

    REQUIRE(strncmp(buffer, "HelloWorld", 10) == 0);
    REQUIRE(stream.Position() == 10);
    REQUIRE(stream.Length() == 10);

    stream.Write(const_data_ptr_cast("HelloWorld"), 10);
    REQUIRE(stream.Position() == 20);
    REQUIRE(stream.Length() == 20);

    stream.Rewind();
    REQUIRE(stream.Position() == 0);
    REQUIRE(stream.Length() == 20);

    stream.Read(data_ptr_cast(buffer), 20);
    REQUIRE(strncmp(buffer, "HelloWorldHelloWorld", 20) == 0);
}

TEST_CASE("Memory stream fixed", "[streams]") {

    char stream_buffer[5];

    { 
        MemoryStream stream(data_ptr_cast(stream_buffer), 5);

        REQUIRE(stream.Position() == 0);
        REQUIRE(stream.Capacity() == 5);
        REQUIRE(stream.Length() == 0);

        stream.Write(const_data_ptr_cast("Hello"), 5);
        REQUIRE(stream.Position() == 5);

        // We should throw here since we cant resize a non-owned buffer
        REQUIRE_THROWS(stream.Write(const_data_ptr_cast("World"), 5));

        stream.Rewind();
        REQUIRE(stream.Position() == 0);
    }

    // Ensure we dont accidentally free the buffer when the stream go out of scope
    REQUIRE(strncmp(stream_buffer, "Hello", 5) == 0);
}

}