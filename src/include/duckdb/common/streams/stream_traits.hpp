#pragma once

#include "duckdb/common/typedefs.hpp"
#include <type_traits>

namespace duckdb {

enum class SeekOrigin {
    START,
    CURRENT,
    END
};

// Read Stream Trait
template<typename T, typename = T>
// NOLINTNEXTLINE
struct is_read_stream : std::false_type { };

template<typename T>
struct is_read_stream<
    T, typename std::enable_if<
        std::is_same<decltype(std::declval<T>().Read(static_cast<data_ptr_t>(nullptr), 0)), idx_t>::value,
        T>::type> : std::true_type {};

// Write Stream Trait
template<typename T, typename = T>
// NOLINTNEXTLINE
struct is_write_stream : std::false_type { };

template<typename T>
struct is_write_stream<
    T, typename std::enable_if<
        std::is_same<decltype(std::declval<T>().Write(static_cast<const_data_ptr_t>(nullptr), 0)), idx_t>::value,
        T>::type> : std::true_type {};

// Seekable Stream Trait
template<typename T, typename = T>
// NOLINTNEXTLINE
struct is_seek_stream : std::false_type { };

template<typename T>
struct is_seek_stream<
    T, typename std::enable_if<
        std::is_same<decltype(std::declval<T>().Seek(SeekOrigin::START, static_cast<int64_t>(0))), idx_t>::value && 
        std::is_same<decltype(std::declval<T>().Position()), idx_t>::value &&
        std::is_same<decltype(std::declval<T>().Length()), idx_t>::value &&
        std::is_same<decltype(std::declval<T>().Rewind()), void>::value,
        T>::type> : std::true_type {};

} // namespace duckdb