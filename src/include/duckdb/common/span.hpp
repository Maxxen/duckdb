#pragma once

#include <cstddef> // size_t
#include <type_traits>

namespace duckdb {

// NOLINTBEGIN(readability-identifier-naming): mimic std casing

template <class T>
class span {

	// asserts
	static_assert(!std::is_abstract<T>::value, "T must not be an abstract class");
	static_assert(std::is_object<T>::value, "T must be an object");

	// typedefs for std compatability
	using element_type = T;
	using value_type = typename std::remove_cv<T>::type;
	using size_type = std::size_t;
	using difference_type = std::ptrdiff_t;
	using pointer = T *;
	using const_pointer = const T *;
	using reference = T &;
	using const_reference = const T &;
	using iterator = pointer;
	using reverse_iterator = std::reverse_iterator<iterator>;

	pointer ptr;
	size_type count;

public:
	constexpr span(pointer ptr_p, size_type count_p) : ptr(ptr_p), count(count_p) {
	}
	constexpr span(pointer begin, pointer end) : ptr(begin), count(static_cast<size_type>(end - begin)) {
	}

	// Obeservers
	//! returns the number of elements
	constexpr size_type size() const noexcept {
		return count;
	}
	//! returns the size of the sequence in bytes
	constexpr size_type size_bytes() const noexcept {
		return size() * sizeof(T);
	}
	//! checks if the sequence is empty
	constexpr bool empty() const noexcept {
		return size() == 0;
	}

	// Iterators
	//! returns an iterator to the beginning
	constexpr pointer begin() const noexcept {
		return data();
	}
	//! returns an iterator to the end
	constexpr pointer end() const noexcept {
		return data() + size();
	}
	//! returns a reverse iterator to the beginning
	constexpr reverse_iterator rbegin() const noexcept {
		return reverse_iterator(end());
	}
	//! returns a reverse iterator to the end
	constexpr reverse_iterator rend() const noexcept {
		return reverse_iterator(begin());
	}

	// Element Access
	//! direct access to the underlying contiguous storage
	constexpr pointer data() const noexcept {
		return ptr;
	}
	//! access the first element
	reference front() const {
		D_ASSERT(!empty());
		return *data();
	}
	//! access the last element
	reference back() const {
		D_ASSERT(!empty());
		return *(data() + (size() - 1));
	}
	//! access specified element
	reference operator[](size_type idx) const {
		D_ASSERT(idx < size());
		return *(data() + idx);
	}

	// Subviews
	//! obtains a subspan consisting of the first N elements of the sequence
	span first(size_type count_p) const {
		D_ASSERT(count_p <= size());
		return {data(), count_p};
	}
	//! obtains a subspan consisting of the last N elements of the sequence
	span last(size_type count_p) const {
		D_ASSERT(count_p <= size());
		return {data() + (size() - count_p), count_p};
	}
	//! obtains a subspan
	span subspan(size_type offset, size_type count_p) const {
		D_ASSERT(offset + count_p <= size());
		return {data() + offset, count_p};
	}
};

//! construct a span from a pointer and a size
template <class T>
span<T> make_span(T *ptr, size_t count) {
	return span<T>(ptr, count);
}

//! construct a span from two pointers
template <class T>
span<T> make_span(T *begin, T *end) {
	D_ASSERT(begin <= end);
	return span<T>(begin, end);
}

namespace detail {

// trait helpers to determine if a type is a span
template <typename>
struct is_span : std::false_type {};

template <typename T>
struct is_span<span<T>> : std::true_type {
	typedef T ELEMENT_TYPE;
};

} // namespace detail

// NOLINTEND(readability-identifier-naming)

} // namespace duckdb
