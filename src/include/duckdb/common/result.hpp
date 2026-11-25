//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

#include <variant>

namespace duckdb {

//! Tag wrapper that marks a value as the error of a Result
template <class E>
struct Err {
	explicit Err(E error_p) : error(std::move(error_p)) {
	}
	E error;
};

//! Holds either a value of type T or an error of type E
template <class T, class E>
class Result {
public:
	Result(T &&value) : data(std::in_place_index<0>, std::move(value)) { // NOLINT: allow implicit conversion
	}
	Result(const T &value) : data(std::in_place_index<0>, value) { // NOLINT: allow implicit conversion
	}
	Result(Err<E> err) : data(std::in_place_index<1>, std::move(err.error)) { // NOLINT: allow implicit conversion
	}

public:
	bool IsOk() const {
		return data.index() == 0;
	}
	bool IsError() const {
		return data.index() == 1;
	}
	explicit operator bool() const {
		return IsOk();
	}

	T &GetValue() {
		if (!IsOk()) {
			throw InternalException("Result::GetValue called on a Result that holds an error");
		}
		return *std::get_if<0>(&data);
	}
	const T &GetValue() const {
		if (!IsOk()) {
			throw InternalException("Result::GetValue called on a Result that holds an error");
		}
		return *std::get_if<0>(&data);
	}
	E &GetError() {
		if (!IsError()) {
			throw InternalException("Result::GetError called on a Result that holds a value");
		}
		return *std::get_if<1>(&data);
	}
	const E &GetError() const {
		if (!IsError()) {
			throw InternalException("Result::GetError called on a Result that holds a value");
		}
		return *std::get_if<1>(&data);
	}

	T &operator*() {
		return GetValue();
	}
	const T &operator*() const {
		return GetValue();
	}
	T *operator->() {
		return &GetValue();
	}
	const T *operator->() const {
		return &GetValue();
	}

private:
	std::variant<T, E> data;
};

} // namespace duckdb
