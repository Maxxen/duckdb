#pragma once

#include "duckdb/common/helper.hpp"

namespace duckdb {

class Service {
public:
	virtual const char *GetServiceKey() const = 0;
	virtual ~Service() = default;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
