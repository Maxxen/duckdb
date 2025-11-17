#pragma once

namespace duckdb {

class ServiceFactoryInfo {
public:
	virtual ~ServiceFactoryInfo() = default;

	template <class T>
	T &Cast() {
		DynamicCastCheck<T>(this);
		return reinterpret_cast<T &>(*this);
	}

	template <class T>
	const T &Cast() const {
		DynamicCastCheck<T>(this);
		return reinterpret_cast<const T &>(*this);
	}
};

class ServiceFactory {
public:
	virtual ~ServiceFactory() = default;
	virtual shared_ptr<Service> Create(const ServiceProvider &provider) = 0;
};

} // namespace duckdb
