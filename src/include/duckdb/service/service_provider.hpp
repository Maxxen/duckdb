#pragma once

#include "duckdb/service/service.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

enum class ServiceScope : uint8_t {
	GLOBAL,     // Scoped to the entire database instance
	CONNECTION, // Scoped to a single connection
	QUERY,      // Scoped to a single query
};

class ServiceProvider {
public:
	virtual ServiceScope GetScope() const = 0;
	virtual shared_ptr<Service> TryGetSharedService(const char *key) const = 0;
	virtual ~ServiceProvider() = default;

public:
	optional_ptr<Service> TryGetService(const char *key) const {
		auto service = TryGetSharedService(key);
		if (!service) {
			return nullptr;
		}
		return service.get();
	}

	Service &GetService(const char *key) const {
		auto service = TryGetService(key);
		if (!service) {
			throw InvalidConfigurationException("Service with key '%s' not found", key);
		}
		return *service;
	}

public:
	template <class T>
	shared_ptr<T> TryGetSharedService() const {
		auto service = TryGetSharedService(T::SERVICE_KEY);
		if (!service) {
			return nullptr;
		}
		return shared_ptr_cast<Service, T>(service);
	}

	template <class T>
	optional_ptr<T> TryGetService() const {
		auto service = TryGetService(T::SERVICE_KEY);
		if (!service) {
			return nullptr;
		}
		return service.get();
	}

	template <class T>
	T &GetService() const {
		return GetService(T::SERVICE_KEY).template Cast<T>();
	}
};

} // namespace duckdb
