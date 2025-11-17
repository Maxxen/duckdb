#pragma once

#include "duckdb/service/service.hpp"
#include "duckdb/service/service_provider.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class ServiceFactory;
class ServiceFactoryInfo;
class ServiceRegistry;

typedef std::function<shared_ptr<Service>(const ServiceProvider &provider)> service_factory_callback_t;

class ServiceContainer : public ServiceProvider {
public:
	explicit ServiceContainer(ServiceScope scope, optional_ptr<const ServiceProvider> parent = nullptr);

	// Get the scope of this service container
	ServiceScope GetScope() const override {
		return scope;
	}

	// Try to get a shared service by key
	shared_ptr<Service> TryGetSharedService(const char *key) const override;

	// Register a service with a factory callback
	void AddService(ServiceScope scope, const string &key, service_factory_callback_t factory);

	// Register a service with a service factory
	void AddService(ServiceScope scope, const string &key, unique_ptr<ServiceFactory> factory);

private:
	ServiceScope scope;
	optional_ptr<const ServiceProvider> parent;
	shared_ptr<ServiceRegistry> registry;

	mutable mutex service_lock; // FIXME: Replace with a reader-writer lock
	mutable case_insensitive_map_t<shared_ptr<Service>> services;
};

} // namespace duckdb
