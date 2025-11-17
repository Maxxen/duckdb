#include "duckdb/services/service_container.hpp"

#include <duckdb/storage/storage_lock.hpp>

namespace duckdb {

void ServiceContainer::AddService(shared_ptr<Service> service) {
	// Lock the service container for thread-safe access
	lock_guard<mutex> guard(service_lock);

	// Add the service to the current container
	services[service->GetServiceKey()] = std::move(service);
}

shared_ptr<Service> ServiceContainer::TryGetSharedService(const char *key) const {
	// Lock the service container for thread-safe access
	lock_guard<mutex> guard(service_lock);

	// Check if the service exists in the current container
	auto entry = services.find(key);
	if (entry != services.end()) {
		return entry->second;
	}

	// If not found, check the parent container if it exists
	if (parent) {
		return parent->TryGetSharedService(key);
	}

	return nullptr;
}

} // namespace duckdb
