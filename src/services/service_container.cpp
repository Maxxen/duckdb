#include "duckdb/services/service_container.hpp"
#include "duckdb/services/service_factory.hpp"

namespace duckdb {

namespace {
class CallbackServiceFactory final : public ServiceFactory {
public:
	CallbackServiceFactory(service_factory_callback_t callback) : callback(callback) {
	}
	shared_ptr<Service> Create(const ServiceProvider &provider) override {
		return callback(provider);
	}

private:
	service_factory_callback_t callback;
};
} // namespace

class ServiceRegistry {
public:
	shared_ptr<Service> TryConstructService(ServiceScope scope, const string &key,
	                                        const ServiceProvider &provider) const {
		lock_guard<mutex> guard(registry_lock);

		auto scope_entry = factories.find(scope);
		if (scope_entry == factories.end()) {
			// Service cannot be constructed in this scope
			return nullptr;
		}

		const auto &factories = scope_entry->second;
		auto factory_entry = factories.find(key);
		if (factory_entry == factories.end()) {
			// No factory registered for this key in this scope
			return nullptr;
		}
		return factory_entry->second->Create(provider);
	}

	void AddService(ServiceScope scope, const string &key, unique_ptr<ServiceFactory> factory) {
		lock_guard<mutex> guard(registry_lock);
		factories[scope][key] = std::move(factory);
	}

private:
	mutable mutex registry_lock;
	unordered_map<ServiceScope, case_insensitive_map_t<unique_ptr<ServiceFactory>>> factories;
};

ServiceContainer::ServiceContainer(ServiceScope scope, optional_ptr<const ServiceProvider> parent)
    : scope(scope), parent(parent) {
#ifdef DEBUG
	if (!parent) {
		D_ASSERT(scope == ServiceScope::GLOBAL);
	} else {
		// Ensure that the parent scope is larger than the current scope
		switch (scope) {
		case ServiceScope::GLOBAL:
			D_ASSERT(false); // GLOBAL scope cannot have a parent
			break;
		case ServiceScope::CONNECTION:
			D_ASSERT(parent->GetScope() == ServiceScope::GLOBAL);
			break;
		case ServiceScope::QUERY:
			D_ASSERT(parent->GetScope() == ServiceScope::CONNECTION);
			break;
		default:
			D_ASSERT(false); // Unknown scope
			break;
			break;
		}
	}
#endif
	registry = make_shared_ptr<ServiceRegistry>();
}

void ServiceContainer::AddService(ServiceScope scope, const string &key, unique_ptr<ServiceFactory> factory) {
	registry->AddService(scope, key, std::move(factory));
}

void ServiceContainer::AddService(ServiceScope scope, const string &key, service_factory_callback_t factory) {
	registry->AddService(scope, key, make_uniq<CallbackServiceFactory>(factory));
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

	// Else, attempt to construct the service using the registry
	auto service = registry->TryConstructService(scope, key, *this);
	if (service) {
		// Store the constructed service for future use
		services[key] = service;
		return service;
	}

	return nullptr;
}

} // namespace duckdb
