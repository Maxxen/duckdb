#pragma once

#include "duckdb/services/service.hpp"
#include "duckdb/services/service_provider.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class ServiceContainer : public ServiceProvider {
public:
	shared_ptr<Service> TryGetSharedService(const char *key) const override;

	void AddService(shared_ptr<Service> service);

	template <class T, class... ARGS>
	void AddService(ARGS &&... args) {
		AddService(make_shared_ptr<T>(std::forward<ARGS>(args)...));
	}

	template <class BASE, class DERIVED, class... ARGS>
	void AddServiceBase(ARGS &&... args) {
		AddService(shared_ptr<BASE>(new DERIVED(std::forward<ARGS>(args)...)));
	}

private:
	mutable mutex service_lock; // FIXME: Replace with a reader-writer lock

	optional_ptr<ServiceProvider> parent;
	case_insensitive_map_t<shared_ptr<Service>> services;
};

} // namespace duckdb
