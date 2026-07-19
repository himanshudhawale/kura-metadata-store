#pragma once

#include "kura/metadata/core/request_id.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <chrono>

namespace kura::metadata {

struct LeaseGrantRequest {
    RequestId request_id;
    LeaseId requested_id;
    std::chrono::seconds ttl;
};

struct LeaseKeepAliveRequest {
    LeaseId id;
};

struct LeaseRevokeRequest {
    RequestId request_id;
    LeaseId id;
};

}  // namespace kura::metadata
