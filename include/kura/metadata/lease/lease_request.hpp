#pragma once

#include "kura/metadata/core/request_id.hpp"
#include "kura/metadata/lease/lease_id.hpp"

namespace kura::metadata {

struct LeaseGrantRequest {
    RequestId request_id;
    LeaseId requested_id;
    LeaseDuration ttl;
    LeaseTick tick;
};

struct LeaseKeepAliveRequest {
    LeaseId id;
    FencingToken fencing_token;
    LeaseTick tick;
};

struct LeaseTimeToLiveRequest {
    LeaseId id;
    LeaseTick tick;
};

struct LeaseRevokeRequest {
    RequestId request_id;
    LeaseId id;
    FencingToken fencing_token;
    LeaseTick tick;
};

}  // namespace kura::metadata
