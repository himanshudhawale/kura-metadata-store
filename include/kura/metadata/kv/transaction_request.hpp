#pragma once

#include "kura/metadata/core/request_id.hpp"
#include "kura/metadata/kv/compare.hpp"
#include "kura/metadata/kv/request_operation.hpp"
#include "kura/metadata/lease/lease_ownership.hpp"

#include <vector>

namespace kura::metadata {

struct TransactionRequest {
    RequestId request_id;
    std::vector<Compare> comparisons;
    std::vector<LeaseOwnership> lease_ownership;
    LeaseTick lease_tick;
    std::vector<RequestOperation> success;
    std::vector<RequestOperation> failure;
};

}  // namespace kura::metadata
