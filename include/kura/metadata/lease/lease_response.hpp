#pragma once

#include "kura/metadata/kv/operation_result.hpp"
#include "kura/metadata/lease/lease_record.hpp"

#include <cstdint>
#include <optional>
#include <vector>

namespace kura::metadata {

enum class LeaseResultCode {
    ok,
    not_found,
    expired,
    fencing_token_mismatch
};

struct LeaseGrantResult {
    LeaseRecord lease;
    std::int64_t revision{};
};

struct LeaseLookupResult {
    LeaseResultCode code{LeaseResultCode::ok};
    std::optional<LeaseRecord> lease;
    LeaseDuration remaining_ttl;
    std::int64_t revision{};
};

struct LeaseCleanupResult {
    LeaseResultCode code{LeaseResultCode::ok};
    std::vector<LeaseId> leases;
    std::vector<KeyValue> deleted_keys;
    std::int64_t revision{};
};

}  // namespace kura::metadata
