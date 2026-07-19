#pragma once

#include "kura/metadata/kv/operation_result.hpp"
#include "kura/metadata/lease/lease_record.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct InMemoryStoreSnapshot {
    std::vector<KeyValue> values;
    std::vector<LeaseRecord> leases;
    std::int64_t revision{};
    LeaseTick logical_tick;
    std::int64_t next_lease_id{1};
    FencingToken next_fencing_token{1};

    bool operator==(const InMemoryStoreSnapshot&) const = default;
};

}  // namespace kura::metadata
