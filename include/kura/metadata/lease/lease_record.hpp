#pragma once

#include "kura/metadata/byte_sequence.hpp"
#include "kura/metadata/lease/lease_id.hpp"

#include <vector>

namespace kura::metadata {

struct LeaseRecord {
    LeaseId id;
    FencingToken fencing_token;
    LeaseDuration granted_ttl;
    LeaseTick expiry_tick;
    std::vector<ByteSequence> attached_keys;

    bool operator==(const LeaseRecord&) const = default;
};

}  // namespace kura::metadata
