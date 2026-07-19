#pragma once

#include "kura/metadata/lease/lease_id.hpp"

#include <chrono>

namespace kura::metadata {

struct LeaseRecord {
    LeaseId id;
    std::chrono::seconds granted_ttl;
    std::chrono::seconds remaining_ttl;
};

}  // namespace kura::metadata
