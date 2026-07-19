#pragma once

#include "kura/metadata/lease/lease_id.hpp"

namespace kura::metadata {

struct LeaseOwnership {
    LeaseId id;
    FencingToken fencing_token;

    bool operator==(const LeaseOwnership&) const = default;
};

}  // namespace kura::metadata
