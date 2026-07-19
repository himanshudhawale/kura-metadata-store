#pragma once

#include "kura/metadata/raft/node_id.hpp"

#include <vector>

namespace kura::metadata {

struct ClusterMembership {
    std::vector<NodeId> voters;
    std::vector<NodeId> learners;

    bool operator==(const ClusterMembership&) const = default;
};

}  // namespace kura::metadata
