#pragma once

#include "kura/metadata/raft/snapshot_metadata.hpp"

#include <cstdint>
#include <vector>

namespace kura::metadata {

struct Snapshot {
    SnapshotMetadata metadata;
    std::vector<std::uint8_t> state;
    std::vector<std::uint8_t> integrity_hash;
};

}  // namespace kura::metadata
