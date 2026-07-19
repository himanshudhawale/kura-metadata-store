#pragma once

#include "kura/metadata/core/revision.hpp"
#include "kura/metadata/raft/log_index.hpp"
#include "kura/metadata/raft/membership.hpp"
#include "kura/metadata/raft/term.hpp"

#include <cstdint>

namespace kura::metadata {

struct SnapshotMetadata {
    LogIndex last_included_index;
    Term last_included_term;
    Revision store_revision;
    Revision compaction_revision;
    ClusterMembership membership;
    std::uint32_t format_version{};

    bool operator==(const SnapshotMetadata&) const = default;
};

}  // namespace kura::metadata
