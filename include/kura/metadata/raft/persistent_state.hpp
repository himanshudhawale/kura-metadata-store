#pragma once

#include "kura/metadata/raft/log_entry.hpp"
#include "kura/metadata/raft/node_id.hpp"
#include "kura/metadata/raft/snapshot_metadata.hpp"
#include "kura/metadata/raft/term.hpp"

#include <optional>
#include <vector>

namespace kura::metadata {

struct RaftHardState {
    Term current_term;
    std::optional<NodeId> voted_for;

    bool operator==(const RaftHardState&) const = default;
};

struct PersistentRaftState {
    Term current_term;
    std::optional<NodeId> voted_for;
    std::optional<SnapshotMetadata> snapshot;
    std::vector<LogEntry> log;
};

}  // namespace kura::metadata
