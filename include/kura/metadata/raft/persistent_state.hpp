#pragma once

#include "kura/metadata/raft/log_entry.hpp"
#include "kura/metadata/raft/node_id.hpp"
#include "kura/metadata/raft/term.hpp"

#include <optional>
#include <vector>

namespace kura::metadata {

struct PersistentRaftState {
    Term current_term;
    std::optional<NodeId> voted_for;
    std::vector<LogEntry> log;
};

}  // namespace kura::metadata
