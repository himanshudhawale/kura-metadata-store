#pragma once

#include "kura/metadata/raft/log_entry.hpp"
#include "kura/metadata/raft/node_id.hpp"
#include "kura/metadata/raft/read_index.hpp"

#include <optional>
#include <vector>

namespace kura::metadata {

struct AppendEntriesRequest {
    Term term;
    NodeId leader;
    LogIndex previous_log_index;
    Term previous_log_term;
    std::vector<LogEntry> entries;
    LogIndex leader_commit;
    std::optional<ReadIndexContext> read_context;
};

struct AppendEntriesResponse {
    Term term;
    bool succeeded{};
    LogIndex matched_index;
    std::optional<ReadIndexContext> read_context;
};

}  // namespace kura::metadata
