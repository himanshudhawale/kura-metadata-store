#pragma once

#include "kura/metadata/raft/log_index.hpp"
#include "kura/metadata/raft/node_id.hpp"
#include "kura/metadata/raft/term.hpp"

namespace kura::metadata {

struct RequestVoteRequest {
    Term term;
    NodeId candidate;
    LogIndex last_log_index;
    Term last_log_term;
};

struct RequestVoteResponse {
    Term term;
    bool granted{};
};

}  // namespace kura::metadata
