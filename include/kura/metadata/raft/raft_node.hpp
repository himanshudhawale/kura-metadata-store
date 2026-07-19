#pragma once

#include "kura/metadata/raft/append_entries.hpp"
#include "kura/metadata/raft/request_vote.hpp"
#include "kura/metadata/raft/role.hpp"

namespace kura::metadata {

class RaftNode {
public:
    virtual ~RaftNode() = default;

    [[nodiscard]] virtual RaftRole role() const = 0;
    [[nodiscard]] virtual AppendEntriesResponse append(
        const AppendEntriesRequest& request) = 0;
    [[nodiscard]] virtual RequestVoteResponse vote(
        const RequestVoteRequest& request) = 0;
};

}  // namespace kura::metadata
