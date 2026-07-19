#pragma once

#include "kura/metadata/core/command.hpp"
#include "kura/metadata/raft/append_entries.hpp"
#include "kura/metadata/raft/persistent_state.hpp"
#include "kura/metadata/raft/request_vote.hpp"
#include "kura/metadata/raft/role.hpp"

#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace kura::metadata::figure2 {

enum class StateClass {
    persistent,
    volatile_all_servers,
    volatile_leader
};

enum class RuleId {
    observe_higher_term,
    apply_committed,
    follower_election_timeout,
    request_vote_reject_stale,
    request_vote_grant,
    request_vote_deny,
    append_entries_reject_stale,
    append_entries_reject_log_mismatch,
    append_entries_accept,
    append_entries_advance_commit,
    candidate_start_election,
    candidate_restart_election,
    candidate_win_election,
    candidate_accept_append_entries,
    leader_initialize,
    leader_append_client,
    leader_replicate,
    leader_record_replication,
    leader_retry_replication,
    leader_advance_commit,
    leader_heartbeat
};

struct RuleDescriptor {
    RuleId id;
    std::string_view name;
    std::string_view server_scope;
    std::string_view preconditions;
    std::string_view state_changes;
    std::string_view emitted_effects;
    std::string_view persistence_ordering;
};

[[nodiscard]] const std::vector<RuleDescriptor>& catalog();
[[nodiscard]] std::string_view rule_name(RuleId id);

struct VolatileState {
    LogIndex commit_index;
    LogIndex last_applied;
};

struct PeerProgress {
    LogIndex next_index{1};
    LogIndex match_index;
};

struct LeaderState {
    std::map<NodeId, PeerProgress> progress;
    std::set<LogIndex> pending_clients;
};

struct State {
    NodeId node;
    std::vector<NodeId> peers;
    RaftRole role{RaftRole::follower};
    PersistentRaftState persistent;
    VolatileState volatile_state;
    std::optional<NodeId> known_leader;
    std::set<NodeId> votes_received;
    std::optional<LeaderState> leader;
};

struct ElectionTimeout {};
struct HeartbeatTimeout {};
struct ApplyCommitted {};

struct ReceiveRequestVote {
    NodeId from;
    RequestVoteRequest request;
};

struct ReceiveRequestVoteResponse {
    NodeId from;
    RequestVoteResponse response;
};

struct ReceiveAppendEntries {
    NodeId from;
    AppendEntriesRequest request;
};

struct ReceiveAppendEntriesResponse {
    NodeId from;
    AppendEntriesResponse response;
};

struct ReceiveClientCommand {
    CommandEnvelope command;
};

using Event = std::variant<
    ElectionTimeout,
    HeartbeatTimeout,
    ApplyCommitted,
    ReceiveRequestVote,
    ReceiveRequestVoteResponse,
    ReceiveAppendEntries,
    ReceiveAppendEntriesResponse,
    ReceiveClientCommand>;

enum class PersistentField {
    current_term,
    voted_for,
    log
};

struct PersistState {
    std::vector<PersistentField> fields;
};

struct SendRequestVote {
    NodeId to;
    RequestVoteRequest request;
};

struct SendRequestVoteResponse {
    NodeId to;
    RequestVoteResponse response;
};

struct SendAppendEntries {
    NodeId to;
    AppendEntriesRequest request;
};

struct SendAppendEntriesResponse {
    NodeId to;
    AppendEntriesResponse response;
};

struct ResetElectionTimer {};

struct ApplyCommand {
    LogIndex index;
    CommandEnvelope command;
};

struct CompleteClientCommand {
    LogIndex index;
};

using Effect = std::variant<
    PersistState,
    SendRequestVote,
    SendRequestVoteResponse,
    SendAppendEntries,
    SendAppendEntriesResponse,
    ResetElectionTimer,
    ApplyCommand,
    CompleteClientCommand>;

struct StepResult {
    State state;
    std::vector<RuleId> rules;
    std::vector<Effect> effects;
};

struct InvariantViolation {
    std::string message;
};

[[nodiscard]] std::vector<InvariantViolation> validate(const State& state);
[[nodiscard]] bool is_log_at_least_as_up_to_date(
    const State& state,
    Term candidate_last_term,
    LogIndex candidate_last_index);
[[nodiscard]] StepResult step(const State& state, const Event& event);

}  // namespace kura::metadata::figure2
