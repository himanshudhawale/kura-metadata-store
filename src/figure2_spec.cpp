#include "kura/metadata/raft/figure2_spec.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kura::metadata::figure2 {
namespace {

const std::vector<RuleDescriptor> rule_catalog{
    {RuleId::observe_higher_term, "observe higher term", "all servers",
     "An incoming RPC request or response has term > currentTerm.",
     "Set currentTerm, clear votedFor, become follower, and clear candidate/leader volatile state.",
     "PersistRaftHardState(currentTerm, votedFor) blocks further processing.",
     "The new term and empty vote reach stable storage before any response or new-term activity."},
    {RuleId::apply_committed, "apply committed entry", "all servers",
     "commitIndex > lastApplied.",
     "Increment lastApplied by one.",
     "ApplyCommand, then CompleteClientCommand when this leader owns the pending request.",
     "No Raft persistence; apply is emitted strictly in index order and client completion follows apply."},
    {RuleId::follower_election_timeout, "follower election timeout", "follower",
     "Election timeout event while follower.",
     "Become candidate and start an election.",
     "The candidate-start effects are emitted.",
     "Term and self-vote persistence precede RequestVote sends."},
    {RuleId::request_vote_reject_stale, "reject stale RequestVote", "RPC receiver",
     "request.term < currentTerm.",
     "No state change.",
     "SendRequestVoteResponse(granted=false, currentTerm).",
     "No persistence is needed because persistent state is unchanged."},
    {RuleId::request_vote_grant, "grant RequestVote", "RPC receiver",
     "Term is current; votedFor is empty or candidate; candidate log is at least as up-to-date.",
     "Set votedFor to candidate.",
     "PersistRaftHardState(term, vote); completion releases timer reset and granted response.",
     "The vote reaches stable storage before the granted response."},
    {RuleId::request_vote_deny, "deny RequestVote", "RPC receiver",
     "Term is current, but prior vote or log freshness condition fails.",
     "No state change.",
     "SendRequestVoteResponse(granted=false).",
     "Any preceding higher-term persistence completes before the denial."},
    {RuleId::append_entries_reject_stale, "reject stale AppendEntries", "RPC receiver",
     "request.term < currentTerm.",
     "No state change.",
     "SendAppendEntriesResponse(succeeded=false, currentTerm).",
     "No persistence is needed because persistent state is unchanged."},
    {RuleId::append_entries_reject_log_mismatch, "reject log mismatch", "RPC receiver",
     "The log lacks prevLogIndex or its term differs from prevLogTerm.",
     "No log or commit-index change.",
     "ResetElectionTimer and failed AppendEntries response.",
     "Any preceding higher-term persistence completes before the response."},
    {RuleId::append_entries_accept, "accept AppendEntries", "RPC receiver",
     "Term is current and the previous-log entry matches.",
     "Become follower; delete a conflicting suffix and append entries not already present.",
     "ResetElectionTimer, optional PersistState(log), then successful response.",
     "A changed log reaches stable storage before the successful response."},
    {RuleId::append_entries_advance_commit, "advance follower commit", "RPC receiver",
     "Accepted AppendEntries has leaderCommit > commitIndex.",
     "Set commitIndex=min(leaderCommit,last new/existing log index).",
     "No additional effect; application is a separate all-server step.",
     "commitIndex is volatile; appended entries are persisted before success."},
    {RuleId::candidate_start_election, "start election", "candidate",
     "Follower timeout or explicit candidate election start.",
     "Increment term, vote for self, retain only self vote, clear known leader.",
     "PersistRaftHardState(term,self); completion releases timer reset and RequestVote sends.",
     "Term and self-vote reach stable storage before any election RPC."},
    {RuleId::candidate_restart_election, "restart election", "candidate",
     "Election timeout while candidate.",
     "Start a new election in a higher term.",
     "The candidate-start effects are emitted.",
     "New term and self-vote persistence precede RequestVote sends."},
    {RuleId::candidate_win_election, "win election", "candidate",
     "Granted votes, including self, form a majority in the current term.",
     "Become leader and initialize leader-only state.",
     "Initial empty AppendEntries to every peer.",
     "No new persistence; prior self-vote persistence already completed."},
    {RuleId::candidate_accept_append_entries, "candidate steps down", "candidate",
     "AppendEntries term is at least currentTerm.",
     "Become follower and record the leader.",
     "Continue through normal AppendEntries receiver rules.",
     "Higher term is persisted first when applicable."},
    {RuleId::leader_initialize, "initialize leader", "leader",
     "Candidate wins an election.",
     "For each peer set nextIndex=lastLogIndex+1 and matchIndex=0.",
     "Send initial empty AppendEntries heartbeats.",
     "No persistent state changes."},
    {RuleId::leader_append_client, "append client command", "leader",
     "A client command arrives at the leader.",
     "Append a current-term entry and track its pending client response.",
     "PersistState(log), then replication sends.",
     "The local entry reaches stable storage before it is sent to peers; response waits for apply."},
    {RuleId::leader_replicate, "replicate log", "leader",
     "A heartbeat/replication trigger occurs for a peer.",
     "No state change.",
     "Send AppendEntries from peer.nextIndex, possibly empty.",
     "All entries in the request were persisted before this send."},
    {RuleId::leader_record_replication, "record replication success", "leader",
     "Current-term successful AppendEntries response.",
     "Raise matchIndex to matchedIndex and nextIndex to matchedIndex+1.",
     "May trigger volatile commit advancement.",
     "Follower success implies its log persistence completed."},
    {RuleId::leader_retry_replication, "retry replication", "leader",
     "Current-term failed AppendEntries response.",
     "Decrement nextIndex, never below one.",
     "Send retry AppendEntries from the new nextIndex.",
     "No persistence; retry references only already-persisted entries."},
    {RuleId::leader_advance_commit, "advance leader commit", "leader",
     "A majority match N > commitIndex and log[N].term == currentTerm.",
     "Set commitIndex to the greatest such N.",
     "No immediate apply; application is a separate all-server step.",
     "commitIndex is volatile; the current-term restriction prevents unsafe indirect commitment."},
    {RuleId::leader_heartbeat, "leader heartbeat", "leader",
     "Heartbeat timeout event.",
     "No state change.",
     "Send AppendEntries to every peer.",
     "Requests contain only entries already persisted locally."},
};

std::uint64_t last_index(const State& state) {
    return state.persistent.log.empty()
        ? 0
        : state.persistent.log.back().index.value;
}

Term last_term(const State& state) {
    return state.persistent.log.empty()
        ? Term{}
        : state.persistent.log.back().term;
}

Term term_at(const State& state, const std::uint64_t index) {
    return index == 0 ? Term{} : state.persistent.log.at(index - 1).term;
}

std::size_t majority(const State& state) {
    return (state.peers.size() + 1) / 2 + 1;
}

void ensure_peer(const State& state, const NodeId peer) {
    if (std::ranges::find(state.peers, peer) == state.peers.end()) {
        throw std::invalid_argument("RPC sender is not a voting peer");
    }
}

void ensure_valid(const State& state, const std::string_view when) {
    const auto violations = validate(state);
    if (!violations.empty()) {
        throw std::invalid_argument(
            std::string(when) + ": " + violations.front().message);
    }
}

RaftHardState hard_state(const State& state) {
    return {
        .current_term = state.persistent.current_term,
        .voted_for = state.persistent.voted_for};
}

void persist_hard_state(StepResult& result) {
    if (result.state.next_hard_state_request_id == 0
        || result.state.next_hard_state_request_id
            == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("Raft hard-state request ID exhausted");
    }
    result.effects.push_back(PersistRaftHardState{
        .request_id = result.state.next_hard_state_request_id++,
        .state = hard_state(result.state)});
}

void gate_on_hard_state(StepResult& result) {
    const auto persistence = std::ranges::find_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<PersistRaftHardState>(effect);
        });
    if (persistence == result.effects.end()) {
        return;
    }
    if (result.state.pending_hard_state) {
        throw std::logic_error("Raft hard-state persistence is already pending");
    }
    const auto request = std::get<PersistRaftHardState>(*persistence);
    auto after = persistence;
    ++after;
    std::vector<Effect> deferred(
        std::make_move_iterator(after),
        std::make_move_iterator(result.effects.end()));
    result.effects.erase(after, result.effects.end());
    result.state.pending_hard_state = PendingHardStatePersistence{
        .request = request,
        .deferred_effects = std::move(deferred)};
}

AppendEntriesRequest append_request(const State& state, const NodeId peer) {
    const auto& progress = state.leader->progress.at(peer);
    const auto previous = progress.next_index.value - 1;
    std::vector<LogEntry> entries;
    if (progress.next_index.value <= last_index(state)) {
        entries.assign(
            state.persistent.log.begin()
                + static_cast<std::ptrdiff_t>(progress.next_index.value - 1),
            state.persistent.log.end());
    }
    return {
        .term = state.persistent.current_term,
        .leader = state.node,
        .previous_log_index = {previous},
        .previous_log_term = term_at(state, previous),
        .entries = std::move(entries),
        .leader_commit = state.volatile_state.commit_index};
}

void send_append(StepResult& result, const NodeId peer) {
    result.rules.push_back(RuleId::leader_replicate);
    result.effects.push_back(
        SendAppendEntries{peer, append_request(result.state, peer)});
}

void initialize_leader(StepResult& result) {
    if (last_index(result.state) == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("Raft log index exhausted");
    }
    result.state.role = RaftRole::leader;
    result.state.votes_received.clear();
    result.state.known_leader = result.state.node;
    result.state.leader.emplace();
    const auto next = last_index(result.state) + 1;
    for (const auto peer : result.state.peers) {
        result.state.leader->progress.emplace(
            peer, PeerProgress{.next_index = {next}, .match_index = {}});
    }
    result.rules.push_back(RuleId::candidate_win_election);
    result.rules.push_back(RuleId::leader_initialize);
    for (const auto peer : result.state.peers) {
        send_append(result, peer);
    }
}

void start_election(StepResult& result) {
    if (result.state.persistent.current_term.value
        == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("Raft term exhausted");
    }
    ++result.state.persistent.current_term.value;
    result.state.persistent.voted_for = result.state.node;
    result.state.role = RaftRole::candidate;
    result.state.known_leader.reset();
    result.state.leader.reset();
    result.state.votes_received = {result.state.node};
    result.rules.push_back(RuleId::candidate_start_election);
    persist_hard_state(result);
    result.effects.push_back(ResetElectionTimer{});
    for (const auto peer : result.state.peers) {
        result.effects.push_back(SendRequestVote{
            .to = peer,
            .request = {
                .term = result.state.persistent.current_term,
                .candidate = result.state.node,
                .last_log_index = {last_index(result.state)},
                .last_log_term = last_term(result.state)}});
    }
    if (majority(result.state) == 1) {
        initialize_leader(result);
    }
}

template <typename Rpc>
Term rpc_term(const Rpc& rpc) {
    if constexpr (
        std::is_same_v<Rpc, ReceiveRequestVote>
        || std::is_same_v<Rpc, ReceiveAppendEntries>) {
        return rpc.request.term;
    } else if constexpr (
        std::is_same_v<Rpc, ReceiveRequestVoteResponse>
        || std::is_same_v<Rpc, ReceiveAppendEntriesResponse>) {
        return rpc.response.term;
    } else {
        return {};
    }
}

template <typename Rpc>
void observe_higher_term(StepResult& result, const Rpc& rpc) {
    const auto observed = rpc_term(rpc);
    if (observed > result.state.persistent.current_term) {
        result.state.persistent.current_term = observed;
        result.state.persistent.voted_for.reset();
        result.state.role = RaftRole::follower;
        result.state.known_leader.reset();
        result.state.votes_received.clear();
        result.state.leader.reset();
        result.rules.push_back(RuleId::observe_higher_term);
        persist_hard_state(result);
    }
}

void handle_request_vote(StepResult& result, const ReceiveRequestVote& event) {
    ensure_peer(result.state, event.from);
    if (event.request.candidate != event.from) {
        throw std::invalid_argument("RequestVote sender does not match candidate");
    }
    observe_higher_term(result, event);
    if (event.request.term < result.state.persistent.current_term) {
        result.rules.push_back(RuleId::request_vote_reject_stale);
        result.effects.push_back(SendRequestVoteResponse{
            event.from,
            {result.state.persistent.current_term, false}});
        return;
    }
    const bool can_vote = !result.state.persistent.voted_for
        || result.state.persistent.voted_for == event.request.candidate;
    const bool fresh = is_log_at_least_as_up_to_date(
        result.state, event.request.last_log_term, event.request.last_log_index);
    if (can_vote && fresh) {
        const bool changed =
            result.state.persistent.voted_for != event.request.candidate;
        result.state.persistent.voted_for = event.request.candidate;
        result.rules.push_back(RuleId::request_vote_grant);
        if (changed) {
            persist_hard_state(result);
        }
        result.effects.push_back(ResetElectionTimer{});
        result.effects.push_back(SendRequestVoteResponse{
            event.from,
            {result.state.persistent.current_term, true}});
    } else {
        result.rules.push_back(RuleId::request_vote_deny);
        result.effects.push_back(SendRequestVoteResponse{
            event.from,
            {result.state.persistent.current_term, false}});
    }
}

void handle_request_vote_response(
    StepResult& result,
    const ReceiveRequestVoteResponse& event) {
    ensure_peer(result.state, event.from);
    observe_higher_term(result, event);
    if (result.state.role != RaftRole::candidate
        || event.response.term != result.state.persistent.current_term
        || !event.response.granted) {
        return;
    }
    result.state.votes_received.insert(event.from);
    if (result.state.votes_received.size() >= majority(result.state)) {
        initialize_leader(result);
    }
}

void handle_append_entries(
    StepResult& result,
    const ReceiveAppendEntries& event) {
    ensure_peer(result.state, event.from);
    if (event.request.leader != event.from) {
        throw std::invalid_argument("AppendEntries sender does not match leader");
    }
    const bool was_candidate = result.state.role == RaftRole::candidate;
    observe_higher_term(result, event);
    if (event.request.term < result.state.persistent.current_term) {
        result.rules.push_back(RuleId::append_entries_reject_stale);
        result.effects.push_back(SendAppendEntriesResponse{
            event.from,
            {result.state.persistent.current_term, false, {}}});
        return;
    }
    if (was_candidate) {
        result.rules.push_back(RuleId::candidate_accept_append_entries);
    }
    result.state.role = RaftRole::follower;
    result.state.leader.reset();
    result.state.votes_received.clear();
    result.state.known_leader = event.from;
    result.effects.push_back(ResetElectionTimer{});

    const auto previous = event.request.previous_log_index.value;
    const bool matches = previous <= last_index(result.state)
        && term_at(result.state, previous) == event.request.previous_log_term;
    if (!matches) {
        result.rules.push_back(RuleId::append_entries_reject_log_mismatch);
        result.effects.push_back(SendAppendEntriesResponse{
            event.from,
            {result.state.persistent.current_term, false, {}}});
        return;
    }

    std::uint64_t expected = previous + 1;
    for (const auto& entry : event.request.entries) {
        if (entry.index.value != expected++) {
            throw std::invalid_argument("AppendEntries entries are not contiguous");
        }
    }

    bool log_changed = false;
    std::size_t incoming = 0;
    while (incoming < event.request.entries.size()) {
        const auto index = event.request.entries[incoming].index.value;
        if (index > last_index(result.state)) {
            break;
        }
        if (term_at(result.state, index)
            != event.request.entries[incoming].term) {
            if (index
                <= result.state.volatile_state.commit_index.value) {
                result.rules.push_back(
                    RuleId::append_entries_reject_log_mismatch);
                result.effects.push_back(SendAppendEntriesResponse{
                    event.from,
                    {
                        result.state.persistent.current_term,
                        false,
                        {}}});
                return;
            }
            result.state.persistent.log.erase(
                result.state.persistent.log.begin()
                    + static_cast<std::ptrdiff_t>(index - 1),
                result.state.persistent.log.end());
            log_changed = true;
            break;
        }
        ++incoming;
    }
    if (incoming < event.request.entries.size()) {
        result.state.persistent.log.insert(
            result.state.persistent.log.end(),
            event.request.entries.begin()
                + static_cast<std::ptrdiff_t>(incoming),
            event.request.entries.end());
        log_changed = true;
    }
    result.rules.push_back(RuleId::append_entries_accept);
    if (log_changed) {
        result.effects.push_back(PersistState{{PersistentField::log}});
    }

    const auto old_commit = result.state.volatile_state.commit_index;
    if (event.request.leader_commit > old_commit) {
        const auto last_new_index =
            previous + event.request.entries.size();
        const LogIndex bounded_commit{
            std::min(event.request.leader_commit.value, last_new_index)};
        if (bounded_commit > old_commit) {
            result.state.volatile_state.commit_index = bounded_commit;
            result.rules.push_back(RuleId::append_entries_advance_commit);
        }
    }
    result.effects.push_back(SendAppendEntriesResponse{
        event.from,
        {
            result.state.persistent.current_term,
            true,
            {previous + event.request.entries.size()}}});
}

void advance_leader_commit(StepResult& result) {
    const auto old_commit = result.state.volatile_state.commit_index.value;
    for (auto candidate = last_index(result.state);
         candidate > old_commit;
         --candidate) {
        if (term_at(result.state, candidate)
            != result.state.persistent.current_term) {
            continue;
        }
        std::size_t replicated = 1;
        for (const auto& [peer, progress] : result.state.leader->progress) {
            static_cast<void>(peer);
            if (progress.match_index.value >= candidate) {
                ++replicated;
            }
        }
        if (replicated >= majority(result.state)) {
            result.state.volatile_state.commit_index = {candidate};
            result.rules.push_back(RuleId::leader_advance_commit);
            return;
        }
    }
}

void handle_append_entries_response(
    StepResult& result,
    const ReceiveAppendEntriesResponse& event) {
    ensure_peer(result.state, event.from);
    observe_higher_term(result, event);
    if (result.state.role != RaftRole::leader
        || event.response.term != result.state.persistent.current_term) {
        return;
    }
    const auto progress_it = result.state.leader->progress.find(event.from);
    if (progress_it == result.state.leader->progress.end()) {
        throw std::invalid_argument("AppendEntries response from non-peer");
    }
    auto& progress = progress_it->second;
    if (event.response.succeeded) {
        if (event.response.matched_index.value > last_index(result.state)) {
            throw std::invalid_argument("matched index exceeds leader log");
        }
        progress.match_index = {
            std::max(
                progress.match_index.value,
                event.response.matched_index.value)};
        progress.next_index = {progress.match_index.value + 1};
        result.rules.push_back(RuleId::leader_record_replication);
        advance_leader_commit(result);
    } else {
        progress.next_index = {
            std::max<std::uint64_t>(1, progress.next_index.value - 1)};
        result.rules.push_back(RuleId::leader_retry_replication);
        send_append(result, event.from);
    }
}

void handle_apply(StepResult& result) {
    if (result.state.volatile_state.commit_index
        <= result.state.volatile_state.last_applied) {
        return;
    }
    ++result.state.volatile_state.last_applied.value;
    const auto index = result.state.volatile_state.last_applied;
    result.rules.push_back(RuleId::apply_committed);
    result.effects.push_back(ApplyCommand{
        index, result.state.persistent.log.at(index.value - 1).command});
    if (result.state.leader
        && result.state.leader->pending_clients.erase(index) != 0) {
        result.effects.push_back(CompleteClientCommand{index});
    }
}

void handle_client(StepResult& result, const ReceiveClientCommand& event) {
    if (result.state.role != RaftRole::leader) {
        return;
    }
    if (last_index(result.state) == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("Raft log index exhausted");
    }
    const LogIndex index{last_index(result.state) + 1};
    result.state.persistent.log.push_back({
        .term = result.state.persistent.current_term,
        .index = index,
        .command = event.command});
    result.state.leader->pending_clients.insert(index);
    result.rules.push_back(RuleId::leader_append_client);
    result.effects.push_back(PersistState{{PersistentField::log}});
    for (const auto peer : result.state.peers) {
        send_append(result, peer);
    }
    advance_leader_commit(result);
}

void handle_timeout(StepResult& result) {
    if (result.state.role == RaftRole::leader) {
        result.rules.push_back(RuleId::leader_heartbeat);
        for (const auto peer : result.state.peers) {
            send_append(result, peer);
        }
        return;
    }
    if (result.state.role == RaftRole::follower) {
        result.rules.push_back(RuleId::follower_election_timeout);
    } else if (result.state.role == RaftRole::candidate) {
        result.rules.push_back(RuleId::candidate_restart_election);
    } else {
        return;
    }
    start_election(result);
}

}  // namespace

const std::vector<RuleDescriptor>& catalog() {
    return rule_catalog;
}

std::string_view rule_name(const RuleId id) {
    const auto found = std::ranges::find(
        rule_catalog, id, &RuleDescriptor::id);
    if (found == rule_catalog.end()) {
        throw std::invalid_argument("unknown Figure 2 rule");
    }
    return found->name;
}

std::vector<InvariantViolation> validate(const State& state) {
    std::vector<InvariantViolation> violations;
    std::set<NodeId> peers;
    for (const auto peer : state.peers) {
        if (peer == state.node) {
            violations.push_back({"peer list contains the local node"});
        }
        if (!peers.insert(peer).second) {
            violations.push_back({"peer list contains a duplicate"});
        }
    }
    std::uint64_t expected = 1;
    for (const auto& entry : state.persistent.log) {
        if (entry.index.value != expected++) {
            violations.push_back({"log indexes must be contiguous from one"});
            break;
        }
        if (entry.term > state.persistent.current_term) {
            violations.push_back({"log entry term exceeds currentTerm"});
            break;
        }
    }
    const auto end = last_index(state);
    if (state.volatile_state.commit_index.value > end) {
        violations.push_back({"commitIndex exceeds the last log index"});
    }
    if (state.volatile_state.last_applied
        > state.volatile_state.commit_index) {
        violations.push_back({"lastApplied exceeds commitIndex"});
    }
    if (state.role == RaftRole::leader && !state.leader) {
        violations.push_back({"leader role lacks leader-only state"});
    }
    if (state.role != RaftRole::leader && state.leader) {
        violations.push_back({"non-leader retains leader-only state"});
    }
    if (state.role != RaftRole::candidate && !state.votes_received.empty()) {
        violations.push_back({"non-candidate retains received votes"});
    }
    if (state.next_hard_state_request_id == 0) {
        violations.push_back({"next hard-state request ID is zero"});
    }
    if (state.pending_hard_state
        && state.pending_hard_state->request.request_id == 0) {
        violations.push_back({"pending hard-state request ID is zero"});
    }
    for (const auto voter : state.votes_received) {
        if (voter != state.node && !peers.contains(voter)) {
            violations.push_back({"candidate has a vote from a non-peer"});
        }
    }
    if (state.leader) {
        if (state.leader->progress.size() != state.peers.size()) {
            violations.push_back({"leader progress does not cover every peer"});
        }
        for (const auto peer : state.peers) {
            const auto found = state.leader->progress.find(peer);
            if (found == state.leader->progress.end()) {
                violations.push_back({"leader progress is missing a peer"});
                continue;
            }
            if (found->second.next_index.value < 1
                || found->second.next_index.value > end + 1) {
                violations.push_back({"peer nextIndex is outside the log"});
            }
            if (found->second.match_index.value > end
                || found->second.match_index.value
                    >= found->second.next_index.value) {
                violations.push_back({"peer matchIndex/nextIndex is invalid"});
            }
        }
        for (const auto pending : state.leader->pending_clients) {
            if (pending.value == 0 || pending.value > end) {
                violations.push_back({"pending client index is outside the log"});
            }
        }
    }
    return violations;
}

bool is_log_at_least_as_up_to_date(
    const State& state,
    const Term candidate_last_term,
    const LogIndex candidate_last_index) {
    const auto local_term = last_term(state);
    return candidate_last_term > local_term
        || (candidate_last_term == local_term
            && candidate_last_index.value >= last_index(state));
}

StepResult step(const State& state, const Event& event) {
    ensure_valid(state, "invalid pre-state");
    const bool is_completion =
        std::holds_alternative<RaftHardStatePersisted>(event);
    if (state.pending_hard_state && !is_completion) {
        throw std::logic_error(
            "Raft core input is blocked on hard-state persistence");
    }
    if (!state.pending_hard_state && is_completion) {
        throw std::invalid_argument(
            "unexpected Raft hard-state persistence completion");
    }
    StepResult result{.state = state};
    std::visit(
        [&result](const auto& concrete) {
            using Concrete = std::decay_t<decltype(concrete)>;
            if constexpr (std::is_same_v<Concrete, ElectionTimeout>) {
                if (result.state.role != RaftRole::leader) {
                    handle_timeout(result);
                }
            } else if constexpr (std::is_same_v<Concrete, HeartbeatTimeout>) {
                if (result.state.role == RaftRole::leader) {
                    handle_timeout(result);
                }
            } else if constexpr (std::is_same_v<Concrete, ApplyCommitted>) {
                handle_apply(result);
            } else if constexpr (
                std::is_same_v<Concrete, ReceiveRequestVote>) {
                handle_request_vote(result, concrete);
            } else if constexpr (
                std::is_same_v<Concrete, ReceiveRequestVoteResponse>) {
                handle_request_vote_response(result, concrete);
            } else if constexpr (
                std::is_same_v<Concrete, ReceiveAppendEntries>) {
                handle_append_entries(result, concrete);
            } else if constexpr (
                std::is_same_v<Concrete, ReceiveAppendEntriesResponse>) {
                handle_append_entries_response(result, concrete);
            } else if constexpr (
                std::is_same_v<Concrete, ReceiveClientCommand>) {
                handle_client(result, concrete);
            } else if constexpr (
                std::is_same_v<Concrete, RaftHardStatePersisted>) {
                const auto pending =
                    std::move(*result.state.pending_hard_state);
                if (concrete.request_id != pending.request.request_id
                    || concrete.state != pending.request.state) {
                    throw std::invalid_argument(
                        "Raft hard-state completion does not match pending request");
                }
                result.state.pending_hard_state.reset();
                result.effects = std::move(pending.deferred_effects);
            }
        },
        event);
    gate_on_hard_state(result);
    ensure_valid(result.state, "invalid post-state");
    return result;
}

}  // namespace kura::metadata::figure2
