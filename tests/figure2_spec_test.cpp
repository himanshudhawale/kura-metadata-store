#include "kura/metadata/raft/figure2_spec.hpp"

#include <algorithm>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace {

using namespace kura::metadata;
using namespace kura::metadata::figure2;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

std::set<RuleId> exercised_rules;

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename Exception, typename Operation>
void expect_throws(Operation&& operation, const std::string& message) {
    try {
        std::forward<Operation>(operation)();
    } catch (const Exception&) {
        return;
    }
    throw TestFailure(message);
}

template <typename EffectType>
std::size_t effect_position(const StepResult& result) {
    const auto found = std::ranges::find_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<EffectType>(effect);
        });
    return found == result.effects.end()
        ? result.effects.size()
        : static_cast<std::size_t>(
              std::distance(result.effects.begin(), found));
}

void record(const StepResult& result) {
    exercised_rules.insert(result.rules.begin(), result.rules.end());
}

StepResult complete_hard_state(StepResult result) {
    while (result.state.pending_hard_state) {
        const auto request = result.state.pending_hard_state->request;
        result = step(
            result.state,
            RaftHardStatePersisted{
                .request_id = request.request_id,
                .state = request.state});
    }
    return result;
}

CommandEnvelope command(const std::uint64_t sequence) {
    return {
        .request_id = {.client = 9, .sequence = sequence},
        .type_tag = 4,
        .payload = {static_cast<std::uint8_t>(sequence)}};
}

LogEntry entry(
    const std::uint64_t index,
    const std::uint64_t term_value) {
    return {
        .term = {term_value},
        .index = {index},
        .command = command(index)};
}

State follower(
    const std::uint64_t term_value = 0,
    std::vector<LogEntry> log = {}) {
    return {
        .node = {1},
        .peers = {{2}, {3}},
        .role = RaftRole::follower,
        .persistent = {
            .current_term = {term_value},
            .voted_for = std::nullopt,
            .log = std::move(log)}};
}

void catalog_and_invariants_are_executable() {
    expect(catalog().size() == 21, "catalog must contain every mapped rule");
    std::set<RuleId> ids;
    for (const auto& descriptor : catalog()) {
        expect(ids.insert(descriptor.id).second, "rule IDs must be unique");
        expect(
            !descriptor.preconditions.empty()
                && !descriptor.state_changes.empty()
                && !descriptor.emitted_effects.empty()
                && !descriptor.persistence_ordering.empty(),
            "every rule must specify all transition dimensions");
        expect(!rule_name(descriptor.id).empty(), "every rule has a name");
    }

    auto invalid = follower(2, {entry(2, 2)});
    expect(
        !validate(invalid).empty(),
        "non-contiguous logs must fail invariant validation");
    invalid = follower(2, {entry(1, 2)});
    invalid.volatile_state.commit_index = {2};
    expect(
        !validate(invalid).empty(),
        "commitIndex beyond the log must fail invariant validation");

    const auto local = follower(4, {entry(1, 2), entry(2, 4)});
    expect(
        is_log_at_least_as_up_to_date(local, Term{5}, LogIndex{1}),
        "a higher last term is fresher despite a shorter index");
    expect(
        !is_log_at_least_as_up_to_date(local, Term{4}, LogIndex{1}),
        "equal last term requires at least the local last index");
}

void request_vote_rules_and_persistence_order() {
    auto state = follower(2, {entry(1, 2)});
    auto result = step(state, ReceiveRequestVote{
                                  .from = {2},
                                  .request = {{1}, {2}, {1}, {2}}});
    record(result);
    expect(
        result.rules == std::vector{RuleId::request_vote_reject_stale},
        "stale vote request is rejected without term change");
    expect(
        effect_position<PersistRaftHardState>(result)
            == result.effects.size(),
        "stale vote rejection does not persist unchanged state");

    result = step(state, ReceiveRequestVote{
                             .from = {2},
                             .request = {{3}, {2}, {1}, {1}}});
    record(result);
    expect(
        result.state.persistent.current_term == Term{3}
            && !result.state.persistent.voted_for,
        "higher term is adopted but stale-log candidate is denied");
    expect(
        effect_position<PersistRaftHardState>(result) == 0
            && effect_position<SendRequestVoteResponse>(result)
                == result.effects.size(),
        "higher-term vote denial is withheld for durable completion");
    result = complete_hard_state(std::move(result));
    expect(
        effect_position<SendRequestVoteResponse>(result)
            < result.effects.size(),
        "durable completion releases the higher-term vote denial");

    result = step(state, ReceiveRequestVote{
                             .from = {2},
                             .request = {{3}, {2}, {1}, {3}}});
    record(result);
    expect(
        result.state.persistent.voted_for == NodeId{2},
        "fresh higher-term candidate receives the vote");
    expect(
        effect_position<PersistRaftHardState>(result) == 0
            && effect_position<SendRequestVoteResponse>(result)
                == result.effects.size(),
        "granted vote is withheld for durable completion");
    result = complete_hard_state(std::move(result));
    expect(
        effect_position<SendRequestVoteResponse>(result)
            < result.effects.size()
            && std::get<SendRequestVoteResponse>(result.effects.back())
                .response.granted,
        "durable completion releases the granted vote");

    state.persistent.voted_for = NodeId{3};
    result = step(state, ReceiveRequestVote{
                             .from = {2},
                             .request = {{2}, {2}, {1}, {2}}});
    record(result);
    expect(
        std::get<SendRequestVoteResponse>(result.effects.back())
                .response.granted
            == false,
        "one vote per term is enforced");
}

void hard_state_completion_is_an_explicit_input() {
    auto result = step(follower(), ElectionTimeout{});
    expect(result.state.pending_hard_state.has_value(), "hard state is not pending");
    const auto pending = result.state.pending_hard_state->request;
    expect(
        std::get<PersistRaftHardState>(result.effects.front()) == pending,
        "persist effect and pending request differ");
    expect_throws<std::logic_error>(
        [&] {
            static_cast<void>(step(result.state, HeartbeatTimeout{}));
        },
        "the core accepted input before durable completion");
    expect_throws<std::invalid_argument>(
        [&] {
            static_cast<void>(step(
                result.state,
                RaftHardStatePersisted{
                    .request_id = pending.request_id + 1,
                    .state = pending.state}));
        },
        "a mismatched durable completion was accepted");

    result = step(
        result.state,
        RaftHardStatePersisted{
            .request_id = pending.request_id,
            .state = pending.state});
    expect(
        !result.state.pending_hard_state
            && effect_position<SendRequestVote>(result)
                < result.effects.size(),
        "matching durable completion did not release deferred effects");
}

void append_entries_rules_and_persistence_order() {
    auto state = follower(3, {entry(1, 1), entry(2, 2)});
    auto result = step(state, ReceiveAppendEntries{
                                  .from = {2},
                                  .request = {
                                      .term = {2},
                                      .leader = {2},
                                      .previous_log_index = {2},
                                      .previous_log_term = {2}}});
    record(result);
    expect(
        result.rules == std::vector{RuleId::append_entries_reject_stale},
        "stale leader is rejected");
    expect(
        effect_position<ResetElectionTimer>(result) == result.effects.size(),
        "stale leader cannot reset the election timer");

    result = step(state, ReceiveAppendEntries{
                             .from = {2},
                             .request = {
                                 .term = {3},
                                 .leader = {2},
                                 .previous_log_index = {2},
                                 .previous_log_term = {1}}});
    record(result);
    expect(
        std::ranges::find(
            result.rules,
            RuleId::append_entries_reject_log_mismatch)
            != result.rules.end(),
        "previous-term mismatch rejects append");

    result = step(state, ReceiveAppendEntries{
                             .from = {2},
                             .request = {
                                 .term = {3},
                                 .leader = {2},
                                 .previous_log_index = {1},
                                 .previous_log_term = {1},
                                 .entries = {entry(2, 3), entry(3, 3)},
                                 .leader_commit = {3}}});
    record(result);
    expect(
        result.state.persistent.log.size() == 3
            && result.state.persistent.log[1].term == Term{3},
        "conflicting suffix is replaced");
    expect(
        result.state.volatile_state.commit_index == LogIndex{3},
        "follower commit is capped by its log");
    expect(
        effect_position<PersistState>(result)
            < effect_position<SendAppendEntriesResponse>(result),
        "changed log persists before successful response");

    state.role = RaftRole::candidate;
    state.votes_received = {{1}};
    result = step(state, ReceiveAppendEntries{
                             .from = {2},
                             .request = {
                                 .term = {3},
                                 .leader = {2},
                                 .previous_log_index = {2},
                                 .previous_log_term = {2}}});
    record(result);
    expect(
        result.state.role == RaftRole::follower,
        "candidate accepts current-term leader and steps down");
}

State elect_leader() {
    auto result = step(follower(), ElectionTimeout{});
    record(result);
    expect(
        result.state.role == RaftRole::candidate
            && result.state.persistent.current_term == Term{1}
            && result.state.persistent.voted_for == NodeId{1},
        "follower timeout starts a persisted self-vote election");
    expect(
        effect_position<PersistRaftHardState>(result) == 0
            && effect_position<SendRequestVote>(result)
                == result.effects.size(),
        "vote requests are withheld for durable election persistence");
    result = complete_hard_state(std::move(result));
    expect(
        effect_position<SendRequestVote>(result) < result.effects.size(),
        "durable completion releases vote requests");

    auto restarted = step(result.state, ElectionTimeout{});
    record(restarted);
    expect(
        restarted.state.persistent.current_term == Term{2},
        "candidate timeout starts a higher-term election");
    restarted = complete_hard_state(std::move(restarted));

    result = step(result.state, ReceiveRequestVoteResponse{
                                    .from = {2},
                                    .response = {{1}, true}});
    record(result);
    expect(
        result.state.role == RaftRole::leader && result.state.leader,
        "majority vote initializes leader-only state");
    expect(
        result.state.leader->progress.at(NodeId{2}).next_index
            == LogIndex{1},
        "new leader initializes peer progress at last index plus one");
    return result.state;
}

void leader_replication_commit_and_apply_rules() {
    auto leader = elect_leader();

    auto result = step(leader, HeartbeatTimeout{});
    record(result);
    expect(
        effect_position<SendAppendEntries>(result) < result.effects.size(),
        "leader heartbeat emits AppendEntries");

    result = step(leader, ReceiveClientCommand{command(1)});
    record(result);
    expect(
        result.state.persistent.log.size() == 1,
        "leader appends client command in its current term");
    expect(
        effect_position<PersistState>(result)
            < effect_position<SendAppendEntries>(result),
        "leader log persistence precedes replication");
    leader = result.state;

    result = step(leader, ReceiveAppendEntriesResponse{
                              .from = {2},
                              .response = {{1}, false, {}}});
    record(result);
    expect(
        result.state.leader->progress.at(NodeId{2}).next_index
            == LogIndex{1},
        "failed replication never decrements nextIndex below one");
    expect(
        effect_position<SendAppendEntries>(result) < result.effects.size(),
        "failed replication emits an immediate retry");

    result = step(leader, ReceiveAppendEntriesResponse{
                              .from = {2},
                              .response = {{1}, true, {1}}});
    record(result);
    expect(
        result.state.volatile_state.commit_index == LogIndex{1},
        "current-term entry commits after majority replication");
    leader = result.state;

    result = step(leader, ApplyCommitted{});
    record(result);
    expect(
        result.effects.size() == 2
            && std::holds_alternative<ApplyCommand>(result.effects[0])
            && std::holds_alternative<CompleteClientCommand>(
                result.effects[1]),
        "client completes only after ordered state-machine apply");

    result = step(leader, ReceiveAppendEntriesResponse{
                              .from = {2},
                              .response = {{2}, false, {}}});
    record(result);
    expect(
        result.state.role == RaftRole::follower
            && result.state.persistent.current_term == Term{2}
            && effect_position<PersistRaftHardState>(result) == 0
            && result.state.pending_hard_state,
        "higher response term requires an explicit persistence completion");
}

void leader_does_not_directly_commit_an_old_term_entry() {
    auto election = step(
        follower(1, {entry(1, 1)}),
        ElectionTimeout{});
    record(election);
    election = complete_hard_state(std::move(election));
    auto result = step(
        election.state,
        ReceiveRequestVoteResponse{
            .from = {2},
            .response = {{2}, true}});
    record(result);
    auto leader = result.state;

    result = step(leader, ReceiveAppendEntriesResponse{
                              .from = {2},
                              .response = {{2}, true, {1}}});
    record(result);
    expect(
        result.state.volatile_state.commit_index == LogIndex{},
        "majority replication cannot directly commit an old-term entry");

    result = step(result.state, ReceiveClientCommand{command(2)});
    record(result);
    result = step(result.state, ReceiveAppendEntriesResponse{
                                    .from = {2},
                                    .response = {{2}, true, {2}}});
    record(result);
    expect(
        result.state.volatile_state.commit_index == LogIndex{2},
        "committing a current-term entry indirectly commits its prefix");
}

void every_catalog_rule_has_a_deterministic_scenario() {
    for (const auto& descriptor : catalog()) {
        expect(
            exercised_rules.contains(descriptor.id),
            "missing deterministic scenario for "
                + std::string(descriptor.name));
    }
}

}  // namespace

int main() {
    try {
        catalog_and_invariants_are_executable();
        request_vote_rules_and_persistence_order();
        hard_state_completion_is_an_explicit_input();
        append_entries_rules_and_persistence_order();
        leader_replication_commit_and_apply_rules();
        leader_does_not_directly_commit_an_old_term_entry();
        every_catalog_rule_has_a_deterministic_scenario();
        std::cout << "figure2_spec_test: all tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "figure2_spec_test: " << error.what() << '\n';
        return 1;
    }
}
