#include "kura/metadata/raft/election.hpp"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using namespace kura::metadata;
using namespace kura::metadata::raft::election;
namespace simulation = kura::metadata::raft::simulation;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

template <typename EffectType>
const EffectType* find_effect(const StepResult& result) {
    const auto found = std::ranges::find_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<EffectType>(effect);
        });
    return found == result.effects.end()
        ? nullptr
        : &std::get<EffectType>(*found);
}

template <typename EffectType>
std::size_t count_effects(const StepResult& result) {
    return static_cast<std::size_t>(std::ranges::count_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<EffectType>(effect);
        }));
}

StepResult complete_one(Core& core) {
    const auto& pending =
        *core.specification_state().pending_hard_state;
    return core.step(RaftHardStatePersisted{
        .request_id = pending.request.request_id,
        .state = pending.request.state});
}

std::vector<StepResult> complete_all(Core& core) {
    std::vector<StepResult> results;
    while (core.snapshot().waiting_for_persistence) {
        results.push_back(complete_one(core));
    }
    return results;
}

Core three_node(
    const NodeId self,
    const std::uint64_t seed = 1,
    const RaftHardState recovered = {},
    std::vector<LogEntry> log = {}) {
    std::vector<NodeId> peers;
    for (const auto node : std::vector<NodeId>{{1}, {2}, {3}}) {
        if (node != self) {
            peers.push_back(node);
        }
    }
    return Core(
        self,
        std::move(peers),
        recovered,
        seed,
        TimeoutRange{5, 10},
        std::move(log));
}

Core five_node(const NodeId self) {
    std::vector<NodeId> peers;
    for (const auto node :
         std::vector<NodeId>{{1}, {2}, {3}, {4}, {5}}) {
        if (node != self) {
            peers.push_back(node);
        }
    }
    return Core(self, std::move(peers), {}, 7, TimeoutRange{5, 10});
}

StepResult start_election(Core& core) {
    const auto initial = core.start();
    const auto* deadline = find_effect<ResetElectionDeadline>(initial);
    expect(deadline != nullptr, "startup must set an election deadline");
    auto timed_out = core.step(ElectionDeadline{deadline->timer_id});
    expect(
        core.snapshot().role == RaftRole::candidate,
        "timeout must enter candidate role");
    expect(
        find_effect<PersistRaftHardState>(timed_out) != nullptr,
        "election must request durable self-vote");
    expect(
        count_effects<figure2::SendRequestVote>(timed_out) == 0,
        "RequestVote must wait for durable self-vote");
    return timed_out;
}

LogEntry log_entry(
    const std::uint64_t index,
    const std::uint64_t term) {
    return {
        .term = {term},
        .index = {index},
        .command = {
            .request_id = {.client = 1, .sequence = index},
            .type_tag = 1}};
}

void deterministic_deadlines_and_ordered_self_vote() {
    auto first = three_node({1}, 0x1234);
    auto second = three_node({1}, 0x1234);
    const auto first_start = first.start();
    const auto second_start = second.start();
    expect(
        find_effect<ResetElectionDeadline>(first_start)->delay
            == find_effect<ResetElectionDeadline>(second_start)->delay,
        "equal seeds must select equal logical deadlines");

    const auto timer =
        find_effect<ResetElectionDeadline>(first_start)->timer_id;
    const auto result = first.step(ElectionDeadline{timer});
    expect(
        std::holds_alternative<RoleTransition>(result.effects.at(0))
            && std::holds_alternative<PersistRaftHardState>(
                result.effects.at(1)),
        "candidate transition and persistence order must be deterministic");
    const auto completed = complete_all(first);
    expect(completed.size() == 1, "self-vote needs one durable completion");
    expect(
        count_effects<figure2::SendRequestVote>(completed.back()) == 2,
        "durable self-vote releases one request per peer");
    expect(
        find_effect<ResetElectionDeadline>(completed.back()) != nullptr,
        "durable election start resets the logical deadline");
}

void split_vote_and_simultaneous_timeouts() {
    auto one = three_node({1});
    auto two = three_node({2});
    auto three = three_node({3});
    start_election(one);
    start_election(two);
    start_election(three);
    complete_all(one);
    complete_all(two);
    complete_all(three);

    const auto denied_by_two = two.step(figure2::ReceiveRequestVote{
        .from = {1},
        .request = {{1}, {1}, {}, {}}});
    const auto denied_by_three = three.step(figure2::ReceiveRequestVote{
        .from = {1},
        .request = {{1}, {1}, {}, {}}});
    expect(
        !find_effect<figure2::SendRequestVoteResponse>(denied_by_two)
             ->response.granted
            && !find_effect<figure2::SendRequestVoteResponse>(denied_by_three)
                    ->response.granted,
        "simultaneous candidates must retain their one vote per term");
    expect(
        one.snapshot().role == RaftRole::candidate
            && two.snapshot().role == RaftRole::candidate
            && three.snapshot().role == RaftRole::candidate,
        "a split vote must elect no leader");
}

void vote_freshness_stale_terms_and_recovery() {
    auto follower = three_node(
        {1},
        1,
        RaftHardState{.current_term = {4}, .voted_for = std::nullopt},
        {log_entry(1, 2), log_entry(2, 4)});
    static_cast<void>(follower.start());

    auto stale_term = follower.step(figure2::ReceiveRequestVote{
        .from = {2},
        .request = {{3}, {2}, {2}, {4}}});
    expect(
        !find_effect<figure2::SendRequestVoteResponse>(stale_term)
             ->response.granted,
        "stale candidate term must be rejected");

    auto shorter_same_term = follower.step(figure2::ReceiveRequestVote{
        .from = {2},
        .request = {{4}, {2}, {1}, {4}}});
    expect(
        !find_effect<figure2::SendRequestVoteResponse>(shorter_same_term)
             ->response.granted,
        "same last term with shorter log must be rejected");

    auto higher_last_term = follower.step(figure2::ReceiveRequestVote{
        .from = {2},
        .request = {{4}, {2}, {1}, {5}}});
    expect(
        find_effect<PersistRaftHardState>(higher_last_term) != nullptr
            && find_effect<figure2::SendRequestVoteResponse>(
                   higher_last_term)
                == nullptr,
        "fresh vote must be withheld until persistence");
    const auto released = complete_one(follower);
    expect(
        find_effect<figure2::SendRequestVoteResponse>(released)
            ->response.granted,
        "durable fresh vote must be granted");

    auto restarted = three_node(
        {1},
        1,
        RaftHardState{.current_term = {4}, .voted_for = NodeId{2}},
        {log_entry(1, 2), log_entry(2, 4)});
    static_cast<void>(restarted.start());
    const auto second_candidate =
        restarted.step(figure2::ReceiveRequestVote{
            .from = {3},
            .request = {{4}, {3}, {2}, {4}}});
    expect(
        !find_effect<figure2::SendRequestVoteResponse>(second_candidate)
             ->response.granted,
        "restart must not grant a second vote in the recovered term");
}

void persistence_gates_higher_term_and_vote() {
    auto follower = three_node(
        {1},
        1,
        RaftHardState{.current_term = {2}, .voted_for = std::nullopt});
    static_cast<void>(follower.start());
    auto request = follower.step(figure2::ReceiveRequestVote{
        .from = {2},
        .request = {{3}, {2}, {}, {}}});
    expect(
        find_effect<PersistRaftHardState>(request) != nullptr
            && find_effect<figure2::SendRequestVoteResponse>(request)
                == nullptr,
        "higher term must be persisted before its vote response");

    auto term_completed = complete_one(follower);
    expect(
        find_effect<PersistRaftHardState>(term_completed) != nullptr
            && find_effect<figure2::SendRequestVoteResponse>(
                   term_completed)
                == nullptr,
        "higher term and vote use explicit ordered completions");
    auto vote_completed = complete_one(follower);
    expect(
        find_effect<figure2::SendRequestVoteResponse>(vote_completed)
            ->response.granted,
        "response must follow durable term and vote");
}

void duplicate_votes_quorums_and_higher_term_stepdown() {
    auto candidate = five_node({1});
    start_election(candidate);
    complete_all(candidate);

    auto first = candidate.step(figure2::ReceiveRequestVoteResponse{
        .from = {2}, .response = {{1}, true}});
    expect(
        candidate.snapshot().role == RaftRole::candidate
            && candidate.snapshot().votes_received.size() == 2,
        "two of five votes are not a quorum");
    static_cast<void>(first);
    static_cast<void>(candidate.step(figure2::ReceiveRequestVoteResponse{
        .from = {2}, .response = {{1}, true}}));
    expect(
        candidate.snapshot().votes_received.size() == 2,
        "duplicate response must not count twice");
    const auto won = candidate.step(figure2::ReceiveRequestVoteResponse{
        .from = {3}, .response = {{1}, true}});
    expect(
        candidate.snapshot().role == RaftRole::leader
            && find_effect<RoleTransition>(won)->to == RaftRole::leader,
        "three of five votes, including self, must elect");

    auto three_candidate = three_node({1});
    start_election(three_candidate);
    complete_all(three_candidate);
    const auto stepped_down =
        three_candidate.step(figure2::ReceiveRequestVoteResponse{
            .from = {2}, .response = {{7}, false}});
    expect(
        three_candidate.snapshot().role == RaftRole::follower
            && three_candidate.snapshot().hard_state.current_term == Term{7},
        "higher-term response must step a candidate down");
    expect(
        find_effect<RoleTransition>(stepped_down) != nullptr
            && find_effect<PersistRaftHardState>(stepped_down) != nullptr,
        "higher-term stepdown must explicitly transition and persist");
}

simulation::PendingEvent find_pending(
    const simulation::Simulator& simulator,
    const simulation::PendingEventKind kind,
    const NodeId to) {
    const auto pending = simulator.pending_events();
    const auto found = std::ranges::find_if(
        pending,
        [kind, to](const simulation::PendingEvent& event) {
            return event.kind == kind && event.to == to;
        });
    if (found == pending.end()) {
        throw TestFailure("expected simulation event was not pending");
    }
    return *found;
}

void simulator_crash_before_completion() {
    auto observations =
        std::make_shared<std::map<std::uint64_t, Snapshot>>();
    SimulationConfig config{
        .timeouts = {1, 1},
        .seed = 9,
        .observer =
            [observations](const NodeId node, const Snapshot& snapshot) {
                (*observations)[node.value] = snapshot;
            }};
    simulation::Simulator simulator(
        simulation::Simulator::three_node_topology(),
        15,
        make_simulation_factory(config));
    simulator.set_persistence_delay({1}, 10);
    for (const auto node : simulation::Simulator::three_node_topology()) {
        simulator.deliver(
            find_pending(
                simulator,
                simulation::PendingEventKind::start,
                node)
                .id);
    }
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::timer,
            {1})
            .id);
    expect(
        observations->at(1).waiting_for_persistence,
        "timeout must wait for delayed persistence");
    simulator.crash({1});
    simulator.restart({1});
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::start,
            {1})
            .id);
    expect(
        observations->at(1).hard_state == RaftHardState{},
        "crash before completion must reload only durable hard state");
}

void simulator_restart_reloads_completed_vote() {
    auto observations =
        std::make_shared<std::map<std::uint64_t, Snapshot>>();
    SimulationConfig config{
        .timeouts = {1, 1},
        .seed = 12,
        .observer =
            [observations](const NodeId node, const Snapshot& snapshot) {
                (*observations)[node.value] = snapshot;
            }};
    simulation::Simulator simulator(
        simulation::Simulator::three_node_topology(),
        16,
        make_simulation_factory(config));
    for (const auto node : simulation::Simulator::three_node_topology()) {
        simulator.deliver(
            find_pending(
                simulator,
                simulation::PendingEventKind::start,
                node)
                .id);
    }
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::timer,
            {2})
            .id);
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::disk_completion,
            {2})
            .id);
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::message,
            {1})
            .id);
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::disk_completion,
            {1})
            .id);
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::disk_completion,
            {1})
            .id);
    expect(
        observations->at(1).hard_state.voted_for == NodeId{2},
        "follower must record the completed vote");

    simulator.crash({1});
    simulator.restart({1});
    simulator.deliver(
        find_pending(
            simulator,
            simulation::PendingEventKind::start,
            {1})
            .id);
    expect(
        observations->at(1).hard_state
            == RaftHardState{
                .current_term = {1},
                .voted_for = NodeId{2}},
        "restart must reload the completed term and vote");
}

void simulator_elects_in_three_and_five_node_schedules() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations =
            std::make_shared<std::map<std::uint64_t, Snapshot>>();
        SimulationConfig config{
            .timeouts = {5, 25},
            .seed = 0x4567,
            .observer =
                [observations](const NodeId node, const Snapshot& snapshot) {
                    (*observations)[node.value] = snapshot;
                }};
        simulation::Simulator simulator(
            topology,
            0x9876,
            make_simulation_factory(config));
        bool elected = false;
        for (std::size_t steps = 0; steps < 500 && !elected; ++steps) {
            expect(simulator.step(), "election schedule ended without leader");
            elected = std::ranges::any_of(
                *observations,
                [](const auto& entry) {
                    return entry.second.role == RaftRole::leader;
                });
        }
        expect(elected, "deterministic odd-node schedule must elect a leader");
    }
}

}  // namespace

int main() {
    try {
        deterministic_deadlines_and_ordered_self_vote();
        split_vote_and_simultaneous_timeouts();
        vote_freshness_stale_terms_and_recovery();
        persistence_gates_higher_term_and_vote();
        duplicate_votes_quorums_and_higher_term_stepdown();
        simulator_crash_before_completion();
        simulator_restart_reloads_completed_vote();
        simulator_elects_in_three_and_five_node_schedules();
    } catch (const std::exception& error) {
        std::cerr << "raft election test failure: " << error.what() << '\n';
        return 1;
    }
    std::cout << "raft election tests passed\n";
    return 0;
}
