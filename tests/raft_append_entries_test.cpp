#include "kura/metadata/raft/election.hpp"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
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
std::vector<EffectType> effects(const StepResult& result) {
    std::vector<EffectType> selected;
    for (const auto& effect : result.effects) {
        if (const auto* typed = std::get_if<EffectType>(&effect)) {
            selected.push_back(*typed);
        }
    }
    return selected;
}

CommandEnvelope command(const std::uint64_t sequence) {
    return {
        .request_id = {.client = 7, .sequence = sequence},
        .type_tag = 19,
        .payload = {
            static_cast<std::uint8_t>(sequence),
            static_cast<std::uint8_t>(sequence + 1)}};
}

LogEntry entry(
    const std::uint64_t index,
    const std::uint64_t term,
    const std::uint64_t sequence = 0) {
    return {
        .term = {term},
        .index = {index},
        .command = command(sequence == 0 ? index : sequence)};
}

Core core(
    const NodeId self,
    const std::uint64_t term = 0,
    std::vector<LogEntry> log = {},
    const std::size_t cluster_size = 3) {
    std::vector<NodeId> peers;
    for (std::uint64_t id = 1; id <= cluster_size; ++id) {
        if (NodeId{id} != self) {
            peers.push_back(NodeId{id});
        }
    }
    return Core(
        self,
        std::move(peers),
        {.current_term = {term}},
        42,
        {10, 20},
        std::move(log),
        3);
}

StepResult complete_one(Core& node) {
    if (const auto* pending = node.pending_log_persistence()) {
        return node.step(RaftLogPersisted{
            .request_id = pending->request_id,
            .log = pending->log});
    }
    const auto& pending = *node.specification_state().pending_hard_state;
    return node.step(RaftHardStatePersisted{
        .request_id = pending.request.request_id,
        .state = pending.request.state});
}

void complete_all(Core& node) {
    while (node.snapshot().waiting_for_persistence) {
        static_cast<void>(complete_one(node));
    }
}

StepResult elect(Core& node, const std::size_t cluster_size = 3) {
    const auto started = node.start();
    const auto* timer = find_effect<ResetElectionDeadline>(started);
    expect(timer != nullptr, "node start did not set election timer");
    static_cast<void>(node.step(ElectionDeadline{timer->timer_id}));
    complete_all(node);
    StepResult result;
    const auto term = node.snapshot().hard_state.current_term;
    for (std::uint64_t voter = 2;
         voter <= cluster_size / 2 + 1;
         ++voter) {
        result = node.step(figure2::ReceiveRequestVoteResponse{
            .from = {voter}, .response = {term, true}});
    }
    expect(node.snapshot().role == RaftRole::leader, "election failed");
    return result;
}

AppendEntriesRequest append(
    const std::uint64_t term,
    const std::uint64_t leader,
    const std::uint64_t previous_index,
    const std::uint64_t previous_term,
    std::vector<LogEntry> entries = {}) {
    return {
        .term = {term},
        .leader = {leader},
        .previous_log_index = {previous_index},
        .previous_log_term = {previous_term},
        .entries = std::move(entries),
        .leader_commit = {}};
}

void empty_log_initialization_and_heartbeats() {
    auto leader = core({1});
    const auto elected = elect(leader);
    const auto initial = effects<figure2::SendAppendEntries>(elected);
    expect(initial.size() == 2, "leader must initialize every peer");
    for (const auto& send : initial) {
        expect(
            send.request.previous_log_index == LogIndex{}
                && send.request.previous_log_term == Term{}
                && send.request.entries.empty(),
            "empty-log initial AppendEntries must be a heartbeat");
        const auto progress = leader.snapshot().peer_progress.at(send.to);
        expect(
            progress.next_index == LogIndex{1}
                && progress.match_index == LogIndex{},
            "empty-log peer progress must initialize at one/zero");
    }

    const auto heartbeat_timer = *leader.snapshot().heartbeat_timer;
    const auto heartbeat =
        leader.step(HeartbeatDeadline{heartbeat_timer});
    expect(
        effects<figure2::SendAppendEntries>(heartbeat).size() == 2
            && find_effect<ResetHeartbeatDeadline>(heartbeat) != nullptr,
        "heartbeat timeout must send to every peer and reschedule");
}

void local_append_waits_for_persistence() {
    auto leader = core({1});
    static_cast<void>(elect(leader));
    const auto proposed = leader.step(
        figure2::ReceiveClientCommand{command(1)});
    expect(
        find_effect<PersistRaftLog>(proposed) != nullptr
            && effects<figure2::SendAppendEntries>(proposed).empty(),
        "leader must not replicate an unpersisted local entry");
    expect(
        leader.snapshot().log == std::vector{entry(1, 1)},
        "leader must create a typed current-term entry");
    const auto released = complete_one(leader);
    const auto sends = effects<figure2::SendAppendEntries>(released);
    expect(
        sends.size() == 2
            && sends.front().request.entries == std::vector{entry(1, 1)},
        "durable local append must release replication");
}

void follower_conflict_truncation_duplicates_and_ordering() {
    auto follower = core(
        {2},
        3,
        {entry(1, 1), entry(2, 2), entry(3, 2)});
    static_cast<void>(follower.start());
    const auto request = append(
        3,
        1,
        1,
        1,
        {entry(2, 3, 20), entry(3, 3, 30)});
    const auto changed = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = request});
    expect(
        find_effect<PersistRaftLog>(changed) != nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(changed)
                == nullptr,
        "changed follower log must persist before success");
    expect(
        follower.snapshot().log
            == std::vector{
                entry(1, 1),
                entry(2, 3, 20),
                entry(3, 3, 30)},
        "conflicting suffix must be strictly truncated and replaced");
    const auto completed = complete_one(follower);
    expect(
        find_effect<figure2::SendAppendEntriesResponse>(completed)
            ->response.succeeded,
        "durable replacement must release success");

    const auto duplicate = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = request});
    expect(
        find_effect<PersistRaftLog>(duplicate) == nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(duplicate)
                   ->response.succeeded,
        "duplicate AppendEntries must be an idempotent immediate success");

    const auto old_log = follower.snapshot().log;
    const auto out_of_order = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = append(3, 1, 4, 3, {entry(5, 3)})});
    expect(
        !find_effect<figure2::SendAppendEntriesResponse>(out_of_order)
             ->response.succeeded
            && follower.snapshot().log == old_log,
        "request beyond the follower suffix must fail without mutation");
}

void malformed_term_and_index_sequences_are_rejected() {
    auto follower = core({2}, 3);
    static_cast<void>(follower.start());
    expect_throws<std::invalid_argument>(
        [&] {
            static_cast<void>(
                follower.step(figure2::ReceiveAppendEntries{
                    .from = {1},
                    .request = append(
                        3, 1, 0, 0, {entry(2, 3)})}));
        },
        "AppendEntries with an index gap was accepted");
    expect_throws<std::invalid_argument>(
        [&] {
            static_cast<void>(
                follower.step(figure2::ReceiveAppendEntries{
                    .from = {1},
                    .request = append(
                        3, 1, 0, 0, {entry(1, 4)})}));
        },
        "AppendEntries with a future entry term was accepted");
    expect_throws<std::invalid_argument>(
        [&] {
            static_cast<void>(
                follower.step(figure2::ReceiveAppendEntries{
                    .from = {1},
                    .request = append(3, 1, 0, 1)}));
        },
        "AppendEntries with a nonzero term at index zero was accepted");
}

void stale_and_higher_term_requests_are_ordered() {
    auto follower = core({2}, 2);
    static_cast<void>(follower.start());
    const auto stale = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = append(1, 1, 0, 0)});
    expect(
        !find_effect<figure2::SendAppendEntriesResponse>(stale)
             ->response.succeeded
            && find_effect<PersistRaftHardState>(stale) == nullptr,
        "stale leader must be rejected without persistence");

    const auto higher = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = append(4, 1, 0, 0, {entry(1, 4)})});
    expect(
        find_effect<PersistRaftHardState>(higher) != nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(higher)
                == nullptr,
        "higher term must persist before AppendEntries processing responds");
    const auto hard_completed = complete_one(follower);
    expect(
        find_effect<PersistRaftLog>(hard_completed) != nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(
                   hard_completed)
                == nullptr,
        "changed log must have a second explicit completion");
    const auto log_completed = complete_one(follower);
    expect(
        find_effect<figure2::SendAppendEntriesResponse>(log_completed)
            ->response.succeeded,
        "success must follow durable higher term and log");
}

void leader_backtracks_and_does_not_commit() {
    auto leader = core(
        {1},
        2,
        {entry(1, 1), entry(2, 2), entry(3, 2)});
    static_cast<void>(elect(leader));
    const auto term = leader.snapshot().hard_state.current_term;
    expect(
        leader.snapshot().peer_progress.at({2}).next_index == LogIndex{4},
        "leader nextIndex must begin after its last entry");

    auto retry = leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = {term, false, {}}});
    auto send = find_effect<figure2::SendAppendEntries>(retry);
    expect(
        send->request.previous_log_index == LogIndex{2}
            && send->request.entries == std::vector{entry(3, 2)},
        "first rejection must backtrack one index");
    retry = leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = {term, false, {}}});
    send = find_effect<figure2::SendAppendEntries>(retry);
    expect(
        send->request.previous_log_index == LogIndex{1}
            && send->request.entries
                == std::vector{entry(2, 2), entry(3, 2)},
        "repeated rejection must deterministically backtrack and resend");

    static_cast<void>(leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = {term, true, {3}}}));
    expect(
        leader.snapshot().peer_progress.at({2}).match_index == LogIndex{3}
            && leader.specification_state().volatile_state.commit_index
                == LogIndex{},
        "old-term match must update progress without direct commitment");
}

void lagging_follower_converges_under_reordered_requests() {
    auto leader = core(
        {1},
        2,
        {entry(1, 1), entry(2, 2), entry(3, 2)});
    const auto elected = elect(leader);
    const auto term = leader.snapshot().hard_state.current_term;
    const auto initial_sends = effects<figure2::SendAppendEntries>(elected);
    const auto initial = std::ranges::find(
        initial_sends, NodeId{2}, &figure2::SendAppendEntries::to);
    expect(initial != initial_sends.end(), "initial send to follower missing");

    auto follower = core(
        {2},
        term.value,
        {entry(1, 1), entry(2, 1, 200)});
    static_cast<void>(follower.start());
    auto rejected = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = initial->request});
    expect(
        !find_effect<figure2::SendAppendEntriesResponse>(rejected)
             ->response.succeeded,
        "lagging follower must reject a missing previous index");

    auto retry = leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = {term, false, {}}});
    auto request = find_effect<figure2::SendAppendEntries>(retry)->request;
    rejected = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = request});
    expect(
        !find_effect<figure2::SendAppendEntriesResponse>(rejected)
             ->response.succeeded,
        "divergent follower must reject the wrong previous term");

    retry = leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = {term, false, {}}});
    request = find_effect<figure2::SendAppendEntries>(retry)->request;
    auto repairing = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = request});
    expect(
        find_effect<PersistRaftLog>(repairing) != nullptr,
        "matching earlier prefix must begin durable repair");
    const auto repaired = complete_one(follower);
    const auto success =
        find_effect<figure2::SendAppendEntriesResponse>(repaired)->response;
    static_cast<void>(leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = {2}, .response = success}));
    expect(
        follower.snapshot().log == leader.snapshot().log
            && leader.snapshot().peer_progress.at({2}).match_index
                == LogIndex{3},
        "backtracking must converge the divergent follower");

    const auto reordered = follower.step(figure2::ReceiveAppendEntries{
        .from = {1}, .request = initial->request});
    expect(
        find_effect<PersistRaftLog>(reordered) == nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(reordered)
                   ->response.succeeded
            && follower.snapshot().log == leader.snapshot().log,
        "late initial heartbeat must be harmless after convergence");
}

void higher_term_response_steps_leader_down() {
    auto leader = core({1});
    static_cast<void>(elect(leader));
    const auto result =
        leader.step(figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {{9}, false, {}}});
    expect(
        leader.snapshot().role == RaftRole::follower
            && leader.snapshot().hard_state.current_term == Term{9}
            && find_effect<PersistRaftHardState>(result) != nullptr
            && find_effect<CancelHeartbeatDeadline>(result) != nullptr,
        "higher-term replication response must step down and persist");
}

simulation::PendingEvent pending(
    const simulation::Simulator& simulator,
    const simulation::PendingEventKind kind,
    const NodeId to) {
    const auto events = simulator.pending_events();
    const auto found = std::ranges::find_if(
        events,
        [kind, to](const simulation::PendingEvent& event) {
            return event.kind == kind && event.to == to;
        });
    if (found == events.end()) {
        throw TestFailure("expected simulator event was not pending");
    }
    return *found;
}

void simulator_replicates_on_odd_cluster_schedules() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations =
            std::make_shared<std::map<std::uint64_t, Snapshot>>();
        SimulationConfig config{
            .timeouts = {5, 25},
            .seed = 0x1234,
            .heartbeat_interval = 2,
            .commands_on_leadership = {command(99)},
            .observer =
                [observations](const NodeId node, const Snapshot& snapshot) {
                    (*observations)[node.value] = snapshot;
                }};
        simulation::Simulator simulator(
            topology,
            0x5678,
            make_simulation_factory(config));
        bool replicated = false;
        for (std::size_t steps = 0; steps < 2'000 && !replicated; ++steps) {
            expect(simulator.step(), "replication schedule stopped early");
            replicated =
                observations->size() == topology.size()
                && std::ranges::all_of(
                    *observations,
                    [](const auto& observed) {
                        return observed.second.log.size() == 1
                            && !observed.second.waiting_for_persistence;
                    });
        }
        expect(
            replicated,
            "deterministic odd-cluster schedule must replicate one entry");
    }
}

void simulator_crash_discards_uncompleted_log() {
    auto observations =
        std::make_shared<std::map<std::uint64_t, Snapshot>>();
    SimulationConfig config{
        .timeouts = {5, 25},
        .seed = 0x8888,
        .heartbeat_interval = 2,
        .commands_on_leadership = {command(77)},
        .observer =
            [observations](const NodeId node, const Snapshot& snapshot) {
                (*observations)[node.value] = snapshot;
            }};
    simulation::Simulator simulator(
        simulation::Simulator::three_node_topology(),
        0x9999,
        make_simulation_factory(config));

    NodeId leader;
    for (std::size_t steps = 0; steps < 500 && leader == NodeId{}; ++steps) {
        expect(simulator.step(), "leader election schedule stopped early");
        for (const auto& [id, snapshot] : *observations) {
            if (snapshot.role == RaftRole::leader) {
                leader = {id};
            }
        }
    }
    expect(leader != NodeId{}, "simulator did not elect a leader");
    const NodeId follower = leader == NodeId{1} ? NodeId{2} : NodeId{1};
    simulator.set_persistence_delay(follower, 20);

    bool append_pending = false;
    for (std::size_t steps = 0; steps < 500 && !append_pending; ++steps) {
        expect(simulator.step(), "append schedule stopped early");
        const auto& snapshot = observations->at(follower.value);
        append_pending =
            snapshot.log.size() == 1 && snapshot.waiting_for_persistence;
    }
    expect(append_pending, "follower never reached delayed log persistence");
    simulator.crash(follower);
    simulator.restart(follower);
    simulator.deliver(
        pending(simulator, simulation::PendingEventKind::start, follower).id);
    expect(
        observations->at(follower.value).log.empty(),
        "restart must discard an AppendEntries log without completion");
}

}  // namespace

int main() {
    try {
        empty_log_initialization_and_heartbeats();
        local_append_waits_for_persistence();
        follower_conflict_truncation_duplicates_and_ordering();
        malformed_term_and_index_sequences_are_rejected();
        stale_and_higher_term_requests_are_ordered();
        leader_backtracks_and_does_not_commit();
        lagging_follower_converges_under_reordered_requests();
        higher_term_response_steps_leader_down();
        simulator_replicates_on_odd_cluster_schedules();
        simulator_crash_discards_uncompleted_log();
    } catch (const std::exception& error) {
        std::cerr << "raft AppendEntries test failure: "
                  << error.what() << '\n';
        return 1;
    }
    std::cout << "raft AppendEntries tests passed\n";
    return 0;
}
