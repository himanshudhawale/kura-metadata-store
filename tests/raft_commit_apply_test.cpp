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
std::size_t count_effects(const StepResult& result) {
    return static_cast<std::size_t>(std::ranges::count_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<EffectType>(effect);
        }));
}

CommandEnvelope command(const std::uint64_t sequence) {
    return {
        .request_id = {.client = 13, .sequence = sequence},
        .type_tag = 23,
        .payload = {static_cast<std::uint8_t>(sequence)}};
}

LogEntry entry(
    const std::uint64_t index,
    const std::uint64_t term) {
    return {
        .term = {term},
        .index = {index},
        .command = command(index)};
}

Core core(
    const NodeId self,
    const std::uint64_t term = 0,
    std::vector<LogEntry> log = {},
    const std::size_t cluster_size = 3,
    const LogIndex recovered_applied = {}) {
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
        71,
        {10, 20},
        std::move(log),
        3,
        recovered_applied);
}

StepResult complete_storage(Core& node) {
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

void complete_all_storage(Core& node) {
    while (node.snapshot().waiting_for_persistence) {
        static_cast<void>(complete_storage(node));
    }
}

StepResult elect(Core& node, const std::size_t cluster_size = 3) {
    const auto started = node.start();
    const auto* timer = find_effect<ResetElectionDeadline>(started);
    expect(timer != nullptr, "start did not set an election deadline");
    static_cast<void>(node.step(ElectionDeadline{timer->timer_id}));
    complete_all_storage(node);
    StepResult elected;
    const auto term = node.snapshot().hard_state.current_term;
    for (std::uint64_t voter = 2;
         voter <= cluster_size / 2 + 1;
         ++voter) {
        elected = node.step(figure2::ReceiveRequestVoteResponse{
            .from = {voter}, .response = {term, true}});
    }
    expect(node.snapshot().role == RaftRole::leader, "election failed");
    return elected;
}

void propose(Core& leader, const std::uint64_t sequence) {
    const auto proposed = leader.step(
        figure2::ReceiveClientCommand{command(sequence)});
    expect(
        find_effect<PersistRaftLog>(proposed) != nullptr
            && find_effect<ApplyLogEntry>(proposed) == nullptr,
        "proposal must persist without applying");
    static_cast<void>(complete_storage(leader));
}

StepResult match(
    Core& leader,
    const NodeId peer,
    const std::uint64_t index) {
    return leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = peer,
        .response = {
            leader.snapshot().hard_state.current_term,
            true,
            {index}}});
}

StepResult apply(Core& node) {
    const auto* pending = node.pending_application();
    expect(pending != nullptr, "expected a pending application");
    return node.step(LogEntryApplied{
        .request_id = pending->request_id,
        .index = pending->entry.index});
}

void three_and_five_node_majorities() {
    auto three = core({1});
    static_cast<void>(elect(three));
    propose(three, 1);
    const auto committed = match(three, {2}, 1);
    expect(
        three.snapshot().commit_index == LogIndex{1}
            && find_effect<ApplyLogEntry>(committed)->entry.index
                == LogIndex{1},
        "self plus one of three must commit a current-term entry");

    auto five = core({1}, 0, {}, 5);
    static_cast<void>(elect(five, 5));
    propose(five, 1);
    auto minority = match(five, {2}, 1);
    expect(
        five.snapshot().commit_index == LogIndex{}
            && find_effect<ApplyLogEntry>(minority) == nullptr,
        "self plus one of five must not commit");
    const auto majority = match(five, {3}, 1);
    expect(
        five.snapshot().commit_index == LogIndex{1}
            && find_effect<ApplyLogEntry>(majority) != nullptr,
        "self plus two of five must commit");
}

void current_term_rule_and_indirect_prior_commit() {
    auto leader = core({1}, 2, {entry(1, 2)});
    static_cast<void>(elect(leader));
    const auto old_term = match(leader, {2}, 1);
    expect(
        leader.snapshot().commit_index == LogIndex{}
            && find_effect<ApplyLogEntry>(old_term) == nullptr,
        "majority replication must not directly commit an old-term entry");

    propose(leader, 2);
    const auto indirect = match(leader, {2}, 2);
    expect(
        leader.snapshot().commit_index == LogIndex{2}
            && find_effect<ApplyLogEntry>(indirect)->entry.index
                == LogIndex{1},
        "committing a current-term entry must indirectly commit its prefix");
    const auto second = apply(leader);
    expect(
        leader.snapshot().last_applied == LogIndex{1}
            && find_effect<ApplyLogEntry>(second)->entry.index
                == LogIndex{2},
        "indirectly committed prefix must apply in index order");
    static_cast<void>(apply(leader));
    expect(
        leader.snapshot().last_applied == LogIndex{2},
        "all indirectly committed entries must apply");
}

void follower_commit_is_bounded_by_last_new_entry() {
    auto follower = core(
        {2},
        3,
        {entry(1, 1), entry(2, 2), entry(3, 2)});
    static_cast<void>(follower.start());
    const auto heartbeat = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = {
            .term = {3},
            .leader = {1},
            .previous_log_index = {1},
            .previous_log_term = {1},
            .leader_commit = {3}}});
    expect(
        follower.snapshot().commit_index == LogIndex{1}
            && find_effect<ApplyLogEntry>(heartbeat)->entry.index
                == LogIndex{1},
        "follower commit must be bounded by the last entry in the request");
    static_cast<void>(apply(follower));
    const auto reordered = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = {
            .term = {3},
            .leader = {1},
            .leader_commit = {3}}});
    expect(
        follower.snapshot().commit_index == LogIndex{1}
            && follower.snapshot().last_applied == LogIndex{1}
            && find_effect<ApplyLogEntry>(reordered) == nullptr,
        "out-of-order heartbeat must not regress commit or replay apply");
}

void committed_prefix_cannot_be_rewritten() {
    auto follower = core(
        {2},
        3,
        {entry(1, 1), entry(2, 2)},
        3,
        LogIndex{1});
    static_cast<void>(follower.start());
    const auto rejected = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = {
            .term = {3},
            .leader = {1},
            .entries = {entry(1, 3)},
            .leader_commit = {1}}});
    expect(
        !find_effect<figure2::SendAppendEntriesResponse>(rejected)
             ->response.succeeded
            && find_effect<PersistRaftLog>(rejected) == nullptr
            && follower.snapshot().log
                == std::vector{entry(1, 1), entry(2, 2)}
            && follower.snapshot().last_applied == LogIndex{1},
        "conflict repair must never rewrite the committed prefix");
}

void changed_log_is_durable_before_apply() {
    auto follower = core({2}, 1);
    static_cast<void>(follower.start());
    const auto appended = follower.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = {
            .term = {1},
            .leader = {1},
            .entries = {entry(1, 1)},
            .leader_commit = {1}}});
    expect(
        find_effect<PersistRaftLog>(appended) != nullptr
            && find_effect<ApplyLogEntry>(appended) == nullptr
            && follower.snapshot().last_applied == LogIndex{},
        "follower must not apply an entry before log completion");
    const auto durable = complete_storage(follower);
    expect(
        find_effect<figure2::SendAppendEntriesResponse>(durable) != nullptr
            && find_effect<ApplyLogEntry>(durable)->entry.index
                == LogIndex{1},
        "durable completion must release response and apply");
}

void application_is_exactly_ordered_and_backpressured() {
    auto leader = core({1});
    static_cast<void>(elect(leader));
    propose(leader, 1);
    propose(leader, 2);
    auto committed = match(leader, {2}, 2);
    const auto first = *find_effect<ApplyLogEntry>(committed);
    expect(
        first.entry.index == LogIndex{1}
            && count_effects<ApplyLogEntry>(committed) == 1,
        "commit batch must expose exactly one ordered apply");

    const auto duplicate = match(leader, {2}, 2);
    expect(
        find_effect<ApplyLogEntry>(duplicate) == nullptr,
        "duplicate response must not duplicate an apply effect");
    expect_throws<std::invalid_argument>(
        [&] {
            static_cast<void>(leader.step(LogEntryApplied{
                .request_id = first.request_id,
                .index = {2}}));
        },
        "out-of-order application completion was accepted");

    const auto failed = leader.step(LogEntryApplyFailed{
        .request_id = first.request_id,
        .index = first.entry.index});
    expect(
        leader.snapshot().application_blocked
            && find_effect<ApplicationBackpressured>(failed)->index
                == LogIndex{1},
        "application failure must expose deterministic backpressure");
    const auto heartbeat = leader.step(
        HeartbeatDeadline{*leader.snapshot().heartbeat_timer});
    expect(
        find_effect<figure2::SendAppendEntries>(heartbeat) != nullptr,
        "application backpressure must not stop Raft protocol progress");

    const auto retried = leader.step(RetryApplication{});
    const auto retry = *find_effect<ApplyLogEntry>(retried);
    expect(
        retry.request_id != first.request_id
            && retry.entry == first.entry,
        "retry must correlate a new attempt for the same entry");
    const auto next = leader.step(LogEntryApplied{
        .request_id = retry.request_id,
        .index = retry.entry.index});
    expect(
        find_effect<figure2::CompleteClientCommand>(next)->index
                == LogIndex{1}
            && find_effect<ApplyLogEntry>(next)->entry.index == LogIndex{2},
        "successful retry must correlate the client and release the next entry");
    const auto second_applied = apply(leader);
    expect(
        leader.snapshot().last_applied == LogIndex{2}
            && !leader.snapshot().application_pending
            && find_effect<figure2::CompleteClientCommand>(second_applied)
                   ->index
                == LogIndex{2},
        "ordered application must drain with correlated client completions");
}

void application_survives_leadership_change() {
    auto leader = core({1});
    static_cast<void>(elect(leader));
    propose(leader, 1);
    static_cast<void>(match(leader, {2}, 1));
    const auto pending = *leader.pending_application();
    const auto stepped_down =
        leader.step(figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {{8}, false, {}}});
    expect(
        leader.snapshot().role == RaftRole::follower
            && leader.snapshot().application_pending
            && find_effect<PersistRaftHardState>(stepped_down) != nullptr,
        "leader step-down must retain committed application work");
    const auto applied = leader.step(LogEntryApplied{
        .request_id = pending.request_id,
        .index = pending.entry.index});
    static_cast<void>(applied);
    expect(
        leader.snapshot().last_applied == LogIndex{1},
        "committed work must apply after leadership loss");
}

void recovery_skips_represented_applied_state() {
    auto recovered = core(
        {2},
        3,
        {entry(1, 1), entry(2, 2)},
        3,
        LogIndex{1});
    const auto started = recovered.start();
    expect(
        recovered.snapshot().commit_index == LogIndex{1}
            && recovered.snapshot().last_applied == LogIndex{1}
            && find_effect<ApplyLogEntry>(started) == nullptr,
        "restart must not replay represented applied state");
    const auto advanced = recovered.step(figure2::ReceiveAppendEntries{
        .from = {1},
        .request = {
            .term = {3},
            .leader = {1},
            .previous_log_index = {2},
            .previous_log_term = {2},
            .leader_commit = {2}}});
    expect(
        find_effect<ApplyLogEntry>(advanced)->entry.index == LogIndex{2},
        "restart must resume at the first unapplied committed index");
}

simulation::PendingEvent pending_event(
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

SimulationConfig simulation_config(
    const std::shared_ptr<std::map<std::uint64_t, Snapshot>>& observations) {
    return {
        .timeouts = {50, 80},
        .seed = 0x1357,
        .heartbeat_interval = 3,
        .commands_on_leadership = {command(1)},
        .observer =
            [observations](const NodeId node, const Snapshot& snapshot) {
                (*observations)[node.value] = snapshot;
            }};
}

void simulator_applies_on_three_and_five_nodes() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations =
            std::make_shared<std::map<std::uint64_t, Snapshot>>();
        simulation::Simulator simulator(
            topology,
            0x2468,
            make_simulation_factory(simulation_config(observations)));
        bool applied_everywhere = false;
        for (std::size_t steps = 0;
             steps < 5'000 && !applied_everywhere;
             ++steps) {
            expect(simulator.step(), "apply schedule stopped early");
            applied_everywhere =
                observations->size() == topology.size()
                && std::ranges::all_of(
                    *observations,
                    [](const auto& observed) {
                        return observed.second.last_applied == LogIndex{1};
                    });
        }
        expect(
            applied_everywhere,
            "odd-node schedule must commit and apply on every follower");
    }
}

void partitioned_minority_cannot_commit() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations =
            std::make_shared<std::map<std::uint64_t, Snapshot>>();
        simulation::Simulator simulator(
            topology,
            0x9753,
            make_simulation_factory(simulation_config(observations)));
        NodeId leader;
        for (std::size_t steps = 0; steps < 1'000 && leader == NodeId{};
             ++steps) {
            expect(simulator.step(), "minority schedule stopped early");
            for (const auto& [id, snapshot] : *observations) {
                if (snapshot.role == RaftRole::leader) {
                    leader = {id};
                }
            }
        }
        expect(leader != NodeId{}, "minority schedule elected no leader");

        std::vector<NodeId> minority{leader};
        if (topology.size() == 5) {
            const auto companion = std::ranges::find_if(
                topology,
                [leader](const NodeId node) {
                    return node != leader;
                });
            minority.push_back(*companion);
        }
        for (const auto first : minority) {
            for (const auto second : topology) {
                if (std::ranges::find(minority, second) == minority.end()) {
                    simulator.partition(first, second);
                }
            }
        }
        for (std::size_t steps = 0; steps < 100; ++steps) {
            expect(simulator.step(), "partitioned schedule stopped early");
        }
        expect(
            observations->at(leader.value).commit_index == LogIndex{}
                && observations->at(leader.value).last_applied == LogIndex{},
            "partitioned minority leader must not commit or apply");
    }
}

void simulator_restart_preserves_applied_marker() {
    auto observations =
        std::make_shared<std::map<std::uint64_t, Snapshot>>();
    simulation::Simulator simulator(
        simulation::Simulator::three_node_topology(),
        0x8642,
        make_simulation_factory(simulation_config(observations)));
    NodeId applied_node;
    for (std::size_t steps = 0; steps < 5'000 && applied_node == NodeId{};
         ++steps) {
        expect(simulator.step(), "recovery schedule stopped early");
        for (const auto& [id, snapshot] : *observations) {
            if (snapshot.last_applied == LogIndex{1}) {
                applied_node = {id};
            }
        }
    }
    expect(applied_node != NodeId{}, "no node completed application");
    simulator.crash(applied_node);
    simulator.restart(applied_node);
    simulator.deliver(
        pending_event(
            simulator,
            simulation::PendingEventKind::start,
            applied_node)
            .id);
    expect(
        observations->at(applied_node.value).last_applied == LogIndex{1}
            && !observations->at(applied_node.value).application_pending,
        "restart must reload applied state without replay");
}

}  // namespace

int main() {
    try {
        three_and_five_node_majorities();
        current_term_rule_and_indirect_prior_commit();
        follower_commit_is_bounded_by_last_new_entry();
        committed_prefix_cannot_be_rewritten();
        changed_log_is_durable_before_apply();
        application_is_exactly_ordered_and_backpressured();
        application_survives_leadership_change();
        recovery_skips_represented_applied_state();
        simulator_applies_on_three_and_five_nodes();
        partitioned_minority_cannot_commit();
        simulator_restart_preserves_applied_marker();
    } catch (const std::exception& error) {
        std::cerr << "raft commit/apply test failure: "
                  << error.what() << '\n';
        return 1;
    }
    std::cout << "raft commit/apply tests passed\n";
    return 0;
}
