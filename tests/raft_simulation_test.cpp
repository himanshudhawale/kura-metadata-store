#include "kura/metadata/raft/simulation.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

namespace simulation = kura::metadata::raft::simulation;
using kura::metadata::NodeId;
using simulation::Bytes;
using simulation::CancelTimerAction;
using simulation::DurableRecord;
using simulation::MessageEvent;
using simulation::NodeAction;
using simulation::NodeAdapter;
using simulation::NodeEvent;
using simulation::PendingEvent;
using simulation::PendingEventKind;
using simulation::PersistAction;
using simulation::PersistedEvent;
using simulation::SendAction;
using simulation::SetTimerAction;
using simulation::SimulationFailure;
using simulation::Simulator;
using simulation::StartEvent;
using simulation::TimerEvent;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

struct ProbeObservation {
    std::size_t starts{};
    std::size_t peer_count{};
    std::vector<std::pair<std::uint64_t, Bytes>> messages;
    std::vector<std::uint64_t> persisted;
    std::vector<std::uint64_t> timers;
    std::vector<DurableRecord> recovered;

    bool operator==(const ProbeObservation&) const = default;
};

struct ProbePlan {
    bool send_on_start{};
    bool persist_on_start{};
    bool timer_on_start{};
    bool throw_on_start{};
};

using Observations =
    std::shared_ptr<std::map<std::uint64_t, ProbeObservation>>;

class DeterministicProbeNode final : public NodeAdapter {
public:
    DeterministicProbeNode(
        const NodeId id,
        Observations observations,
        ProbePlan plan)
        : id_(id),
          observations_(std::move(observations)),
          plan_(plan) {}

    std::vector<NodeAction> step(const NodeEvent& event) override {
        return std::visit(
            [this](const auto& typed) {
                return handle(typed);
            },
            event);
    }

private:
    std::vector<NodeAction> handle(const StartEvent& event) {
        if (plan_.throw_on_start) {
            throw std::runtime_error("probe failure");
        }
        auto& observation = observations_->at(id_.value);
        ++observation.starts;
        observation.peer_count = event.peers.size();
        observation.recovered = event.durable_records;

        std::vector<NodeAction> actions;
        if (plan_.send_on_start) {
            for (const auto peer : event.peers) {
                actions.emplace_back(SendAction{
                    .to = peer,
                    .bytes = {
                        static_cast<std::uint8_t>(id_.value),
                        static_cast<std::uint8_t>(peer.value)}});
            }
        }
        if (plan_.persist_on_start) {
            actions.emplace_back(PersistAction{
                .request_id = 100 + id_.value,
                .bytes = {static_cast<std::uint8_t>(id_.value)}});
        }
        if (plan_.timer_on_start) {
            actions.emplace_back(SetTimerAction{
                .timer_id = 200 + id_.value,
                .delay = id_.value});
            actions.emplace_back(CancelTimerAction{
                .timer_id = 900 + id_.value});
        }
        return actions;
    }

    std::vector<NodeAction> handle(const MessageEvent& event) {
        observations_->at(id_.value)
            .messages.emplace_back(event.from.value, event.bytes);
        return {};
    }

    std::vector<NodeAction> handle(const PersistedEvent& event) {
        observations_->at(id_.value).persisted.push_back(event.request_id);
        return {};
    }

    std::vector<NodeAction> handle(const TimerEvent& event) {
        observations_->at(id_.value).timers.push_back(event.timer_id);
        return {};
    }

    NodeId id_;
    Observations observations_;
    ProbePlan plan_;
};

struct ProbeFixture {
    Observations observations{
        std::make_shared<std::map<std::uint64_t, ProbeObservation>>()};
    std::map<std::uint64_t, ProbePlan> plans;

    simulation::NodeFactory factory() {
        const auto captured_observations = observations;
        const auto captured_plans = plans;
        return [captured_observations, captured_plans](const NodeId id) {
            captured_observations->try_emplace(id.value);
            const auto found = captured_plans.find(id.value);
            const auto plan =
                found == captured_plans.end() ? ProbePlan{} : found->second;
            return std::make_unique<DeterministicProbeNode>(
                id,
                captured_observations,
                plan);
        };
    }
};

PendingEvent find_event(
    const Simulator& simulator,
    const PendingEventKind kind,
    const NodeId to) {
    const auto pending = simulator.pending_events();
    const auto found = std::find_if(
        pending.begin(),
        pending.end(),
        [kind, to](const PendingEvent& event) {
            return event.kind == kind && event.to == to;
        });
    if (found == pending.end()) {
        throw TestFailure("expected pending event was not found");
    }
    return *found;
}

void deliver_start(Simulator& simulator, const NodeId node) {
    simulator.deliver(
        find_event(simulator, PendingEventKind::start, node).id);
}

void test_supported_topologies() {
    ProbeFixture three;
    Simulator three_nodes(
        Simulator::three_node_topology(),
        10,
        three.factory());
    three_nodes.run();
    expect(three.observations->size() == 3, "three-node topology size");
    for (const auto& [id, observation] : *three.observations) {
        static_cast<void>(id);
        expect(observation.starts == 1, "three-node start count");
        expect(observation.peer_count == 2, "three-node peer count");
    }

    ProbeFixture five;
    Simulator five_nodes(
        Simulator::five_node_topology(),
        11,
        five.factory());
    five_nodes.run();
    expect(five.observations->size() == 5, "five-node topology size");
    for (const auto& [id, observation] : *five.observations) {
        static_cast<void>(id);
        expect(observation.peer_count == 4, "five-node peer count");
    }
}

void test_seed_history_and_replay() {
    auto make_fixture = [] {
        ProbeFixture fixture;
        fixture.plans[1] = {
            .send_on_start = true,
            .persist_on_start = true,
            .timer_on_start = true};
        fixture.plans[2] = {.send_on_start = true};
        return fixture;
    };

    auto first_fixture = make_fixture();
    Simulator first(
        Simulator::three_node_topology(),
        0x12345678,
        first_fixture.factory());
    first.set_network_delay({1}, {2}, 3);
    first.set_persistence_delay({1}, 2);
    first.run();

    auto second_fixture = make_fixture();
    Simulator second(
        Simulator::three_node_topology(),
        0x12345678,
        second_fixture.factory());
    second.set_network_delay({1}, {2}, 3);
    second.set_persistence_delay({1}, 2);
    second.run();

    expect(first.trace() == second.trace(), "same seed must reproduce history");
    expect(
        *first_fixture.observations == *second_fixture.observations,
        "same schedule must reproduce observations");

    auto replay_fixture = make_fixture();
    auto replayed = Simulator::replay(first.trace(), replay_fixture.factory());
    expect(replayed.trace() == first.trace(), "trace replay must be exact");
    expect(
        *replay_fixture.observations == *first_fixture.observations,
        "trace replay must reproduce node observations");

    auto different_fixture = make_fixture();
    Simulator different(
        Simulator::three_node_topology(),
        0x87654321,
        different_fixture.factory());
    different.set_network_delay({1}, {2}, 3);
    different.set_persistence_delay({1}, 2);
    different.run();
    const auto first_history =
        first.trace().substr(first.trace().find('\n') + 1);
    const auto different_history =
        different.trace().substr(different.trace().find('\n') + 1);
    expect(
        different_history != first_history,
        "different seeds should select a different event history");
}

void test_network_faults_and_reordering() {
    ProbeFixture partition_fixture;
    partition_fixture.plans[1] = {.send_on_start = true};
    Simulator partitioned(
        Simulator::three_node_topology(),
        20,
        partition_fixture.factory());
    partitioned.partition({1}, {2});
    deliver_start(partitioned, {1});
    partitioned.run();
    expect(
        partition_fixture.observations->at(2).messages.empty(),
        "partition must drop delivery");
    expect(
        partition_fixture.observations->at(3).messages.size() == 1,
        "partition must not affect another link");

    ProbeFixture fault_fixture;
    fault_fixture.plans[1] = {.send_on_start = true};
    Simulator faults(
        Simulator::three_node_topology(),
        21,
        fault_fixture.factory());
    faults.drop_next({1}, {2});
    faults.duplicate_next({1}, {3}, 2);
    deliver_start(faults, {1});
    faults.run();
    expect(
        fault_fixture.observations->at(2).messages.empty(),
        "drop-next must suppress one message");
    expect(
        fault_fixture.observations->at(3).messages.size() == 3,
        "duplication must create the requested extra copies");

    ProbeFixture reorder_fixture;
    reorder_fixture.plans[1] = {.send_on_start = true};
    Simulator reordered(
        Simulator::three_node_topology(),
        22,
        reorder_fixture.factory());
    reordered.set_network_delay({1}, {2}, 10);
    deliver_start(reordered, {1});
    const auto to_three =
        find_event(reordered, PendingEventKind::message, {3});
    reordered.delay_message(to_three.id, 20);
    deliver_start(reordered, {2});
    deliver_start(reordered, {3});
    expect(reordered.step(), "delayed message to node 2 should be pending");
    expect(reordered.now() == 10, "network delay must use logical time");
    expect(
        reorder_fixture.observations->at(2).messages.size() == 1,
        "node 2 must receive the earlier delayed message");
    expect(reordered.step(), "reordered message to node 3 should be pending");
    expect(reordered.now() == 20, "message delay must reorder delivery");
}

void test_delayed_persistence_crash_and_restart() {
    ProbeFixture fixture;
    fixture.plans[1] = {.persist_on_start = true};
    Simulator simulator(
        Simulator::three_node_topology(),
        30,
        fixture.factory());
    simulator.set_persistence_delay({1}, 10);
    deliver_start(simulator, {1});
    expect(
        simulator.durable_records({1}).empty(),
        "write must not be durable before completion");
    simulator.crash({1});
    expect(!simulator.running({1}), "crash must stop the node");
    simulator.restart({1});
    deliver_start(simulator, {1});
    expect(
        fixture.observations->at(1).recovered.empty(),
        "crash must discard an incomplete persistence request");

    deliver_start(simulator, {2});
    deliver_start(simulator, {3});
    simulator.run();
    expect(simulator.now() == 10, "disk completion must use logical time");
    expect(
        simulator.durable_records({1}).size() == 1,
        "completed write must become durable");
    expect(
        fixture.observations->at(1).persisted ==
            std::vector<std::uint64_t>{101},
        "node must receive persistence completion");

    simulator.crash({1});
    simulator.restart({1});
    deliver_start(simulator, {1});
    expect(
        fixture.observations->at(1).recovered ==
            std::vector<DurableRecord>{
                {.request_id = 101, .bytes = {1}}},
        "restart must expose completed durable records");
}

void test_timers_crash_generation_and_failure_trace() {
    ProbeFixture timer_fixture;
    timer_fixture.plans[1] = {.timer_on_start = true};
    Simulator timers(
        Simulator::three_node_topology(),
        40,
        timer_fixture.factory());
    deliver_start(timers, {1});
    deliver_start(timers, {2});
    deliver_start(timers, {3});
    timers.run();
    expect(timers.now() == 1, "timer must advance logical time only");
    expect(
        timer_fixture.observations->at(1).timers ==
            std::vector<std::uint64_t>{201},
        "timer must fire deterministically");

    ProbeFixture stale_fixture;
    stale_fixture.plans[1] = {.send_on_start = true};
    Simulator stale(
        Simulator::three_node_topology(),
        41,
        stale_fixture.factory());
    deliver_start(stale, {1});
    stale.crash({2});
    stale.restart({2});
    stale.run();
    expect(
        stale_fixture.observations->at(2).messages.empty(),
        "pre-crash message must not enter a restarted node generation");

    bool saw_trace = false;
    try {
        stale.require(false, "probe invariant failed");
    } catch (const SimulationFailure& failure) {
        const std::string text = failure.what();
        saw_trace =
            text.contains("probe invariant failed") &&
            text.contains("sim-v1 seed 41 nodes 1 2 3") &&
            text.contains("control crash 2") &&
            text.contains("control restart 2");
    }
    expect(saw_trace, "invariant failure must include a replayable trace");

    ProbeFixture throwing_fixture;
    throwing_fixture.plans[1] = {.throw_on_start = true};
    Simulator throwing(
        Simulator::three_node_topology(),
        42,
        throwing_fixture.factory());
    bool wrapped = false;
    try {
        deliver_start(throwing, {1});
    } catch (const SimulationFailure& failure) {
        wrapped = std::string(failure.what()).contains(
            "node adapter threw: probe failure");
    }
    expect(wrapped, "adapter exceptions must include simulation context");
}

}  // namespace

int main() {
    try {
        test_supported_topologies();
        test_seed_history_and_replay();
        test_network_faults_and_reordering();
        test_delayed_persistence_crash_and_restart();
        test_timers_crash_generation_and_failure_trace();
    } catch (const std::exception& error) {
        std::cerr << "raft simulation test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "raft simulation tests passed\n";
    return 0;
}
