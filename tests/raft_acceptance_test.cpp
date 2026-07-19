#include "kura/metadata/raft/election.hpp"
#include "kura/metadata/testing/linearizability.hpp"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace kura::metadata;
using namespace kura::metadata::raft::election;
namespace simulation = kura::metadata::raft::simulation;
namespace lin = kura::metadata::testing::linearizability;

class TestFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void expect(const bool condition, const std::string& message) {
    if (!condition) {
        throw TestFailure(message);
    }
}

CommandEnvelope command(const std::uint64_t sequence) {
    return {
        .request_id = {.client = 600, .sequence = sequence},
        .type_tag = static_cast<std::uint32_t>(100 + sequence),
        .payload = {static_cast<std::uint8_t>(sequence)}};
}

ClusterMembership membership(const std::size_t count) {
    ClusterMembership result;
    for (std::uint64_t id = 1; id <= count; ++id) {
        result.voters.push_back({id});
    }
    return result;
}

struct Evidence {
    std::map<std::uint64_t, Snapshot> latest;
    std::map<std::uint64_t, std::vector<LogIndex>> apply_effects;
    std::set<std::uint64_t> completed_commands;
    std::vector<std::pair<NodeId, ReadIndexResponse>> read_responses;
    std::vector<std::pair<NodeId, ReadIndexRejected>> read_rejections;
    std::size_t hard_persistence{};
    std::size_t log_persistence{};
    std::size_t snapshot_persistence{};
    std::size_t install_requests{};
    std::uint64_t logical_sequence{100};
    lin::History history;
};

lin::GetResult read_result(const LogIndex index) {
    const auto first = ByteSequence::from_string("manifest-a");
    const auto second = ByteSequence::from_string("manifest-b");
    if (index == LogIndex{1}) {
        return {
            lin::VersionedValue{first, {1}},
            {1}};
    }
    if (index == LogIndex{2}) {
        return {
            lin::VersionedValue{second, {2}},
            {2}};
    }
    if (index == LogIndex{3}) {
        return {std::nullopt, {3}};
    }
    throw TestFailure("ReadIndex captured an unexpected command prefix");
}

SimulationConfig configuration(
    const std::vector<NodeId>& topology,
    const std::shared_ptr<Evidence>& evidence,
    const std::size_t command_count,
    const std::size_t read_count,
    const bool snapshots) {
    std::vector<CommandEnvelope> commands;
    for (std::uint64_t index = 1; index <= command_count; ++index) {
        commands.push_back(command(index));
    }
    std::vector<ReadIndexRequest> reads;
    for (std::uint64_t index = 1; index <= read_count; ++index) {
        reads.push_back(ReadIndexRequest{{700, index}});
    }
    std::vector<CreateRaftSnapshot> create_snapshots;
    if (snapshots) {
        create_snapshots.push_back({
            .applied_index = {command_count},
            .store_revision = {
                static_cast<std::int64_t>(command_count)},
            .compaction_revision = {
                static_cast<std::int64_t>(command_count)},
            .membership = membership(topology.size()),
            .state = {0x61, 0x63, 0x63}});
    }
    return {
        .timeouts = {25, 45},
        .seed = 0x6ac0,
        .heartbeat_interval = 3,
        .commands_on_leadership = std::move(commands),
        .reads_on_leadership = std::move(reads),
        .snapshots_on_apply = std::move(create_snapshots),
        .observer =
            [evidence](const NodeId node, const Snapshot& snapshot) {
                const auto previous = evidence->latest.find(node.value);
                if (previous != evidence->latest.end()
                    && snapshot.last_applied
                        < previous->second.last_applied) {
                    throw TestFailure("lastApplied regressed in simulation");
                }
                evidence->latest[node.value] = snapshot;
            },
        .effect_observer =
            [evidence](const NodeId node, const Effect& effect) {
                ++evidence->logical_sequence;
                if (const auto* apply =
                        std::get_if<ApplyLogEntry>(&effect)) {
                    evidence->apply_effects[node.value].push_back(
                        apply->entry.index);
                } else if (const auto* complete =
                               std::get_if<
                                   figure2::CompleteClientCommand>(
                                   &effect)) {
                    if (complete->index.value <= 3
                        && evidence->completed_commands.insert(
                               complete->index.value).second) {
                        lin::Result result;
                        if (complete->index == LogIndex{1}) {
                            result = lin::PutResult{{1}};
                        } else if (complete->index == LogIndex{2}) {
                            result =
                                lin::CompareAndSwapResult{true, {2}};
                        } else {
                            result = lin::EraseResult{true, {3}};
                        }
                        evidence->history.completions.push_back({
                            .operation_id = complete->index.value,
                            .completed_at =
                                evidence->logical_sequence,
                            .kind = lin::CompletionKind::succeeded,
                            .result = std::move(result)});
                    }
                } else if (const auto* read =
                               std::get_if<ReadIndexResponse>(&effect)) {
                    evidence->read_responses.emplace_back(node, *read);
                    const auto operation_id =
                        100 + read->request_id.sequence;
                    const auto duplicate = std::ranges::find(
                        evidence->history.completions,
                        operation_id,
                        &lin::Completion::operation_id);
                    if (duplicate == evidence->history.completions.end()) {
                        evidence->history.completions.push_back({
                            .operation_id = operation_id,
                            .completed_at =
                                evidence->logical_sequence,
                            .kind = lin::CompletionKind::succeeded,
                            .result = read_result(
                                read->committed_index)});
                    }
                } else if (const auto* rejected =
                               std::get_if<ReadIndexRejected>(&effect)) {
                    evidence->read_rejections.emplace_back(
                        node, *rejected);
                } else if (
                    std::holds_alternative<PersistRaftHardState>(
                        effect)) {
                    ++evidence->hard_persistence;
                } else if (
                    std::holds_alternative<PersistRaftLog>(effect)) {
                    ++evidence->log_persistence;
                } else if (
                    std::holds_alternative<PersistRaftSnapshot>(
                        effect)) {
                    ++evidence->snapshot_persistence;
                } else if (
                    std::holds_alternative<SendInstallSnapshot>(
                        effect)) {
                    ++evidence->install_requests;
                }
            },
        .client_observer =
            [evidence](const NodeId, const Input& input) {
                ++evidence->logical_sequence;
                const auto key =
                    ByteSequence::from_string("catalog/table");
                lin::Invocation invocation;
                if (const auto* proposal = std::get_if<
                        figure2::ReceiveClientCommand>(&input)) {
                    const auto sequence =
                        proposal->command.request_id.sequence;
                    invocation.operation_id = sequence;
                    invocation.client_id =
                        proposal->command.request_id.client;
                    if (sequence == 1) {
                        invocation.operation = lin::Put{
                            key,
                            ByteSequence::from_string("manifest-a")};
                    } else if (sequence == 2) {
                        invocation.operation = lin::CompareAndSwap{
                            key,
                            ByteSequence::from_string("manifest-a"),
                            ByteSequence::from_string("manifest-b")};
                    } else if (sequence == 3) {
                        invocation.operation = lin::Erase{key};
                    } else {
                        throw TestFailure(
                            "unexpected acceptance command");
                    }
                } else if (const auto* read =
                               std::get_if<ReadIndexRequest>(&input)) {
                    invocation.operation_id =
                        100 + read->request_id.sequence;
                    invocation.client_id = read->request_id.client;
                    invocation.operation = lin::Get{key};
                } else {
                    return;
                }
                invocation.invoked_at =
                    evidence->logical_sequence;
                const auto duplicate = std::ranges::find(
                    evidence->history.invocations,
                    invocation.operation_id,
                    &lin::Invocation::operation_id);
                if (duplicate == evidence->history.invocations.end()) {
                    evidence->history.invocations.push_back(
                        std::move(invocation));
                }
            }};
}

template <typename Predicate>
void advance_until(
    simulation::Simulator& simulator,
    Predicate&& predicate,
    const std::size_t max_events,
    const std::string_view failure) {
    for (std::size_t event = 0;
         event < max_events && !predicate();
         ++event) {
        simulator.require(simulator.step(), "simulation stopped early");
    }
    simulator.require(predicate(), failure);
}

NodeId current_leader(const Evidence& evidence) {
    for (const auto& [id, snapshot] : evidence.latest) {
        if (snapshot.role == RaftRole::leader) {
            return {id};
        }
    }
    return {};
}

std::vector<NodeId> without(
    const std::vector<NodeId>& topology,
    const NodeId excluded) {
    std::vector<NodeId> result;
    std::ranges::copy_if(
        topology,
        std::back_inserter(result),
        [excluded](const NodeId node) {
            return node != excluded;
        });
    return result;
}

void healthy_end_to_end_histories_pass() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto evidence = std::make_shared<Evidence>();
        simulation::Simulator simulator(
            topology,
            0x6000 + topology.size(),
            make_simulation_factory(configuration(
                topology, evidence, 3, 3, true)));
        for (std::size_t first = 0; first < topology.size(); ++first) {
            for (std::size_t second = 0;
                 second < topology.size();
                 ++second) {
                if (first != second) {
                    simulator.set_network_delay(
                        topology[first],
                        topology[second],
                        1 + (first + second) % 3);
                }
            }
        }
        advance_until(
            simulator,
            [&] {
                return current_leader(*evidence) != NodeId{};
            },
            2'000,
            "healthy schedule elected no leader");
        const auto leader = current_leader(*evidence);
        const auto peers = without(topology, leader);
        simulator.duplicate_next(leader, peers.front(), 2);
        simulator.drop_next(leader, peers.back(), 1);

        bool reordered = false;
        for (std::size_t event = 0; event < 100 && !reordered; ++event) {
            simulator.require(simulator.step(), "fault schedule stopped");
            const auto pending = simulator.pending_events();
            const auto message = std::ranges::find_if(
                pending,
                [leader](const simulation::PendingEvent& candidate) {
                    return candidate.kind
                            == simulation::PendingEventKind::message
                        && candidate.from == leader;
                });
            if (message != pending.end()) {
                simulator.delay_message(message->id, 11);
                reordered = true;
            }
        }
        simulator.require(reordered, "no message was available to reorder");

        advance_until(
            simulator,
            [&] {
                return evidence->completed_commands.size() == 3
                    && evidence->read_responses.size() >= 3
                    && std::ranges::all_of(
                        topology,
                        [&evidence](const NodeId node) {
                            const auto found =
                                evidence->latest.find(node.value);
                            return found != evidence->latest.end()
                                && found->second.last_applied
                                    == LogIndex{3}
                                && found->second.snapshot_metadata
                                && found->second.snapshot_metadata
                                       ->last_included_index
                                    == LogIndex{3};
                        });
            },
            12'000,
            "healthy Raft acceptance did not converge");

        simulator.require(
            evidence->hard_persistence > 0
                && evidence->log_persistence > 0
                && evidence->snapshot_persistence == topology.size(),
            "acceptance did not exercise durable completion boundaries");
        bool observed_full_apply = false;
        for (const auto node : topology) {
            const auto& applied = evidence->apply_effects[node.value];
            bool ordered = true;
            for (std::size_t index = 0; index < applied.size(); ++index) {
                ordered = ordered
                    && applied[index].value == index + 1;
            }
            simulator.require(
                ordered,
                "state-machine apply effects were not strictly ordered");
            observed_full_apply =
                observed_full_apply
                || applied
                    == std::vector<LogIndex>({{1}, {2}, {3}});
            simulator.require(
                !simulator.durable_records(node).empty(),
                "node has no durable recovery records");
        }
        simulator.require(
            observed_full_apply,
            "no node applied the committed log before snapshotting");
        const auto checked = lin::check(evidence->history);
        simulator.require(
            checked.outcome == lin::CheckOutcome::linearizable,
            checked.counterexample
                ? checked.counterexample->replay
                : "generated client history was not linearizable");
    }
}

void isolated_old_leader_cannot_complete_read() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto evidence = std::make_shared<Evidence>();
        simulation::Simulator simulator(
            topology,
            0x6100 + topology.size(),
            make_simulation_factory(configuration(
                topology, evidence, 1, 1, false)));
        NodeId old_leader;
        advance_until(
            simulator,
            [&] {
                for (const auto& [id, snapshot] : evidence->latest) {
                    if (snapshot.role == RaftRole::leader
                        && snapshot.pending_read_count == 1) {
                        old_leader = {id};
                        return true;
                    }
                }
                return false;
            },
            4'000,
            "no leader began the acceptance ReadIndex");

        for (const auto& event : simulator.pending_events()) {
            if (event.kind == simulation::PendingEventKind::message
                && (event.from == old_leader
                    || event.to == old_leader)) {
                simulator.delay_message(event.id, 100'000);
            }
        }
        for (const auto peer : topology) {
            if (peer != old_leader) {
                simulator.partition(old_leader, peer);
            }
        }
        NodeId replacement;
        advance_until(
            simulator,
            [&] {
                for (const auto& [id, snapshot] : evidence->latest) {
                    if (NodeId{id} != old_leader
                        && snapshot.role == RaftRole::leader) {
                        replacement = {id};
                        return true;
                    }
                }
                return false;
            },
            6'000,
            "majority did not replace the isolated leader");
        simulator.require(
            replacement != old_leader
                && std::ranges::none_of(
                    evidence->read_responses,
                    [old_leader](const auto& response) {
                        return response.first == old_leader;
                    })
                && evidence->latest.at(old_leader.value)
                       .completed_read_count
                    == 0,
            "partitioned former leader completed a linearizable read");
    }
}

void committed_write_survives_leader_failure() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto evidence = std::make_shared<Evidence>();
        simulation::Simulator simulator(
            topology,
            0x6200 + topology.size(),
            make_simulation_factory(configuration(
                topology, evidence, 1, 0, false)));
        NodeId failed_leader;
        advance_until(
            simulator,
            [&] {
                const auto leader = current_leader(*evidence);
                if (leader != NodeId{}
                    && evidence->latest.at(leader.value).commit_index
                        >= LogIndex{1}) {
                    failed_leader = leader;
                    return true;
                }
                return false;
            },
            5'000,
            "leader committed no write before failure");
        simulator.crash(failed_leader);

        NodeId replacement;
        advance_until(
            simulator,
            [&] {
                for (const auto& [id, snapshot] : evidence->latest) {
                    if (NodeId{id} != failed_leader
                        && snapshot.role == RaftRole::leader
                        && snapshot.commit_index >= LogIndex{1}
                        && snapshot.last_applied >= LogIndex{1}) {
                        replacement = {id};
                        return true;
                    }
                }
                return false;
            },
            8'000,
            "replacement leader lost the committed write");
        simulator.require(
            replacement != NodeId{},
            "no replacement leader retained committed state");

        evidence->latest.erase(failed_leader.value);
        simulator.restart(failed_leader);
        advance_until(
            simulator,
            [&] {
                const auto found =
                    evidence->latest.find(failed_leader.value);
                return found != evidence->latest.end()
                    && found->second.last_applied >= LogIndex{1};
            },
            8'000,
            "restarted leader did not catch up committed state");
    }
}

void minority_partition_cannot_commit() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto evidence = std::make_shared<Evidence>();
        simulation::Simulator simulator(
            topology,
            0x6300 + topology.size(),
            make_simulation_factory(configuration(
                topology, evidence, 1, 0, false)));
        advance_until(
            simulator,
            [&] {
                return current_leader(*evidence) != NodeId{};
            },
            2'000,
            "minority schedule elected no leader");
        const auto leader = current_leader(*evidence);
        std::vector<NodeId> minority{leader};
        if (topology.size() == 5) {
            minority.push_back(without(topology, leader).front());
        }
        for (const auto& event : simulator.pending_events()) {
            if (event.kind != simulation::PendingEventKind::message) {
                continue;
            }
            const bool from_minority = event.from
                && std::ranges::find(minority, *event.from)
                    != minority.end();
            const bool to_minority =
                std::ranges::find(minority, event.to)
                != minority.end();
            if (from_minority != to_minority) {
                simulator.delay_message(event.id, 100'000);
            }
        }
        for (const auto first : minority) {
            for (const auto second : topology) {
                if (std::ranges::find(minority, second)
                    == minority.end()) {
                    simulator.partition(first, second);
                }
            }
        }
        for (std::size_t event = 0; event < 1'000; ++event) {
            simulator.require(
                simulator.step(), "minority simulation stopped early");
        }
        simulator.require(
            evidence->latest.at(leader.value).commit_index
                == LogIndex{},
            "partitioned minority committed an unreplicated write");
    }
}

void lagging_restarted_follower_installs_snapshot() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto evidence = std::make_shared<Evidence>();
        simulation::Simulator simulator(
            topology,
            0x6400 + topology.size(),
            make_simulation_factory(configuration(
                topology, evidence, 3, 0, true)));
        advance_until(
            simulator,
            [&] {
                return current_leader(*evidence) != NodeId{};
            },
            2'000,
            "snapshot schedule elected no leader");
        const auto leader = current_leader(*evidence);
        auto lagging = topology.back();
        if (lagging == leader) {
            lagging = topology.front();
        }
        simulator.crash(lagging);
        advance_until(
            simulator,
            [&] {
                return std::ranges::all_of(
                    topology,
                    [&evidence, lagging](const NodeId node) {
                        if (node == lagging) {
                            return true;
                        }
                        const auto found =
                            evidence->latest.find(node.value);
                        return found != evidence->latest.end()
                            && found->second.snapshot_metadata
                            && found->second.snapshot_metadata
                                   ->last_included_index
                                == LogIndex{3};
                    });
            },
            12'000,
            "running majority did not publish snapshots");

        simulator.set_persistence_delay(lagging, 5);
        simulator.set_network_delay(leader, lagging, 4);
        simulator.duplicate_next(leader, lagging, 1);
        evidence->latest.erase(lagging.value);
        simulator.restart(lagging);
        advance_until(
            simulator,
            [&] {
                const auto found = evidence->latest.find(lagging.value);
                return found != evidence->latest.end()
                    && found->second.snapshot_metadata
                    && found->second.snapshot_metadata
                           ->last_included_index
                        == LogIndex{3}
                    && found->second.last_applied == LogIndex{3};
            },
            15'000,
            "lagging follower did not install the durable snapshot");
        simulator.require(
            evidence->install_requests > 0,
            "catch-up bypassed the real InstallSnapshot core path");

        simulator.crash(lagging);
        evidence->latest.erase(lagging.value);
        simulator.restart(lagging);
        advance_until(
            simulator,
            [&] {
                const auto found = evidence->latest.find(lagging.value);
                return found != evidence->latest.end()
                    && found->second.snapshot_metadata
                    && found->second.last_applied == LogIndex{3};
            },
            2'000,
            "snapshot was not recovered after a second restart");
    }
}

}  // namespace

int main() {
    try {
        healthy_end_to_end_histories_pass();
        isolated_old_leader_cannot_complete_read();
        committed_write_survives_leader_failure();
        minority_partition_cannot_commit();
        lagging_restarted_follower_installs_snapshot();
        std::cout << "raft acceptance tests passed\n";
        return 0;
    } catch (const simulation::SimulationFailure& error) {
        std::cerr << "raft acceptance simulation failure:\n"
                  << error.what() << '\n';
        return 1;
    } catch (const std::exception& error) {
        std::cerr << "raft acceptance test failure: "
                  << error.what() << '\n';
        return 1;
    }
}
