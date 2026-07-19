#include "kura/metadata/raft/election.hpp"
#include "kura/metadata/storage/checksum.hpp"
#include "kura/metadata/storage/snapshot_store.hpp"
#include "kura/metadata/storage/storage_error.hpp"
#include "kura/metadata/storage/wal.hpp"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
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
        .request_id = {.client = 51, .sequence = sequence},
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

ClusterMembership membership(const std::size_t count) {
    ClusterMembership result;
    for (std::uint64_t id = 1; id <= count; ++id) {
        result.voters.push_back(NodeId{id});
    }
    result.learners.push_back(NodeId{count + 1});
    return result;
}

RaftSnapshot raft_snapshot(
    const std::uint64_t index,
    const std::uint64_t term,
    const std::size_t cluster_size = 3,
    std::vector<std::uint8_t> state = {9, 8, 7}) {
    return {
        .metadata = {
            .last_included_index = {index},
            .last_included_term = {term},
            .store_revision = {static_cast<std::int64_t>(index)},
            .compaction_revision = {static_cast<std::int64_t>(index)},
            .membership = membership(cluster_size),
            .format_version = 1},
        .state = std::move(state)};
}

Core core(
    const NodeId self,
    const std::size_t cluster_size = 3,
    const RaftHardState hard = {},
    std::vector<LogEntry> log = {},
    const LogIndex applied = {},
    std::optional<RaftSnapshot> recovered_snapshot = {}) {
    std::vector<NodeId> peers;
    for (std::uint64_t id = 1; id <= cluster_size; ++id) {
        if (NodeId{id} != self) {
            peers.push_back(NodeId{id});
        }
    }
    return Core(
        self,
        std::move(peers),
        hard,
        0x517a,
        {10, 20},
        std::move(log),
        2,
        applied,
        128,
        4'096,
        std::move(recovered_snapshot),
        1U << 20);
}

StepResult complete_hard(Core& node) {
    const auto& pending = *node.specification_state().pending_hard_state;
    return node.step(RaftHardStatePersisted{
        .request_id = pending.request.request_id,
        .state = pending.request.state});
}

StepResult complete_log(Core& node) {
    const auto* pending = node.pending_log_persistence();
    return node.step(RaftLogPersisted{
        .request_id = pending->request_id,
        .log = pending->log});
}

StepResult complete_snapshot(Core& node) {
    const auto* pending = node.pending_snapshot_persistence();
    return node.step(RaftSnapshotPersisted{
        .request_id = pending->request_id,
        .snapshot = pending->snapshot});
}

StepResult complete_install(Core& node) {
    const auto published = complete_snapshot(node);
    const auto* restore =
        find_effect<RestoreStateMachineSnapshot>(published);
    expect(restore != nullptr, "install did not request state restore");
    return node.step(RaftSnapshotRestored{
        .request_id = restore->request_id,
        .index = restore->snapshot.metadata.last_included_index});
}

void elect(Core& node) {
    const auto started = node.start();
    const auto timer =
        find_effect<ResetElectionDeadline>(started)->timer_id;
    static_cast<void>(node.step(ElectionDeadline{timer}));
    static_cast<void>(complete_hard(node));
    const auto term = node.snapshot().hard_state.current_term;
    static_cast<void>(node.step(
        figure2::ReceiveRequestVoteResponse{
            .from = {2}, .response = {term, true}}));
    expect(node.snapshot().role == RaftRole::leader, "election failed");
}

InstallSnapshotRequest install_request(
    const Term term,
    const NodeId leader,
    RaftSnapshot snapshot,
    const std::uint64_t transfer = 71) {
    return {
        .term = term,
        .leader = leader,
        .transfer_id = transfer,
        .metadata = snapshot.metadata,
        .offset = 0,
        .total_size = snapshot.state.size(),
        .chunk = snapshot.state,
        .state_checksum = crc32c(snapshot.state),
        .done = true};
}

void local_creation_is_publication_gated_and_compacts() {
    auto node = core(
        {2},
        3,
        {{3}, std::nullopt},
        {entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 3)},
        {3});
    static_cast<void>(node.start());
    const CreateRaftSnapshot create{
        .applied_index = {3},
        .store_revision = {12},
        .compaction_revision = {10},
        .membership = membership(3),
        .state = {1, 3, 5, 7}};
    const auto requested = node.step(create);
    expect(
        find_effect<PersistRaftSnapshot>(requested) != nullptr
            && find_effect<TruncateRaftLogPrefix>(requested) == nullptr
            && find_effect<RaftSnapshotCreated>(requested) == nullptr,
        "snapshot publication was not the only durability-dependent effect");
    expect(
        node.snapshot().log.size() == 4
            && !node.snapshot().snapshot_metadata,
        "log compacted before snapshot publication");

    const auto published = complete_snapshot(node);
    expect(
        find_effect<RestoreStateMachineSnapshot>(published) == nullptr
            && find_effect<TruncateRaftLogPrefix>(published)->through
                == LogIndex{3}
            && find_effect<RaftSnapshotCreated>(published)->index
                == LogIndex{3},
        "published snapshot did not restore and unlock truncation");
    expect(
        node.snapshot().snapshot_metadata->membership == membership(3)
            && node.snapshot().log
                == std::vector<LogEntry>{entry(4, 3)}
            && node.snapshot().commit_index == LogIndex{3}
            && node.snapshot().last_applied == LogIndex{3},
        "snapshot did not preserve membership and the post-snapshot suffix");

    const auto stale = node.step(create);
    expect(
        find_effect<RaftSnapshotRejected>(stale)->reason
            == SnapshotRejection::stale,
        "non-advancing local snapshot was accepted");
}

void durable_store_and_wal_gate_follow_core_effects() {
    const auto root =
        std::filesystem::current_path() / "raft-snapshot-boundary-test";
    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    try {
        StorageLimits limits;
        limits.wal_segment_bytes = 124;
        limits.max_wal_payload_bytes = 20;
        limits.max_recovery_records = 100;
        limits.max_snapshot_bytes = 1U << 20;
        limits.max_snapshot_members = 16;
        FileSnapshotStore store(root / "snapshots", limits);
        SegmentedWriteAheadLog wal(root / "wal", limits);
        const std::vector<WalEntry> records{
            {WalRecordType::command, {1}, {1}, {1}},
            {WalRecordType::command, {1}, {2}, {2}},
            {WalRecordType::command, {1}, {3}, {3}}};
        wal.append(records, Durability::synchronize);
        bool rejected = false;
        try {
            wal.truncate_through({2}, store);
        } catch (const StorageError&) {
            rejected = true;
        }
        expect(rejected, "WAL truncated without a durable snapshot");

        auto node = core(
            {2},
            3,
            {{1}, std::nullopt},
            {entry(1, 1), entry(2, 1), entry(3, 1)},
            {2});
        static_cast<void>(node.start());
        static_cast<void>(node.step(CreateRaftSnapshot{
            .applied_index = {2},
            .store_revision = {2},
            .compaction_revision = {2},
            .membership = membership(3),
            .state = {2, 4, 6}}));
        const auto pending = *node.pending_snapshot_persistence();
        store.publish({
            .metadata = pending.snapshot.metadata,
            .state = pending.snapshot.state});
        const auto latest = *store.latest();
        const auto completed = node.step(RaftSnapshotPersisted{
            .request_id = pending.request_id,
            .snapshot = {
                .metadata = latest.metadata,
                .state = latest.state,
                .checksum = latest.checksum}});
        const auto through =
            find_effect<TruncateRaftLogPrefix>(completed)->through;
        wal.truncate_through(through, store);
        expect(
            wal.recover()
                == std::vector<WalEntry>{records.back()},
            "durable snapshot did not gate the matching WAL truncation");
    } catch (...) {
        std::filesystem::remove_all(root, ignored);
        throw;
    }
    std::filesystem::remove_all(root, ignored);
}

void install_is_hard_state_and_snapshot_gated() {
    auto follower = core({2});
    static_cast<void>(follower.start());
    auto snapshot = raft_snapshot(3, 1);
    auto request = install_request({1}, {1}, snapshot);
    const auto observed = follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = request});
    expect(
        find_effect<PersistRaftHardState>(observed) != nullptr
            && find_effect<PersistRaftSnapshot>(observed) == nullptr
            && find_effect<SendInstallSnapshotResponse>(observed)
                == nullptr,
        "higher-term install escaped hard-state durability");

    const auto hard = complete_hard(follower);
    expect(
        find_effect<PersistRaftSnapshot>(hard) != nullptr
            && find_effect<SendInstallSnapshotResponse>(hard) == nullptr,
        "install success escaped snapshot durability");
    expect(
        follower.snapshot().last_applied == LogIndex{},
        "unpublished snapshot changed the represented state machine");

    auto restarted = core({2}, 3, {{1}, std::nullopt});
    static_cast<void>(restarted.start());
    const auto retried = restarted.step(
        ReceiveInstallSnapshot{.from = {1}, .request = request});
    expect(
        find_effect<PersistRaftSnapshot>(retried) != nullptr
            && !restarted.snapshot().snapshot_metadata,
        "restart fabricated completion for an interrupted install");

    const auto published = complete_snapshot(restarted);
    expect(
        find_effect<RestoreStateMachineSnapshot>(published) != nullptr
            && find_effect<TruncateRaftLogPrefix>(published) == nullptr
            && find_effect<SendInstallSnapshotResponse>(published)
                == nullptr
            && restarted.snapshot().last_applied == LogIndex{},
        "published install advanced or replied before state restore");
    const auto* restore =
        find_effect<RestoreStateMachineSnapshot>(published);
    auto publication_restart = core(
        {2},
        3,
        {{1}, std::nullopt},
        {},
        {},
        restore->snapshot);
    static_cast<void>(publication_restart.start());
    expect(
        publication_restart.snapshot().last_applied == LogIndex{3}
            && publication_restart.snapshot().snapshot_metadata,
        "restart after publication did not restore the last valid snapshot");
    const auto completed = restarted.step(RaftSnapshotRestored{
        .request_id = restore->request_id,
        .index = restore->snapshot.metadata.last_included_index});
    expect(
        find_effect<TruncateRaftLogPrefix>(completed) != nullptr
            && find_effect<SendInstallSnapshotResponse>(completed)
                   ->response.succeeded,
        "restored install did not unlock truncation and success");
    expect(
        restarted.snapshot().commit_index == LogIndex{3}
            && restarted.snapshot().last_applied == LogIndex{3}
            && restarted.snapshot().snapshot_metadata
                   ->last_included_index
                == LogIndex{3},
        "installed snapshot did not atomically advance Raft indexes");

    const auto duplicate = restarted.step(
        ReceiveInstallSnapshot{.from = {1}, .request = request});
    expect(
        find_effect<PersistRaftSnapshot>(duplicate) == nullptr
            && find_effect<SendInstallSnapshotResponse>(duplicate)
                   ->response.succeeded,
        "duplicate valid install was not idempotent");
}

void invalid_and_stale_installs_never_succeed() {
    auto follower = core({2}, 3, {{2}, std::nullopt});
    static_cast<void>(follower.start());
    auto valid = install_request({2}, {1}, raft_snapshot(4, 2));

    auto stale_term = valid;
    stale_term.term = {1};
    const auto stale_term_result = follower.step(
        ReceiveInstallSnapshot{
            .from = {1}, .request = stale_term});
    expect(
        !find_effect<SendInstallSnapshotResponse>(stale_term_result)
             ->response.succeeded
            && find_effect<PersistRaftSnapshot>(stale_term_result)
                == nullptr,
        "stale-term snapshot was persisted or acknowledged");

    auto corrupt = valid;
    corrupt.chunk.front() ^= 1;
    const auto corrupt_result = follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = corrupt});
    expect(
        !find_effect<SendInstallSnapshotResponse>(corrupt_result)
             ->response.succeeded
            && find_effect<PersistRaftSnapshot>(corrupt_result) == nullptr,
        "corrupt snapshot was persisted or acknowledged");

    auto out_of_order = valid;
    out_of_order.offset = 1;
    const auto offset_result = follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = out_of_order});
    expect(
        !find_effect<SendInstallSnapshotResponse>(offset_result)
             ->response.succeeded,
        "out-of-order whole-snapshot transfer was accepted");

    auto invalid_members = valid;
    invalid_members.metadata.membership.voters = {{1}, {2}, {4}};
    const auto membership_result = follower.step(
        ReceiveInstallSnapshot{
            .from = {1}, .request = invalid_members});
    expect(
        !find_effect<SendInstallSnapshotResponse>(membership_result)
             ->response.succeeded,
        "snapshot with foreign membership was accepted");

    static_cast<void>(follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = valid}));
    static_cast<void>(complete_install(follower));
    auto stale = valid;
    stale.transfer_id = 72;
    stale.metadata.last_included_index = {3};
    const auto stale_result = follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = stale});
    expect(
        !find_effect<SendInstallSnapshotResponse>(stale_result)
             ->response.succeeded
            && follower.snapshot().last_applied == LogIndex{4},
        "stale snapshot regressed installed state");
}

void follower_resumes_append_after_install() {
    auto follower = core({2}, 3, {{1}, std::nullopt});
    static_cast<void>(follower.start());
    auto request = install_request({1}, {1}, raft_snapshot(3, 1));
    static_cast<void>(follower.step(
        ReceiveInstallSnapshot{.from = {1}, .request = request}));
    static_cast<void>(complete_install(follower));

    const figure2::ReceiveAppendEntries append{
        .from = {1},
        .request = {
            .term = {1},
            .leader = {1},
            .previous_log_index = {3},
            .previous_log_term = {1},
            .entries = {entry(4, 1)},
            .leader_commit = {4}}};
    const auto accepted = follower.step(append);
    expect(
        find_effect<PersistRaftLog>(accepted) != nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(accepted)
                == nullptr,
        "post-install append was not persistence gated");
    const auto durable = complete_log(follower);
    expect(
        find_effect<figure2::SendAppendEntriesResponse>(durable)
                   ->response.succeeded
            && follower.snapshot().log
                == std::vector<LogEntry>{entry(4, 1)}
            && follower.snapshot().commit_index == LogIndex{4},
        "post-install AppendEntries did not resume on snapshot boundary");

    const auto duplicate = follower.step(append);
    expect(
        find_effect<PersistRaftLog>(duplicate) == nullptr
            && find_effect<figure2::SendAppendEntriesResponse>(duplicate)
                   ->response.succeeded,
        "duplicate post-install AppendEntries rewrote the log");
}

void leader_falls_back_to_snapshot_and_resumes_suffix() {
    auto leader = core(
        {1},
        3,
        {{3}, std::nullopt},
        {entry(4, 3)},
        {3},
        raft_snapshot(3, 3));
    elect(leader);
    const auto term = leader.snapshot().hard_state.current_term;
    static_cast<void>(leader.step(
        figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {term, false, {}}}));
    const auto fallback = leader.step(
        figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {term, false, {}}});
    const auto* install = find_effect<SendInstallSnapshot>(fallback);
    expect(
        install != nullptr
            && install->request.metadata.last_included_index
                == LogIndex{3}
            && install->request.offset == 0
            && install->request.done,
        "compacted nextIndex did not trigger canonical snapshot transfer");

    const auto stale_ack = leader.step(
        ReceiveInstallSnapshotResponse{
            .from = {2},
            .response = {
                .term = term,
                .transfer_id = install->request.transfer_id + 1,
                .succeeded = true,
                .last_included_index = {3}}});
    expect(
        stale_ack.effects.empty()
            && leader.snapshot().peer_progress.at({2}).match_index
                == LogIndex{},
        "stale snapshot acknowledgement advanced peer progress");

    const auto resumed = leader.step(
        ReceiveInstallSnapshotResponse{
            .from = {2},
            .response = {
                .term = term,
                .transfer_id = install->request.transfer_id,
                .succeeded = true,
                .last_included_index = {3}}});
    const auto appends =
        effects<figure2::SendAppendEntries>(resumed);
    const auto peer_append = std::ranges::find_if(
        appends,
        [](const figure2::SendAppendEntries& send) {
            return send.to == NodeId{2}
                && !send.request.entries.empty();
        });
    expect(
        leader.snapshot().peer_progress.at({2}).match_index
                == LogIndex{3}
            && peer_append != appends.end()
            && peer_append->request.previous_log_index == LogIndex{3}
            && peer_append->request.entries.front().index
                == LogIndex{4},
        "snapshot acknowledgement did not resume suffix replication");
    const auto duplicate_ack = leader.step(
        ReceiveInstallSnapshotResponse{
            .from = {2},
            .response = {
                .term = term,
                .transfer_id = install->request.transfer_id,
                .succeeded = true,
                .last_included_index = {3}}});
    expect(
        duplicate_ack.effects.empty(),
        "duplicate snapshot acknowledgement retriggered replication");

    const auto higher = leader.step(
        ReceiveInstallSnapshotResponse{
            .from = {2},
            .response = {
                .term = {term.value + 1},
                .transfer_id = 99,
                .succeeded = false}});
    expect(
        leader.snapshot().role == RaftRole::follower
            && find_effect<PersistRaftHardState>(higher) != nullptr,
        "higher-term snapshot response did not step down durably");
}

void higher_term_install_fails_pending_read() {
    auto leader = core({1});
    elect(leader);
    static_cast<void>(leader.step(
        figure2::ReceiveClientCommand{command(1)}));
    static_cast<void>(complete_log(leader));
    const auto term = leader.snapshot().hard_state.current_term;
    static_cast<void>(leader.step(
        figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {term, true, {1}}}));
    const auto application = *leader.pending_application();
    static_cast<void>(leader.step(LogEntryApplied{
        .request_id = application.request_id,
        .index = application.entry.index}));
    const RequestId read_id{77, 1};
    static_cast<void>(leader.step(ReadIndexRequest{read_id}));
    expect(
        leader.snapshot().pending_read_count == 1,
        "test did not establish a pending ReadIndex");

    auto incoming = install_request(
        {term.value + 1},
        {2},
        raft_snapshot(2, term.value + 1));
    const auto stepped_down = leader.step(
        ReceiveInstallSnapshot{.from = {2}, .request = incoming});
    const auto* rejected =
        find_effect<ReadIndexRejected>(stepped_down);
    expect(
        rejected != nullptr
            && rejected->request_id == read_id
            && rejected->reason == ReadIndexFailure::leadership_lost
            && leader.snapshot().pending_read_count == 0,
        "higher-term install did not fail pending linearizable reads");
}

void read_index_uses_compacted_current_term_boundary() {
    auto leader = core({1});
    elect(leader);
    static_cast<void>(leader.step(
        figure2::ReceiveClientCommand{command(1)}));
    static_cast<void>(complete_log(leader));
    const auto term = leader.snapshot().hard_state.current_term;
    static_cast<void>(leader.step(
        figure2::ReceiveAppendEntriesResponse{
            .from = {2}, .response = {term, true, {1}}}));
    const auto application = *leader.pending_application();
    static_cast<void>(leader.step(LogEntryApplied{
        .request_id = application.request_id,
        .index = application.entry.index}));
    static_cast<void>(leader.step(CreateRaftSnapshot{
        .applied_index = {1},
        .store_revision = {1},
        .compaction_revision = {1},
        .membership = membership(3),
        .state = {5, 5}}));
    static_cast<void>(complete_snapshot(leader));
    expect(
        leader.snapshot().log.empty()
            && leader.snapshot().snapshot_metadata
                   ->last_included_term
                == term,
        "leader did not compact through its current-term entry");

    const RequestId read_id{88, 1};
    const auto requested = leader.step(ReadIndexRequest{read_id});
    const auto probes =
        effects<figure2::SendAppendEntries>(requested);
    expect(
        !probes.empty() && leader.snapshot().pending_read_count == 1
            && find_effect<ReadIndexRejected>(requested) == nullptr,
        "compacted current-term commit did not satisfy ReadIndex");
    const auto context = probes.front().request.read_context;
    const auto completed = leader.step(
        figure2::ReceiveAppendEntriesResponse{
            .from = {2},
            .response = {term, true, {1}, context}});
    expect(
        find_effect<ReadIndexResponse>(completed)->request_id == read_id,
        "ReadIndex did not complete across a snapshot boundary");
}

void simulator_snapshots_and_recovers_three_and_five_nodes() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations = std::make_shared<
            std::map<std::uint64_t, raft::election::Snapshot>>();
        SimulationConfig config{
            .timeouts = {20, 35},
            .seed = 0x91a7,
            .heartbeat_interval = 2,
            .commands_on_leadership = {command(1)},
            .snapshots_on_apply = {
                CreateRaftSnapshot{
                    .applied_index = {1},
                    .store_revision = {1},
                    .compaction_revision = {1},
                    .membership = membership(topology.size()),
                    .state = {4, 2}}},
            .observer =
                [observations](
                    const NodeId node,
                    const raft::election::Snapshot& view) {
                    (*observations)[node.value] = view;
                }};
        simulation::Simulator simulator(
            topology,
            0x11cc,
            make_simulation_factory(std::move(config)));
        NodeId leader;
        for (std::size_t steps = 0; steps < 2'000 && leader == NodeId{};
             ++steps) {
            expect(simulator.step(), "election schedule stopped early");
            for (const auto& [id, observed] : *observations) {
                if (observed.role == RaftRole::leader) {
                    leader = {id};
                }
            }
        }
        expect(leader != NodeId{}, "snapshot schedule elected no leader");
        auto lagging = topology.back();
        if (lagging == leader) {
            lagging = topology.front();
        }
        simulator.crash(lagging);

        bool majority_snapshotted = false;
        for (std::size_t steps = 0;
             steps < 8'000 && !majority_snapshotted;
             ++steps) {
            expect(simulator.step(), "snapshot schedule stopped early");
            majority_snapshotted = std::ranges::all_of(
                topology,
                [&observations, lagging](const NodeId node) {
                    if (node == lagging) {
                        return true;
                    }
                    const auto found =
                        observations->find(node.value);
                    return found != observations->end()
                        && found->second.snapshot_metadata
                        && found->second.snapshot_metadata
                               ->last_included_index
                            == LogIndex{1}
                        && found->second.last_applied == LogIndex{1};
                });
        }
        expect(
            majority_snapshotted,
            "odd-node majority did not publish snapshots");
        simulator.restart(lagging);
        bool caught_up = false;
        for (std::size_t steps = 0; steps < 8'000 && !caught_up; ++steps) {
            expect(simulator.step(), "restart schedule stopped early");
            const auto found = observations->find(lagging.value);
            caught_up = found != observations->end()
                && found->second.snapshot_metadata
                && found->second.snapshot_metadata->last_included_index
                    == LogIndex{1}
                && found->second.last_applied == LogIndex{1};
        }
        expect(
            caught_up
                && observations->at(lagging.value).snapshot_metadata
                    ->last_included_index
                    == LogIndex{1},
            "lagging restarted follower did not catch up by snapshot");

        simulator.crash(lagging);
        simulator.restart(lagging);
        for (std::size_t steps = 0; steps < 50; ++steps) {
            expect(simulator.step(), "second restart stopped early");
        }
        expect(
            observations->at(lagging.value).snapshot_metadata
                    ->last_included_index
                == LogIndex{1}
                && observations->at(lagging.value).last_applied
                    == LogIndex{1},
            "restart did not recover represented snapshot state");
    }
}

}  // namespace

int main() {
    try {
        local_creation_is_publication_gated_and_compacts();
        durable_store_and_wal_gate_follow_core_effects();
        install_is_hard_state_and_snapshot_gated();
        invalid_and_stale_installs_never_succeed();
        follower_resumes_append_after_install();
        leader_falls_back_to_snapshot_and_resumes_suffix();
        higher_term_install_fails_pending_read();
        read_index_uses_compacted_current_term_boundary();
        simulator_snapshots_and_recovers_three_and_five_nodes();
        std::cout << "raft snapshot tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "raft snapshot test failure: " << error.what() << '\n';
        return 1;
    }
}
