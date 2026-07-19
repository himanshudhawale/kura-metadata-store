#pragma once

#include "kura/metadata/raft/application.hpp"
#include "kura/metadata/raft/figure2_spec.hpp"
#include "kura/metadata/raft/simulation.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <variant>
#include <vector>

namespace kura::metadata::raft::election {

struct TimeoutRange {
    simulation::LogicalTime minimum{10};
    simulation::LogicalTime maximum{20};
};

struct ElectionDeadline {
    simulation::TimerId timer_id{};
};

struct HeartbeatDeadline {
    simulation::TimerId timer_id{};
};

using Input = std::variant<
    ElectionDeadline,
    HeartbeatDeadline,
    figure2::ReceiveRequestVote,
    figure2::ReceiveRequestVoteResponse,
    figure2::ReceiveAppendEntries,
    figure2::ReceiveAppendEntriesResponse,
    figure2::ReceiveClientCommand,
    RaftHardStatePersisted,
    RaftLogPersisted,
    LogEntryApplied,
    LogEntryApplyFailed,
    RetryApplication,
    ReadIndexRequest,
    CancelReadIndex,
    TimeoutReadIndex>;

struct ResetElectionDeadline {
    simulation::TimerId timer_id{};
    simulation::LogicalTime delay{};
};

struct CancelElectionDeadline {
    simulation::TimerId timer_id{};
};

struct ResetHeartbeatDeadline {
    simulation::TimerId timer_id{};
    simulation::LogicalTime delay{};
};

struct CancelHeartbeatDeadline {
    simulation::TimerId timer_id{};
};

struct RoleTransition {
    RaftRole from;
    RaftRole to;
    Term term;
};

using Effect = std::variant<
    RoleTransition,
    PersistRaftHardState,
    PersistRaftLog,
    figure2::SendRequestVote,
    figure2::SendRequestVoteResponse,
    figure2::SendAppendEntries,
    figure2::SendAppendEntriesResponse,
    ApplyLogEntry,
    ApplicationBackpressured,
    figure2::CompleteClientCommand,
    ReadIndexResponse,
    ReadIndexRejected,
    ResetElectionDeadline,
    CancelElectionDeadline,
    ResetHeartbeatDeadline,
    CancelHeartbeatDeadline>;

struct StepResult {
    std::vector<figure2::RuleId> rules;
    std::vector<Effect> effects;
};

struct Snapshot {
    RaftRole role{RaftRole::follower};
    RaftHardState hard_state;
    std::vector<NodeId> votes_received;
    std::optional<simulation::TimerId> election_timer;
    std::optional<simulation::LogicalTime> election_timeout;
    std::optional<simulation::TimerId> heartbeat_timer;
    std::vector<LogEntry> log;
    std::map<NodeId, figure2::PeerProgress> peer_progress;
    LogIndex commit_index;
    LogIndex last_applied;
    bool application_pending{};
    bool application_blocked{};
    std::size_t pending_read_count{};
    std::uint64_t completed_read_count{};
    std::uint64_t rejected_read_count{};
    bool waiting_for_persistence{};
};

class Core {
public:
    Core(
        NodeId self,
        std::vector<NodeId> peers,
        RaftHardState recovered,
        std::uint64_t seed,
        TimeoutRange timeouts = {},
        std::vector<LogEntry> recovered_log = {},
        simulation::LogicalTime heartbeat_interval = 2,
        LogIndex recovered_applied = {},
        std::size_t max_pending_reads = 128,
        std::size_t max_read_history = 4'096);

    [[nodiscard]] StepResult start();
    [[nodiscard]] StepResult step(const Input& input);
    [[nodiscard]] Snapshot snapshot() const;
    [[nodiscard]] const figure2::State& specification_state() const noexcept;
    [[nodiscard]] const PersistRaftLog*
    pending_log_persistence() const noexcept;
    [[nodiscard]] const ApplyLogEntry*
    pending_application() const noexcept;

private:
    [[nodiscard]] StepResult translate(
        figure2::StepResult result,
        RaftRole previous_role);
    [[nodiscard]] ResetElectionDeadline next_deadline();
    [[nodiscard]] ResetHeartbeatDeadline next_heartbeat();
    void gate_log_persistence(StepResult& result);
    void schedule_application(StepResult& result);
    void acknowledge_read(
        const figure2::ReceiveAppendEntriesResponse& response,
        StepResult& result);
    void complete_ready_reads(StepResult& result);
    void reject_pending_reads(
        ReadIndexFailure reason,
        StepResult& result);
    [[nodiscard]] StepResult begin_read(const ReadIndexRequest& request);
    [[nodiscard]] StepResult cancel_read(const CancelReadIndex& request);
    [[nodiscard]] StepResult timeout_read(const TimeoutReadIndex& request);

    struct PendingLogPersistence {
        PersistRaftLog request;
        std::vector<Effect> deferred_effects;
    };

    struct PendingApplication {
        ApplyLogEntry request;
        bool blocked{};
    };

    struct PendingRead {
        ReadIndexRequest request;
        LogIndex read_index;
        std::set<NodeId> acknowledgements;
        bool quorum_confirmed{};
    };

    figure2::State state_;
    TimeoutRange timeouts_;
    simulation::LogicalTime heartbeat_interval_{};
    std::uint64_t random_state_{};
    simulation::TimerId next_timer_id_{1};
    std::uint64_t next_log_request_id_{1ULL << 63};
    std::uint64_t next_application_request_id_{1ULL << 62};
    std::optional<simulation::TimerId> election_timer_;
    std::optional<simulation::LogicalTime> election_timeout_;
    std::optional<simulation::TimerId> heartbeat_timer_;
    std::optional<PendingLogPersistence> pending_log_;
    std::optional<PendingApplication> pending_application_;
    std::map<ReadIndexContext, PendingRead> pending_reads_;
    std::map<RequestId, ReadIndexContext> pending_read_requests_;
    std::set<RequestId> used_read_requests_;
    std::size_t max_pending_reads_{};
    std::size_t max_read_history_{};
    std::uint64_t next_read_context_{1};
    std::uint64_t completed_read_count_{};
    std::uint64_t rejected_read_count_{};
    bool started_{};
};

using SimulationObserver = std::function<void(NodeId, const Snapshot&)>;

struct SimulationConfig {
    TimeoutRange timeouts;
    std::uint64_t seed{};
    bool mix_node_id_into_seed{true};
    simulation::LogicalTime heartbeat_interval{2};
    std::vector<CommandEnvelope> commands_on_leadership;
    std::vector<ReadIndexRequest> reads_on_leadership;
    std::size_t max_pending_reads{128};
    std::size_t max_read_history{4'096};
    SimulationObserver observer;
};

[[nodiscard]] simulation::NodeFactory make_simulation_factory(
    SimulationConfig config);

}  // namespace kura::metadata::raft::election
