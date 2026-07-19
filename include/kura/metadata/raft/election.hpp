#pragma once

#include "kura/metadata/raft/figure2_spec.hpp"
#include "kura/metadata/raft/simulation.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
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

using Input = std::variant<
    ElectionDeadline,
    figure2::ReceiveRequestVote,
    figure2::ReceiveRequestVoteResponse,
    RaftHardStatePersisted>;

struct ResetElectionDeadline {
    simulation::TimerId timer_id{};
    simulation::LogicalTime delay{};
};

struct CancelElectionDeadline {
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
    figure2::SendRequestVote,
    figure2::SendRequestVoteResponse,
    ResetElectionDeadline,
    CancelElectionDeadline>;

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
        std::vector<LogEntry> recovered_log = {});

    [[nodiscard]] StepResult start();
    [[nodiscard]] StepResult step(const Input& input);
    [[nodiscard]] Snapshot snapshot() const;
    [[nodiscard]] const figure2::State& specification_state() const noexcept;

private:
    [[nodiscard]] StepResult translate(
        figure2::StepResult result,
        RaftRole previous_role);
    [[nodiscard]] ResetElectionDeadline next_deadline();

    figure2::State state_;
    TimeoutRange timeouts_;
    std::uint64_t random_state_{};
    simulation::TimerId next_timer_id_{1};
    std::optional<simulation::TimerId> election_timer_;
    std::optional<simulation::LogicalTime> election_timeout_;
    bool started_{};
};

using SimulationObserver = std::function<void(NodeId, const Snapshot&)>;

struct SimulationConfig {
    TimeoutRange timeouts;
    std::uint64_t seed{};
    bool mix_node_id_into_seed{true};
    SimulationObserver observer;
};

[[nodiscard]] simulation::NodeFactory make_simulation_factory(
    SimulationConfig config);

}  // namespace kura::metadata::raft::election
