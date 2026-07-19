#include "kura/metadata/raft/election.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kura::metadata::raft::election {
namespace {

constexpr std::uint8_t request_vote_tag = 1;
constexpr std::uint8_t request_vote_response_tag = 2;
constexpr std::uint8_t hard_state_tag = 3;
void append_u64(simulation::Bytes& bytes, const std::uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        bytes.push_back(
            static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

std::uint64_t read_u64(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    if (bytes.size() - offset < sizeof(std::uint64_t)) {
        throw std::invalid_argument("truncated Raft election payload");
    }
    std::uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        value |= static_cast<std::uint64_t>(bytes[offset++]) << shift;
    }
    return value;
}

simulation::Bytes encode(const RequestVoteRequest& request) {
    simulation::Bytes bytes;
    bytes.reserve(33);
    bytes.push_back(request_vote_tag);
    append_u64(bytes, request.term.value);
    append_u64(bytes, request.candidate.value);
    append_u64(bytes, request.last_log_index.value);
    append_u64(bytes, request.last_log_term.value);
    return bytes;
}

simulation::Bytes encode(const RequestVoteResponse& response) {
    simulation::Bytes bytes;
    bytes.reserve(10);
    bytes.push_back(request_vote_response_tag);
    append_u64(bytes, response.term.value);
    bytes.push_back(response.granted ? 1 : 0);
    return bytes;
}

simulation::Bytes encode(const RaftHardState& state) {
    simulation::Bytes bytes;
    bytes.reserve(18);
    bytes.push_back(hard_state_tag);
    append_u64(bytes, state.current_term.value);
    bytes.push_back(state.voted_for ? 1 : 0);
    append_u64(bytes, state.voted_for ? state.voted_for->value : 0);
    return bytes;
}

RaftHardState decode_hard_state(const simulation::Bytes& bytes) {
    if (bytes.size() != 18 || bytes.front() != hard_state_tag) {
        throw std::invalid_argument("invalid simulated Raft hard state");
    }
    std::size_t offset = 1;
    RaftHardState state{.current_term = {read_u64(bytes, offset)}};
    const auto has_vote = bytes[offset++];
    const auto vote = read_u64(bytes, offset);
    if (has_vote > 1 || (!has_vote && vote != 0)
        || (has_vote && vote == 0)) {
        throw std::invalid_argument("invalid simulated Raft vote");
    }
    if (has_vote) {
        state.voted_for = NodeId{vote};
    }
    return state;
}

void validate_recovered_transition(
    const RaftHardState& previous,
    const RaftHardState& next) {
    if (next.current_term < previous.current_term
        || (next.current_term == Term{} && next.voted_for)) {
        throw std::invalid_argument("invalid simulated Raft hard-state term");
    }
    if (next.current_term == previous.current_term
        && previous.voted_for
        && next.voted_for != previous.voted_for) {
        throw std::invalid_argument("simulated Raft vote changed in one term");
    }
}

std::variant<RequestVoteRequest, RequestVoteResponse> decode_message(
    const simulation::Bytes& bytes) {
    if (bytes.empty()) {
        throw std::invalid_argument("empty Raft election message");
    }
    std::size_t offset = 1;
    if (bytes.front() == request_vote_tag && bytes.size() == 33) {
        return RequestVoteRequest{
            .term = {read_u64(bytes, offset)},
            .candidate = {read_u64(bytes, offset)},
            .last_log_index = {read_u64(bytes, offset)},
            .last_log_term = {read_u64(bytes, offset)}};
    }
    if (bytes.front() == request_vote_response_tag && bytes.size() == 10) {
        const auto term = read_u64(bytes, offset);
        const auto granted = bytes[offset];
        if (granted > 1) {
            throw std::invalid_argument("invalid RequestVote response");
        }
        return RequestVoteResponse{{term}, granted != 0};
    }
    throw std::invalid_argument("unknown Raft election message");
}

std::uint64_t splitmix64(std::uint64_t& state) {
    state += 0x9e3779b97f4a7c15ULL;
    auto value = state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

class SimulationNode final : public simulation::NodeAdapter {
public:
    SimulationNode(NodeId expected, SimulationConfig config)
        : expected_(expected), config_(std::move(config)) {}

    std::vector<simulation::NodeAction> step(
        const simulation::NodeEvent& event) override {
        if (const auto* start = std::get_if<simulation::StartEvent>(&event)) {
            return handle_start(*start);
        }
        if (!core_) {
            throw std::logic_error("Raft election node has not started");
        }
        if (core_->snapshot().waiting_for_persistence
            && !std::holds_alternative<simulation::PersistedEvent>(event)) {
            queued_.push_back(event);
            return {};
        }
        auto actions = handle_running(event);
        while (!queued_.empty()
               && !core_->snapshot().waiting_for_persistence) {
            const auto next = std::move(queued_.front());
            queued_.erase(queued_.begin());
            auto more = handle_running(next);
            actions.insert(
                actions.end(),
                std::make_move_iterator(more.begin()),
                std::make_move_iterator(more.end()));
        }
        observe();
        return actions;
    }

private:
    std::vector<simulation::NodeAction> handle_start(
        const simulation::StartEvent& event) {
        if (core_ || event.self != expected_) {
            throw std::invalid_argument("invalid Raft election start event");
        }
        RaftHardState recovered;
        for (const auto& record : event.durable_records) {
            const auto decoded = decode_hard_state(record.bytes);
            validate_recovered_transition(recovered, decoded);
            recovered = decoded;
        }
        auto seed = config_.seed;
        if (config_.mix_node_id_into_seed) {
            seed ^= event.self.value * 0x9e3779b97f4a7c15ULL;
        }
        core_.emplace(
            event.self,
            event.peers,
            recovered,
            seed,
            config_.timeouts);
        auto actions = translate(core_->start());
        observe();
        return actions;
    }

    std::vector<simulation::NodeAction> handle_running(
        const simulation::NodeEvent& event) {
        if (const auto* message = std::get_if<simulation::MessageEvent>(&event)) {
            const auto decoded = decode_message(message->bytes);
            if (const auto* request =
                    std::get_if<RequestVoteRequest>(&decoded)) {
                return translate(core_->step(figure2::ReceiveRequestVote{
                    .from = message->from, .request = *request}));
            }
            return translate(core_->step(figure2::ReceiveRequestVoteResponse{
                .from = message->from,
                .response = std::get<RequestVoteResponse>(decoded)}));
        }
        if (std::holds_alternative<simulation::TimerEvent>(event)) {
            const auto timer = std::get<simulation::TimerEvent>(event);
            return translate(core_->step(ElectionDeadline{timer.timer_id}));
        }
        if (const auto* persisted =
                std::get_if<simulation::PersistedEvent>(&event)) {
            const auto& specification = core_->specification_state();
            if (!specification.pending_hard_state) {
                throw std::invalid_argument(
                    "unexpected simulated hard-state completion");
            }
            return translate(core_->step(RaftHardStatePersisted{
                .request_id = persisted->request_id,
                .state = specification.pending_hard_state->request.state}));
        }
        throw std::invalid_argument("duplicate Raft election start event");
    }

    std::vector<simulation::NodeAction> translate(StepResult result) {
        std::vector<simulation::NodeAction> actions;
        for (const auto& effect : result.effects) {
            std::visit(
                [&actions](const auto& typed) {
                    using Typed = std::decay_t<decltype(typed)>;
                    if constexpr (
                        std::is_same_v<Typed, PersistRaftHardState>) {
                        actions.emplace_back(simulation::PersistAction{
                            .request_id = typed.request_id,
                            .bytes = encode(typed.state)});
                    } else if constexpr (
                        std::is_same_v<Typed, figure2::SendRequestVote>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.request)});
                    } else if constexpr (
                        std::is_same_v<
                            Typed,
                            figure2::SendRequestVoteResponse>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.response)});
                    } else if constexpr (
                        std::is_same_v<Typed, ResetElectionDeadline>) {
                        actions.emplace_back(simulation::SetTimerAction{
                            .timer_id = typed.timer_id,
                            .delay = typed.delay});
                    } else if constexpr (
                        std::is_same_v<Typed, CancelElectionDeadline>) {
                        actions.emplace_back(simulation::CancelTimerAction{
                            .timer_id = typed.timer_id});
                    }
                },
                effect);
        }
        return actions;
    }

    void observe() const {
        if (config_.observer) {
            config_.observer(expected_, core_->snapshot());
        }
    }

    NodeId expected_;
    SimulationConfig config_;
    std::optional<Core> core_;
    std::vector<simulation::NodeEvent> queued_;
};

}  // namespace

Core::Core(
    const NodeId self,
    std::vector<NodeId> peers,
    const RaftHardState recovered,
    const std::uint64_t seed,
    const TimeoutRange timeouts,
    std::vector<LogEntry> recovered_log)
    : state_{
          .node = self,
          .peers = std::move(peers),
          .persistent = {
              .current_term = recovered.current_term,
              .voted_for = recovered.voted_for,
              .log = std::move(recovered_log)}},
      timeouts_(timeouts),
      random_state_(seed) {
    if (self.value == 0) {
        throw std::invalid_argument("Raft node ID must be nonzero");
    }
    if (std::ranges::any_of(
            state_.peers,
            [](const NodeId peer) {
                return peer.value == 0;
            })) {
        throw std::invalid_argument("Raft peer IDs must be nonzero");
    }
    const auto cluster_size = state_.peers.size() + 1;
    if (cluster_size < 3 || cluster_size % 2 == 0) {
        throw std::invalid_argument(
            "Raft election cluster must have an odd size of at least three");
    }
    if (timeouts_.minimum == 0 || timeouts_.maximum < timeouts_.minimum) {
        throw std::invalid_argument("invalid Raft election timeout range");
    }
    if (recovered.current_term == Term{} && recovered.voted_for) {
        throw std::invalid_argument("bootstrap term cannot contain a vote");
    }
    if (recovered.voted_for
        && *recovered.voted_for != self
        && std::ranges::find(state_.peers, *recovered.voted_for)
            == state_.peers.end()) {
        throw std::invalid_argument("recovered vote is outside the cluster");
    }
    const auto violations = figure2::validate(state_);
    if (!violations.empty()) {
        throw std::invalid_argument(violations.front().message);
    }
}

StepResult Core::start() {
    if (started_) {
        throw std::logic_error("Raft election core already started");
    }
    started_ = true;
    auto deadline = next_deadline();
    return {.effects = {deadline}};
}

StepResult Core::step(const Input& input) {
    if (!started_) {
        throw std::logic_error("Raft election core has not started");
    }
    if (const auto* deadline = std::get_if<ElectionDeadline>(&input)) {
        if (!election_timer_ || deadline->timer_id != *election_timer_) {
            return {};
        }
        election_timer_.reset();
        election_timeout_.reset();
        const auto previous = state_.role;
        return translate(
            figure2::step(state_, figure2::ElectionTimeout{}),
            previous);
    }
    const auto previous = state_.role;
    return std::visit(
        [this, previous](const auto& typed) -> StepResult {
            using Typed = std::decay_t<decltype(typed)>;
            if constexpr (std::is_same_v<Typed, ElectionDeadline>) {
                return {};
            } else {
                return translate(figure2::step(state_, typed), previous);
            }
        },
        input);
}

Snapshot Core::snapshot() const {
    std::vector<NodeId> votes(
        state_.votes_received.begin(), state_.votes_received.end());
    return {
        .role = state_.role,
        .hard_state = {
            .current_term = state_.persistent.current_term,
            .voted_for = state_.persistent.voted_for},
        .votes_received = std::move(votes),
        .election_timer = election_timer_,
        .election_timeout = election_timeout_,
        .waiting_for_persistence = state_.pending_hard_state.has_value()};
}

const figure2::State& Core::specification_state() const noexcept {
    return state_;
}

StepResult Core::translate(
    figure2::StepResult result,
    const RaftRole previous_role) {
    state_ = std::move(result.state);
    StepResult translated{.rules = std::move(result.rules)};
    if (state_.role != previous_role) {
        translated.effects.emplace_back(RoleTransition{
            .from = previous_role,
            .to = state_.role,
            .term = state_.persistent.current_term});
        if (state_.role == RaftRole::leader && election_timer_) {
            translated.effects.emplace_back(
                CancelElectionDeadline{*election_timer_});
            election_timer_.reset();
            election_timeout_.reset();
        }
    }
    for (auto& effect : result.effects) {
        std::visit(
            [this, &translated](auto&& typed) {
                using Typed = std::decay_t<decltype(typed)>;
                if constexpr (
                    std::is_same_v<Typed, PersistRaftHardState>
                    || std::is_same_v<Typed, figure2::SendRequestVote>
                    || std::is_same_v<
                        Typed,
                        figure2::SendRequestVoteResponse>) {
                    translated.effects.emplace_back(
                        std::forward<decltype(typed)>(typed));
                } else if constexpr (
                    std::is_same_v<Typed, figure2::ResetElectionTimer>) {
                    translated.effects.emplace_back(next_deadline());
                }
            },
            std::move(effect));
    }
    return translated;
}

ResetElectionDeadline Core::next_deadline() {
    if (next_timer_id_ == 0
        || next_timer_id_ == std::numeric_limits<simulation::TimerId>::max()) {
        throw std::overflow_error("Raft election timer ID exhausted");
    }
    const auto width = timeouts_.maximum - timeouts_.minimum;
    const auto delay = width == 0
        ? timeouts_.minimum
        : timeouts_.minimum + splitmix64(random_state_) % (width + 1);
    const auto timer = next_timer_id_++;
    election_timer_ = timer;
    election_timeout_ = delay;
    return {.timer_id = timer, .delay = delay};
}

simulation::NodeFactory make_simulation_factory(SimulationConfig config) {
    return [config = std::move(config)](const NodeId node) {
        return std::make_unique<SimulationNode>(node, config);
    };
}

}  // namespace kura::metadata::raft::election
