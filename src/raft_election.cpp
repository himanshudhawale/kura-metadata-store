#include "kura/metadata/raft/election.hpp"
#include "kura/metadata/storage/checksum.hpp"

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
constexpr std::uint8_t append_entries_tag = 4;
constexpr std::uint8_t append_entries_response_tag = 5;
constexpr std::uint8_t log_state_tag = 6;
constexpr std::uint8_t applied_state_tag = 7;
constexpr std::uint8_t install_snapshot_tag = 8;
constexpr std::uint8_t install_snapshot_response_tag = 9;
constexpr std::uint8_t snapshot_state_tag = 10;

void append_u32(simulation::Bytes& bytes, const std::uint32_t value) {
    for (unsigned shift = 0; shift < 32; shift += 8) {
        bytes.push_back(
            static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

void append_u64(simulation::Bytes& bytes, const std::uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        bytes.push_back(
            static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

std::uint32_t read_u32(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    if (offset > bytes.size() || bytes.size() - offset < sizeof(std::uint32_t)) {
        throw std::invalid_argument("truncated Raft payload");
    }
    std::uint32_t value = 0;
    for (unsigned shift = 0; shift < 32; shift += 8) {
        value |= static_cast<std::uint32_t>(bytes[offset++]) << shift;
    }
    return value;
}

std::uint64_t read_u64(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    if (offset > bytes.size() || bytes.size() - offset < sizeof(std::uint64_t)) {
        throw std::invalid_argument("truncated Raft payload");
    }

    std::uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        value |= static_cast<std::uint64_t>(bytes[offset++]) << shift;
    }
    return value;
}

void encode_metadata(
    simulation::Bytes& bytes,
    const SnapshotMetadata& metadata) {
    append_u64(bytes, metadata.last_included_index.value);
    append_u64(bytes, metadata.last_included_term.value);
    append_u64(
        bytes, static_cast<std::uint64_t>(metadata.store_revision.value));
    append_u64(
        bytes,
        static_cast<std::uint64_t>(metadata.compaction_revision.value));
    append_u32(bytes, metadata.format_version);
    append_u64(bytes, metadata.membership.voters.size());
    for (const auto node : metadata.membership.voters) {
        append_u64(bytes, node.value);
    }
    append_u64(bytes, metadata.membership.learners.size());
    for (const auto node : metadata.membership.learners) {
        append_u64(bytes, node.value);
    }
}

SnapshotMetadata decode_metadata(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    SnapshotMetadata metadata{
        .last_included_index = {read_u64(bytes, offset)},
        .last_included_term = {read_u64(bytes, offset)},
        .store_revision = {
            static_cast<std::int64_t>(read_u64(bytes, offset))},
        .compaction_revision = {
            static_cast<std::int64_t>(read_u64(bytes, offset))},
        .format_version = read_u32(bytes, offset)};
    const auto voters = read_u64(bytes, offset);
    if (voters > (bytes.size() - offset) / 8) {
        throw std::invalid_argument("invalid snapshot voter count");
    }
    for (std::uint64_t index = 0; index < voters; ++index) {
        metadata.membership.voters.push_back(
            NodeId{read_u64(bytes, offset)});
    }
    const auto learners = read_u64(bytes, offset);
    if (learners > (bytes.size() - offset) / 8) {
        throw std::invalid_argument("invalid snapshot learner count");
    }
    for (std::uint64_t index = 0; index < learners; ++index) {
        metadata.membership.learners.push_back(
            NodeId{read_u64(bytes, offset)});
    }
    return metadata;
}

void encode_entry(simulation::Bytes& bytes, const LogEntry& entry) {
    append_u64(bytes, entry.term.value);
    append_u64(bytes, entry.index.value);
    append_u64(bytes, entry.command.request_id.client);
    append_u64(bytes, entry.command.request_id.sequence);
    append_u32(bytes, entry.command.type_tag);
    append_u64(bytes, entry.command.payload.size());
    bytes.insert(
        bytes.end(),
        entry.command.payload.begin(),
        entry.command.payload.end());
}

void encode_context(
    simulation::Bytes& bytes,
    const std::optional<ReadIndexContext> context) {
    bytes.push_back(context ? 1 : 0);
    append_u64(bytes, context ? context->value : 0);
}

std::optional<ReadIndexContext> decode_context(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    if (offset >= bytes.size()) {
        throw std::invalid_argument("truncated ReadIndex context");
    }
    const auto present = bytes[offset++];
    const auto value = read_u64(bytes, offset);
    if (present > 1 || (!present && value != 0)
        || (present && value == 0)) {
        throw std::invalid_argument("invalid ReadIndex context");
    }
    return present
        ? std::optional<ReadIndexContext>{ReadIndexContext{value}}
        : std::nullopt;
}

LogEntry decode_entry(
    const simulation::Bytes& bytes,
    std::size_t& offset) {
    LogEntry entry{
        .term = {read_u64(bytes, offset)},
        .index = {read_u64(bytes, offset)},
        .command = {
            .request_id = {
                .client = read_u64(bytes, offset),
                .sequence = read_u64(bytes, offset)},
            .type_tag = read_u32(bytes, offset)}};
    const auto payload_size = read_u64(bytes, offset);
    if (payload_size > bytes.size() - offset) {
        throw std::invalid_argument("truncated Raft command payload");
    }
    entry.command.payload.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(offset),
        bytes.begin() + static_cast<std::ptrdiff_t>(offset + payload_size));
    offset += static_cast<std::size_t>(payload_size);
    return entry;
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

simulation::Bytes encode(const AppendEntriesRequest& request) {
    simulation::Bytes bytes;
    bytes.push_back(append_entries_tag);
    append_u64(bytes, request.term.value);
    append_u64(bytes, request.leader.value);
    append_u64(bytes, request.previous_log_index.value);
    append_u64(bytes, request.previous_log_term.value);
    append_u64(bytes, request.leader_commit.value);
    encode_context(bytes, request.read_context);
    append_u64(bytes, request.entries.size());
    for (const auto& entry : request.entries) {
        encode_entry(bytes, entry);
    }
    return bytes;
}

simulation::Bytes encode(const AppendEntriesResponse& response) {
    simulation::Bytes bytes;
    bytes.reserve(27);
    bytes.push_back(append_entries_response_tag);
    append_u64(bytes, response.term.value);
    bytes.push_back(response.succeeded ? 1 : 0);
    append_u64(bytes, response.matched_index.value);
    encode_context(bytes, response.read_context);
    return bytes;
}

simulation::Bytes encode(const InstallSnapshotRequest& request) {
    simulation::Bytes bytes;
    bytes.push_back(install_snapshot_tag);
    append_u64(bytes, request.term.value);
    append_u64(bytes, request.leader.value);
    append_u64(bytes, request.transfer_id);
    encode_metadata(bytes, request.metadata);
    append_u64(bytes, request.offset);
    append_u64(bytes, request.total_size);
    append_u32(bytes, request.state_checksum);
    bytes.push_back(request.done ? 1 : 0);
    encode_context(bytes, request.read_context);
    append_u64(bytes, request.chunk.size());
    bytes.insert(bytes.end(), request.chunk.begin(), request.chunk.end());
    return bytes;
}

simulation::Bytes encode(const InstallSnapshotResponse& response) {
    simulation::Bytes bytes;
    bytes.push_back(install_snapshot_response_tag);
    append_u64(bytes, response.term.value);
    append_u64(bytes, response.transfer_id);
    bytes.push_back(response.succeeded ? 1 : 0);
    append_u64(bytes, response.last_included_index.value);
    encode_context(bytes, response.read_context);
    return bytes;
}

simulation::Bytes encode_snapshot_state(const RaftSnapshot& value) {
    simulation::Bytes bytes;
    bytes.push_back(snapshot_state_tag);
    encode_metadata(bytes, value.metadata);
    append_u32(bytes, crc32c(value.state));
    append_u64(bytes, value.state.size());
    bytes.insert(bytes.end(), value.state.begin(), value.state.end());
    return bytes;
}

RaftSnapshot decode_snapshot_state(
    const simulation::Bytes& bytes) {
    if (bytes.empty() || bytes.front() != snapshot_state_tag) {
        throw std::invalid_argument("invalid simulated Raft snapshot");
    }
    std::size_t offset = 1;
    auto metadata = decode_metadata(bytes, offset);
    const auto checksum = read_u32(bytes, offset);
    const auto size = read_u64(bytes, offset);
    if (size > bytes.size() - offset) {
        throw std::invalid_argument("truncated simulated Raft snapshot");
    }
    std::vector<std::uint8_t> state(
        bytes.begin() + static_cast<std::ptrdiff_t>(offset),
        bytes.end());
    if (state.size() != size || crc32c(state) != checksum) {
        throw std::invalid_argument("corrupt simulated Raft snapshot");
    }
    return {
        .metadata = std::move(metadata),
        .state = std::move(state),
        .checksum = checksum};
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

simulation::Bytes encode_log(const std::vector<LogEntry>& log) {
    simulation::Bytes bytes;
    bytes.push_back(log_state_tag);
    append_u64(bytes, log.size());
    for (const auto& entry : log) {
        encode_entry(bytes, entry);
    }
    return bytes;
}

std::vector<LogEntry> decode_log(const simulation::Bytes& bytes) {
    if (bytes.empty() || bytes.front() != log_state_tag) {
        throw std::invalid_argument("invalid simulated Raft log");
    }
    std::size_t offset = 1;
    const auto count = read_u64(bytes, offset);
    if (count > (bytes.size() - offset) / 44) {
        throw std::invalid_argument("invalid simulated Raft log count");
    }
    std::vector<LogEntry> log;
    log.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t index = 0; index < count; ++index) {
        log.push_back(decode_entry(bytes, offset));
    }
    if (offset != bytes.size()) {
        throw std::invalid_argument("trailing bytes in simulated Raft log");
    }
    return log;
}

simulation::Bytes encode_applied(const LogIndex index) {
    simulation::Bytes bytes;
    bytes.reserve(9);
    bytes.push_back(applied_state_tag);
    append_u64(bytes, index.value);
    return bytes;
}

LogIndex decode_applied(const simulation::Bytes& bytes) {
    if (bytes.size() != 9 || bytes.front() != applied_state_tag) {
        throw std::invalid_argument("invalid simulated applied state");
    }
    std::size_t offset = 1;
    const LogIndex index{read_u64(bytes, offset)};
    if (index == LogIndex{}) {
        throw std::invalid_argument(
            "simulated applied index must be nonzero");
    }
    return index;
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

using DecodedMessage = std::variant<
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse>;

DecodedMessage decode_message(
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
    if (bytes.front() == append_entries_tag) {
        AppendEntriesRequest request;
        request.term = {read_u64(bytes, offset)};
        request.leader = {read_u64(bytes, offset)};
        request.previous_log_index = {read_u64(bytes, offset)};
        request.previous_log_term = {read_u64(bytes, offset)};
        request.leader_commit = {read_u64(bytes, offset)};
        request.read_context = decode_context(bytes, offset);
        const auto count = read_u64(bytes, offset);
        if (count > (bytes.size() - offset) / 44) {
            throw std::invalid_argument("invalid AppendEntries count");
        }
        request.entries.reserve(static_cast<std::size_t>(count));
        for (std::uint64_t index = 0; index < count; ++index) {
            request.entries.push_back(decode_entry(bytes, offset));
        }
        if (offset != bytes.size()) {
            throw std::invalid_argument("trailing bytes in AppendEntries");
        }
        return request;
    }
    if (bytes.front() == append_entries_response_tag && bytes.size() == 27) {
        const auto term = read_u64(bytes, offset);
        const auto succeeded = bytes[offset++];
        const auto matched = read_u64(bytes, offset);
        const auto context = decode_context(bytes, offset);
        if (succeeded > 1) {
            throw std::invalid_argument("invalid AppendEntries response");
        }
        return AppendEntriesResponse{
            {term}, succeeded != 0, {matched}, context};
    }
    if (bytes.front() == install_snapshot_tag) {
        InstallSnapshotRequest request;
        request.term = {read_u64(bytes, offset)};
        request.leader = {read_u64(bytes, offset)};
        request.transfer_id = read_u64(bytes, offset);
        request.metadata = decode_metadata(bytes, offset);
        request.offset = read_u64(bytes, offset);
        request.total_size = read_u64(bytes, offset);
        request.state_checksum = read_u32(bytes, offset);
        if (offset >= bytes.size()) {
            throw std::invalid_argument("truncated InstallSnapshot");
        }
        const auto done = bytes[offset++];
        if (done > 1) {
            throw std::invalid_argument("invalid InstallSnapshot done flag");
        }
        request.done = done != 0;
        request.read_context = decode_context(bytes, offset);
        const auto size = read_u64(bytes, offset);
        if (size > bytes.size() - offset) {
            throw std::invalid_argument("truncated InstallSnapshot chunk");
        }
        request.chunk.assign(
            bytes.begin() + static_cast<std::ptrdiff_t>(offset),
            bytes.end());
        if (request.chunk.size() != size) {
            throw std::invalid_argument("invalid InstallSnapshot chunk size");
        }
        return request;
    }
    if (bytes.front() == install_snapshot_response_tag) {
        InstallSnapshotResponse response{
            .term = {read_u64(bytes, offset)},
            .transfer_id = read_u64(bytes, offset)};
        if (offset >= bytes.size()) {
            throw std::invalid_argument(
                "truncated InstallSnapshot response");
        }
        const auto succeeded = bytes[offset++];
        response.last_included_index = {read_u64(bytes, offset)};
        response.read_context = decode_context(bytes, offset);
        if (succeeded > 1 || offset != bytes.size()) {
            throw std::invalid_argument(
                "invalid InstallSnapshot response");
        }
        response.succeeded = succeeded != 0;
        return response;
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

void validate_append_request(const AppendEntriesRequest& request) {
    if (request.term == Term{} || request.leader.value == 0) {
        throw std::invalid_argument(
            "AppendEntries term and leader must be nonzero");
    }
    if (request.previous_log_index == LogIndex{}) {
        if (request.previous_log_term != Term{}) {
            throw std::invalid_argument(
                "AppendEntries index zero must have term zero");
        }
    } else if (request.previous_log_term == Term{}) {
        throw std::invalid_argument(
            "AppendEntries nonzero index must have a nonzero term");
    }
    auto expected = request.previous_log_index.value;
    for (const auto& entry : request.entries) {
        if (expected == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error("AppendEntries index exhausted");
        }
        ++expected;
        if (entry.index.value != expected || entry.term == Term{}
            || entry.term > request.term) {
            throw std::invalid_argument(
                "AppendEntries entries have invalid term/index ordering");
        }
    }
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
        if (core_->snapshot().role == RaftRole::leader
            && !core_->snapshot().waiting_for_persistence
            && next_leader_command_
                < config_.commands_on_leadership.size()) {
            auto proposal = translate(core_->step(
                figure2::ReceiveClientCommand{
                    config_.commands_on_leadership[next_leader_command_++]}));
            actions.insert(
                actions.end(),
                std::make_move_iterator(proposal.begin()),
                std::make_move_iterator(proposal.end()));
        }
        const auto snapshot = core_->snapshot();
        if (!snapshot.waiting_for_persistence
            && next_snapshot_ < config_.snapshots_on_apply.size()
            && config_.snapshots_on_apply[next_snapshot_].applied_index
                == snapshot.last_applied
            && (!snapshot.snapshot_metadata
                || snapshot.snapshot_metadata->last_included_index
                    < snapshot.last_applied)) {
            auto created = translate(core_->step(
                config_.snapshots_on_apply[next_snapshot_++]));
            actions.insert(
                actions.end(),
                std::make_move_iterator(created.begin()),
                std::make_move_iterator(created.end()));
        }
        const auto after_snapshot = core_->snapshot();
        const auto base = after_snapshot.snapshot_metadata
            ? after_snapshot.snapshot_metadata->last_included_index.value
            : 0;
        const bool current_term_committed =
            after_snapshot.commit_index != LogIndex{}
            && (after_snapshot.commit_index.value == base
                    ? after_snapshot.snapshot_metadata
                          ->last_included_term
                    : after_snapshot.log.at(
                          after_snapshot.commit_index.value - base - 1).term)
                == after_snapshot.hard_state.current_term;
        if (after_snapshot.role == RaftRole::leader
            && !after_snapshot.waiting_for_persistence
            && current_term_committed
            && next_read_request_ < config_.reads_on_leadership.size()) {
            auto read = translate(core_->step(
                config_.reads_on_leadership[next_read_request_++]));
            actions.insert(
                actions.end(),
                std::make_move_iterator(read.begin()),
                std::make_move_iterator(read.end()));
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
        std::vector<LogEntry> recovered_log;
        LogIndex recovered_applied;
        std::optional<RaftSnapshot> recovered_snapshot;
        for (const auto& record : event.durable_records) {
            if (record.bytes.empty()) {
                throw std::invalid_argument("empty simulated Raft record");
            }
            if (record.bytes.front() == hard_state_tag) {
                const auto decoded = decode_hard_state(record.bytes);
                validate_recovered_transition(recovered, decoded);
                recovered = decoded;
            } else if (record.bytes.front() == log_state_tag) {
                recovered_log = decode_log(record.bytes);
            } else if (record.bytes.front() == applied_state_tag) {
                const auto applied = decode_applied(record.bytes);
                if (applied < recovered_applied) {
                    throw std::invalid_argument(
                        "simulated applied index regressed");
                }
                recovered_applied = applied;
            } else if (record.bytes.front() == snapshot_state_tag) {
                const auto decoded = decode_snapshot_state(record.bytes);
                if (recovered_snapshot
                    && decoded.metadata.last_included_index
                        <= recovered_snapshot->metadata.last_included_index) {
                    throw std::invalid_argument(
                        "simulated Raft snapshot regressed");
                }
                recovered_snapshot = decoded;
            } else {
                throw std::invalid_argument("unknown simulated Raft record");
            }
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
            config_.timeouts,
            std::move(recovered_log),
            config_.heartbeat_interval,
            recovered_applied,
            config_.max_pending_reads,
            config_.max_read_history,
            std::move(recovered_snapshot));
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
            if (const auto* response =
                    std::get_if<RequestVoteResponse>(&decoded)) {
                return translate(core_->step(
                    figure2::ReceiveRequestVoteResponse{
                        .from = message->from,
                        .response = *response}));
            }
            if (const auto* request =
                    std::get_if<AppendEntriesRequest>(&decoded)) {
                return translate(core_->step(figure2::ReceiveAppendEntries{
                    .from = message->from, .request = *request}));
            }
            if (const auto* response =
                    std::get_if<AppendEntriesResponse>(&decoded)) {
                return translate(core_->step(
                    figure2::ReceiveAppendEntriesResponse{
                        .from = message->from,
                        .response = *response}));
            }
            if (const auto* request =
                    std::get_if<InstallSnapshotRequest>(&decoded)) {
                return translate(core_->step(ReceiveInstallSnapshot{
                    .from = message->from,
                    .request = *request}));
            }
            return translate(core_->step(
                ReceiveInstallSnapshotResponse{
                    .from = message->from,
                    .response =
                        std::get<InstallSnapshotResponse>(decoded)}));
        }
        if (std::holds_alternative<simulation::TimerEvent>(event)) {
            const auto timer = std::get<simulation::TimerEvent>(event);
            if (core_->snapshot().heartbeat_timer == timer.timer_id) {
                return translate(
                    core_->step(HeartbeatDeadline{timer.timer_id}));
            }
            return translate(core_->step(ElectionDeadline{timer.timer_id}));
        }
        if (const auto* persisted =
                std::get_if<simulation::PersistedEvent>(&event)) {
            if (const auto* pending = core_->pending_log_persistence();
                pending && pending->request_id == persisted->request_id) {
                return translate(core_->step(RaftLogPersisted{
                    .request_id = persisted->request_id,
                    .log = pending->log}));
            }
            if (const auto* pending =
                    core_->pending_snapshot_persistence();
                pending && pending->request_id == persisted->request_id) {
                return translate(core_->step(RaftSnapshotPersisted{
                    .request_id = persisted->request_id,
                    .snapshot = pending->snapshot}));
            }
            const auto& specification = core_->specification_state();
            if (specification.pending_hard_state
                && specification.pending_hard_state->request.request_id
                    == persisted->request_id) {
                return translate(core_->step(RaftHardStatePersisted{
                    .request_id = persisted->request_id,
                    .state =
                        specification.pending_hard_state->request.state}));
            }
            if (const auto* pending = core_->pending_application();
                pending && pending->request_id == persisted->request_id) {
                return translate(core_->step(LogEntryApplied{
                    .request_id = persisted->request_id,
                    .index = pending->entry.index}));
            }
            throw std::invalid_argument(
                "unexpected simulated persistence completion");
        }
        throw std::invalid_argument("duplicate Raft election start event");
    }

    std::vector<simulation::NodeAction> translate(StepResult result) {
        std::vector<simulation::NodeAction> actions;
        std::vector<RestoreStateMachineSnapshot> restores;
        for (const auto& effect : result.effects) {
            std::visit(
                [&actions, &restores](const auto& typed) {
                    using Typed = std::decay_t<decltype(typed)>;
                    if constexpr (
                        std::is_same_v<Typed, PersistRaftHardState>) {
                        actions.emplace_back(simulation::PersistAction{
                            .request_id = typed.request_id,
                            .bytes = encode(typed.state)});
                    } else if constexpr (
                        std::is_same_v<Typed, PersistRaftLog>) {
                        actions.emplace_back(simulation::PersistAction{
                            .request_id = typed.request_id,
                            .bytes = encode_log(typed.log)});
                    } else if constexpr (
                        std::is_same_v<Typed, PersistRaftSnapshot>) {
                        actions.emplace_back(simulation::PersistAction{
                            .request_id = typed.request_id,
                            .bytes = encode_snapshot_state(typed.snapshot)});
                    } else if constexpr (
                        std::is_same_v<Typed, ApplyLogEntry>) {
                        actions.emplace_back(simulation::PersistAction{
                            .request_id = typed.request_id,
                            .bytes = encode_applied(typed.entry.index)});
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
                        std::is_same_v<Typed, figure2::SendAppendEntries>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.request)});
                    } else if constexpr (
                        std::is_same_v<
                            Typed,
                            figure2::SendAppendEntriesResponse>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.response)});
                    } else if constexpr (
                        std::is_same_v<Typed, SendInstallSnapshot>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.request)});
                    } else if constexpr (
                        std::is_same_v<
                            Typed,
                            SendInstallSnapshotResponse>) {
                        actions.emplace_back(simulation::SendAction{
                            .to = typed.to,
                            .bytes = encode(typed.response)});
                    } else if constexpr (
                        std::is_same_v<
                            Typed,
                            RestoreStateMachineSnapshot>) {
                        restores.push_back(typed);
                    } else if constexpr (
                        std::is_same_v<Typed, ResetElectionDeadline>) {
                        actions.emplace_back(simulation::SetTimerAction{
                            .timer_id = typed.timer_id,
                            .delay = typed.delay});
                    } else if constexpr (
                        std::is_same_v<Typed, CancelElectionDeadline>) {
                        actions.emplace_back(simulation::CancelTimerAction{
                            .timer_id = typed.timer_id});
                    } else if constexpr (
                        std::is_same_v<Typed, ResetHeartbeatDeadline>) {
                        actions.emplace_back(simulation::SetTimerAction{
                            .timer_id = typed.timer_id,
                            .delay = typed.delay});
                    } else if constexpr (
                        std::is_same_v<Typed, CancelHeartbeatDeadline>) {
                        actions.emplace_back(simulation::CancelTimerAction{
                            .timer_id = typed.timer_id});
                    }
                },
                effect);
        }
        for (const auto& restore : restores) {
            auto continued = translate(core_->step(
                RaftSnapshotRestored{
                    .request_id = restore.request_id,
                    .index =
                        restore.snapshot.metadata.last_included_index}));
            actions.insert(
                actions.end(),
                std::make_move_iterator(continued.begin()),
                std::make_move_iterator(continued.end()));
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
    std::size_t next_leader_command_{};
    std::size_t next_read_request_{};
    std::size_t next_snapshot_{};
};

}  // namespace

Core::Core(
    const NodeId self,
    std::vector<NodeId> peers,
    const RaftHardState recovered,
    const std::uint64_t seed,
    const TimeoutRange timeouts,
    std::vector<LogEntry> recovered_log,
    const simulation::LogicalTime heartbeat_interval,
    const LogIndex recovered_applied,
    const std::size_t max_pending_reads,
    const std::size_t max_read_history,
    std::optional<RaftSnapshot> recovered_snapshot,
    const std::size_t max_snapshot_bytes,
    const std::size_t max_snapshot_members)
    : state_{
          .node = self,
          .peers = std::move(peers),
          .persistent = {
              .current_term = recovered.current_term,
              .voted_for = recovered.voted_for,
              .log = std::move(recovered_log)},
          .volatile_state = {
              .commit_index = recovered_applied,
              .last_applied = recovered_applied}},
      timeouts_(timeouts),
      heartbeat_interval_(heartbeat_interval),
      random_state_(seed),
      max_pending_reads_(max_pending_reads),
      max_read_history_(max_read_history),
      max_snapshot_bytes_(max_snapshot_bytes),
      max_snapshot_members_(max_snapshot_members) {
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
    if (heartbeat_interval_ == 0) {
        throw std::invalid_argument("Raft heartbeat interval must be nonzero");
    }
    if (max_pending_reads_ == 0 || max_read_history_ < max_pending_reads_) {
        throw std::invalid_argument("invalid ReadIndex limits");
    }
    if (max_snapshot_bytes_ < 68) {
        throw std::invalid_argument("invalid Raft snapshot size limit");
    }
    if (max_snapshot_members_ < cluster_size) {
        throw std::invalid_argument("invalid Raft snapshot member limit");
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
    if (std::ranges::any_of(
            state_.persistent.log,
            [](const LogEntry& entry) {
                return entry.term == Term{};
            })) {
        throw std::invalid_argument(
            "recovered Raft log entries must have nonzero terms");
    }
    if (recovered_snapshot) {
        if (recovered_snapshot->metadata.last_included_index.value == 0
            || recovered_snapshot->metadata.last_included_term == Term{}
            || recovered_snapshot->metadata.last_included_term
                > recovered.current_term
            || recovered_snapshot->metadata.format_version != 1) {
            throw std::invalid_argument("invalid recovered Raft snapshot");
        }
        const auto& metadata = recovered_snapshot->metadata;
        if (metadata.store_revision.value < 0
            || metadata.compaction_revision.value < 0
            || metadata.compaction_revision
                > metadata.store_revision) {
            throw std::invalid_argument(
                "invalid recovered snapshot revisions");
        }
        std::set<NodeId> recovered_members;
        bool invalid_member = false;
        for (const auto member : metadata.membership.voters) {
            invalid_member = invalid_member || member == NodeId{}
                || !recovered_members.insert(member).second;
        }
        for (const auto member : metadata.membership.learners) {
            invalid_member = invalid_member || member == NodeId{}
                || !recovered_members.insert(member).second;
        }
        std::set<NodeId> expected_voters(state_.peers.begin(), state_.peers.end());
        expected_voters.insert(state_.node);
        const std::set<NodeId> snapshot_voters(
            metadata.membership.voters.begin(),
            metadata.membership.voters.end());
        const auto member_count = recovered_members.size();
        if (invalid_member || snapshot_voters != expected_voters
            || member_count > max_snapshot_members_
            || member_count > (max_snapshot_bytes_ - 68) / 8
            || recovered_snapshot->state.size()
                > max_snapshot_bytes_ - 68 - member_count * 8) {
            throw std::invalid_argument(
                "invalid recovered snapshot membership or size");
        }
        durable_snapshot_ = *recovered_snapshot;
        const auto index =
            recovered_snapshot->metadata.last_included_index.value;
        const auto matching = std::ranges::find_if(
            state_.persistent.log,
            [index](const LogEntry& entry) {
                return entry.index.value == index;
            });
        if (matching != state_.persistent.log.end()) {
            if (matching->term
                != recovered_snapshot->metadata.last_included_term) {
                throw std::invalid_argument(
                    "recovered snapshot conflicts with Raft log");
            }
            state_.persistent.log.erase(
                state_.persistent.log.begin(), matching + 1);
        } else if (
            !state_.persistent.log.empty()
            && state_.persistent.log.front().index.value != index + 1) {
            throw std::invalid_argument(
                "recovered log does not follow the Raft snapshot");
        }
        state_.persistent.snapshot = recovered_snapshot->metadata;
        state_.volatile_state.commit_index = {
            std::max(recovered_applied.value, index)};
        state_.volatile_state.last_applied =
            state_.volatile_state.commit_index;
    }
    const auto recovered_end = state_.persistent.log.empty()
        ? (state_.persistent.snapshot
              ? state_.persistent.snapshot->last_included_index.value
              : 0)
        : state_.persistent.log.back().index.value;
    if (state_.volatile_state.last_applied.value > recovered_end) {
        throw std::invalid_argument(
            "recovered applied index exceeds the Raft log");
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
    if (const auto* persisted =
            std::get_if<RaftSnapshotPersisted>(&input)) {
        StepResult completed;
        install_persisted_snapshot(*persisted, completed);
        complete_ready_reads(completed);
        schedule_application(completed);
        return completed;
    }
    if (const auto* restored =
            std::get_if<RaftSnapshotRestored>(&input)) {
        StepResult completed;
        complete_snapshot_restore(*restored, completed);
        complete_ready_reads(completed);
        schedule_application(completed);
        return completed;
    }
    if (pending_snapshot_ || pending_snapshot_restore_) {
        throw std::logic_error(
            "Raft core input is blocked on snapshot persistence/restore");
    }
    if (const auto* applied = std::get_if<LogEntryApplied>(&input)) {
        if (!pending_application_
            || pending_application_->blocked
            || applied->request_id
                != pending_application_->request.request_id
            || applied->index
                != pending_application_->request.entry.index) {
            throw std::invalid_argument(
                "application completion does not match pending entry");
        }
        state_.volatile_state.last_applied = applied->index;
        pending_application_.reset();
        StepResult completed;
        if (state_.leader
            && state_.leader->pending_clients.erase(applied->index) != 0) {
            completed.effects.emplace_back(
                figure2::CompleteClientCommand{applied->index});
        }
        complete_ready_reads(completed);
        schedule_application(completed);
        return completed;
    }
    if (const auto* failed = std::get_if<LogEntryApplyFailed>(&input)) {
        if (!pending_application_
            || pending_application_->blocked
            || failed->request_id
                != pending_application_->request.request_id
            || failed->index
                != pending_application_->request.entry.index) {
            throw std::invalid_argument(
                "application failure does not match pending entry");
        }
        pending_application_->blocked = true;
        return {
            .effects = {
                ApplicationBackpressured{.index = failed->index}}};
    }
    if (std::holds_alternative<RetryApplication>(input)) {
        if (!pending_application_ || !pending_application_->blocked) {
            throw std::logic_error(
                "application retry requires a blocked entry");
        }
        if (next_application_request_id_ >= (1ULL << 63)) {
            throw std::overflow_error(
                "Raft application request ID exhausted");
        }
        pending_application_->request.request_id =
            next_application_request_id_++;
        pending_application_->blocked = false;
        return {.effects = {pending_application_->request}};
    }
    if (const auto* persisted = std::get_if<RaftLogPersisted>(&input)) {
        if (!pending_log_) {
            throw std::invalid_argument(
                "unexpected Raft log persistence completion");
        }
        if (persisted->request_id != pending_log_->request.request_id
            || persisted->log != pending_log_->request.log) {
            throw std::invalid_argument(
                "Raft log completion does not match pending request");
        }
        StepResult completed{
            .effects = std::move(pending_log_->deferred_effects)};
        pending_log_.reset();
        replace_compacted_appends(completed);
        schedule_application(completed);
        return completed;
    }
    if (pending_log_) {
        throw std::logic_error(
            "Raft core input is blocked on log persistence");
    }
    if (const auto* persisted =
            std::get_if<RaftHardStatePersisted>(&input)) {
        auto completed = translate(
            figure2::step(state_, *persisted), state_.role);
        if (install_after_hard_state_
            && !state_.pending_hard_state) {
            auto install = std::move(*install_after_hard_state_);
            install_after_hard_state_.reset();
            auto continuation = receive_snapshot(install);
            completed.rules.insert(
                completed.rules.end(),
                continuation.rules.begin(),
                continuation.rules.end());
            completed.effects.insert(
                completed.effects.end(),
                std::make_move_iterator(
                    continuation.effects.begin()),
                std::make_move_iterator(
                    continuation.effects.end()));
        }
        return completed;
    }
    if (const auto* create =
            std::get_if<CreateRaftSnapshot>(&input)) {
        return begin_snapshot(*create);
    }
    if (const auto* install =
            std::get_if<ReceiveInstallSnapshot>(&input)) {
        return receive_snapshot(*install);
    }
    if (const auto* response =
            std::get_if<ReceiveInstallSnapshotResponse>(&input)) {
        return receive_snapshot_response(*response);
    }
    if (const auto* read = std::get_if<ReadIndexRequest>(&input)) {
        return begin_read(*read);
    }
    if (const auto* cancel = std::get_if<CancelReadIndex>(&input)) {
        return cancel_read(*cancel);
    }
    if (const auto* timeout = std::get_if<TimeoutReadIndex>(&input)) {
        return timeout_read(*timeout);
    }
    if (const auto* append =
            std::get_if<figure2::ReceiveAppendEntries>(&input)) {
        validate_append_request(append->request);
    }
    if (const auto* response =
            std::get_if<figure2::ReceiveAppendEntriesResponse>(&input)) {
        const auto previous = state_.role;
        auto result = translate(
            figure2::step(state_, *response),
            previous);
        acknowledge_read(*response, result);
        return result;
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
    if (const auto* heartbeat = std::get_if<HeartbeatDeadline>(&input)) {
        if (!heartbeat_timer_ || heartbeat->timer_id != *heartbeat_timer_
            || state_.role != RaftRole::leader) {
            return {};
        }
        heartbeat_timer_.reset();
        auto result = translate(
            figure2::step(state_, figure2::HeartbeatTimeout{}),
            state_.role);
        result.effects.emplace_back(next_heartbeat());
        return result;
    }
    const auto previous = state_.role;
    return std::visit(
        [this, previous](const auto& typed) -> StepResult {
            using Typed = std::decay_t<decltype(typed)>;
            if constexpr (
                std::is_same_v<Typed, ElectionDeadline>
                || std::is_same_v<Typed, HeartbeatDeadline>
                || std::is_same_v<Typed, RaftLogPersisted>
                || std::is_same_v<Typed, RaftHardStatePersisted>
                || std::is_same_v<Typed, RaftSnapshotPersisted>
                || std::is_same_v<Typed, RaftSnapshotRestored>
                || std::is_same_v<Typed, CreateRaftSnapshot>
                || std::is_same_v<Typed, ReceiveInstallSnapshot>
                || std::is_same_v<
                    Typed,
                    ReceiveInstallSnapshotResponse>
                || std::is_same_v<Typed, LogEntryApplied>
                || std::is_same_v<Typed, LogEntryApplyFailed>
                || std::is_same_v<Typed, RetryApplication>
                || std::is_same_v<Typed, ReadIndexRequest>
                || std::is_same_v<Typed, CancelReadIndex>
                || std::is_same_v<Typed, TimeoutReadIndex>
                || std::is_same_v<
                    Typed,
                    figure2::ReceiveAppendEntriesResponse>) {
                return {};
            } else {
                return translate(
                    figure2::step(state_, typed),
                    previous);
            }
        },
        input);
}

Snapshot Core::snapshot() const {
    std::vector<NodeId> votes(
        state_.votes_received.begin(), state_.votes_received.end());
    std::map<NodeId, figure2::PeerProgress> progress;
    if (state_.leader) {
        progress = state_.leader->progress;
    }
    return {
        .role = state_.role,
        .hard_state = {
            .current_term = state_.persistent.current_term,
            .voted_for = state_.persistent.voted_for},
        .votes_received = std::move(votes),
        .election_timer = election_timer_,
        .election_timeout = election_timeout_,
        .heartbeat_timer = heartbeat_timer_,
        .log = state_.persistent.log,
        .snapshot_metadata = state_.persistent.snapshot,
        .peer_progress = std::move(progress),
        .commit_index = state_.volatile_state.commit_index,
        .last_applied = state_.volatile_state.last_applied,
        .application_pending = pending_application_.has_value(),
        .application_blocked =
            pending_application_ && pending_application_->blocked,
        .pending_read_count = pending_reads_.size(),
        .completed_read_count = completed_read_count_,
        .rejected_read_count = rejected_read_count_,
        .waiting_for_persistence =
            state_.pending_hard_state.has_value() || pending_log_.has_value()
            || pending_snapshot_.has_value()
            || pending_snapshot_restore_.has_value()};
}

const figure2::State& Core::specification_state() const noexcept {
    return state_;
}

const PersistRaftLog* Core::pending_log_persistence() const noexcept {
    return pending_log_ ? &pending_log_->request : nullptr;
}

const ApplyLogEntry* Core::pending_application() const noexcept {
    return pending_application_ ? &pending_application_->request : nullptr;
}

const PersistRaftSnapshot*
Core::pending_snapshot_persistence() const noexcept {
    return pending_snapshot_ ? &pending_snapshot_->request : nullptr;
}

StepResult Core::translate(
    figure2::StepResult result,
    const RaftRole previous_role) {
    const bool resets_election = std::ranges::any_of(
        result.effects,
        [](const figure2::Effect& effect) {
            return std::holds_alternative<figure2::ResetElectionTimer>(
                effect);
        });
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
            translated.effects.emplace_back(next_heartbeat());
        } else if (
            previous_role == RaftRole::leader
            && state_.role != RaftRole::leader) {
            if (heartbeat_timer_) {
                translated.effects.emplace_back(
                    CancelHeartbeatDeadline{*heartbeat_timer_});
                heartbeat_timer_.reset();
            }
            if (!resets_election) {
                translated.effects.emplace_back(next_deadline());
            }
            reject_pending_reads(
                ReadIndexFailure::leadership_lost,
                translated);
            snapshot_transfers_.clear();
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
                        figure2::SendRequestVoteResponse>
                    || std::is_same_v<Typed, figure2::SendAppendEntries>
                    || std::is_same_v<
                        Typed,
                        figure2::SendAppendEntriesResponse>) {
                    translated.effects.emplace_back(
                        std::forward<decltype(typed)>(typed));
                } else if constexpr (
                    std::is_same_v<Typed, figure2::PersistState>) {
                    if (next_log_request_id_
                        == std::numeric_limits<std::uint64_t>::max()) {
                        throw std::overflow_error(
                            "Raft log persistence request ID exhausted");
                    }
                    translated.effects.emplace_back(PersistRaftLog{
                        .request_id = next_log_request_id_++,
                        .log = state_.persistent.log});
                } else if constexpr (
                    std::is_same_v<Typed, figure2::ResetElectionTimer>) {
                    translated.effects.emplace_back(next_deadline());
                }
            },
            std::move(effect));
    }
    gate_log_persistence(translated);
    replace_compacted_appends(translated);
    if (!state_.pending_hard_state && !pending_log_) {
        schedule_application(translated);
    }
    return translated;
}

StepResult Core::begin_read(const ReadIndexRequest& request) {
    auto reject = [this, &request](const ReadIndexFailure reason) {
        ++rejected_read_count_;
        return StepResult{
            .effects = {
                ReadIndexRejected{request.request_id, reason}}};
    };
    if (request.request_id == RequestId{}) {
        throw std::invalid_argument(
            "ReadIndex request ID must be nonzero");
    }
    if (state_.role != RaftRole::leader) {
        return reject(ReadIndexFailure::not_leader);
    }
    if (used_read_requests_.contains(request.request_id)) {
        return reject(ReadIndexFailure::duplicate_request);
    }
    const auto commit = state_.volatile_state.commit_index;
    const auto base = state_.persistent.snapshot
        ? state_.persistent.snapshot->last_included_index.value
        : 0;
    const auto commit_term = commit.value == base && state_.persistent.snapshot
        ? state_.persistent.snapshot->last_included_term
        : (commit.value > base
               ? state_.persistent.log.at(commit.value - base - 1).term
               : Term{});
    if (commit == LogIndex{}
        || commit_term != state_.persistent.current_term) {
        return reject(ReadIndexFailure::current_term_not_committed);
    }
    if (pending_reads_.size() >= max_pending_reads_
        || used_read_requests_.size() >= max_read_history_) {
        return reject(ReadIndexFailure::capacity_exceeded);
    }
    if (next_read_context_ == 0
        || next_read_context_
            == std::numeric_limits<std::uint64_t>::max()) {
        return reject(ReadIndexFailure::context_exhausted);
    }

    const ReadIndexContext context{next_read_context_++};
    used_read_requests_.insert(request.request_id);
    pending_read_requests_.emplace(request.request_id, context);
    pending_reads_.emplace(
        context,
        PendingRead{
            .request = request,
            .read_index = commit,
            .acknowledgements = {state_.node}});

    auto result = translate(
        figure2::step(state_, figure2::HeartbeatTimeout{}),
        state_.role);
    for (auto& effect : result.effects) {
        if (auto* send = std::get_if<figure2::SendAppendEntries>(&effect)) {
            send->request.read_context = context;
        } else if (auto* install_send =
                       std::get_if<SendInstallSnapshot>(&effect)) {
            install_send->request.read_context = context;
        }
    }
    return result;
}

StepResult Core::cancel_read(const CancelReadIndex& request) {
    const auto found = pending_read_requests_.find(request.request_id);
    if (found == pending_read_requests_.end()) {
        return {};
    }

    pending_reads_.erase(found->second);
    pending_read_requests_.erase(found);
    ++rejected_read_count_;
    return {
        .effects = {
            ReadIndexRejected{
                request.request_id,
                ReadIndexFailure::cancelled}}};
}

StepResult Core::timeout_read(const TimeoutReadIndex& request) {
    const auto found = pending_read_requests_.find(request.request_id);
    if (found == pending_read_requests_.end()) {
        return {};
    }
    pending_reads_.erase(found->second);
    pending_read_requests_.erase(found);
    ++rejected_read_count_;
    return {
        .effects = {
            ReadIndexRejected{
                request.request_id,
                ReadIndexFailure::timed_out}}};
}

void Core::acknowledge_read(
    const figure2::ReceiveAppendEntriesResponse& response,
    StepResult& result) {
    if (state_.role != RaftRole::leader
        || response.response.term != state_.persistent.current_term
        || !response.response.succeeded
        || !response.response.read_context) {
        return;
    }
    const auto found =
        pending_reads_.find(*response.response.read_context);
    if (found == pending_reads_.end()) {
        return;
    }
    found->second.acknowledgements.insert(response.from);
    const auto majority = (state_.peers.size() + 1) / 2 + 1;
    if (found->second.acknowledgements.size() >= majority) {
        found->second.quorum_confirmed = true;
    }
    complete_ready_reads(result);
}

void Core::complete_ready_reads(StepResult& result) {
    for (auto current = pending_reads_.begin();
         current != pending_reads_.end();) {
        auto& pending = current->second;
        if (!pending.quorum_confirmed
            || state_.volatile_state.last_applied
                < pending.read_index) {
            ++current;
            continue;
        }
        result.effects.emplace_back(ReadIndexResponse{
            .request_id = pending.request.request_id,
            .committed_index = pending.read_index});
        pending_read_requests_.erase(pending.request.request_id);
        ++completed_read_count_;
        current = pending_reads_.erase(current);
    }
}

void Core::reject_pending_reads(
    const ReadIndexFailure reason,
    StepResult& result) {
    for (const auto& [context, pending] : pending_reads_) {
        static_cast<void>(context);
        result.effects.emplace_back(ReadIndexRejected{
            pending.request.request_id,
            reason});
        ++rejected_read_count_;
    }
    pending_reads_.clear();
    pending_read_requests_.clear();
}

void Core::schedule_application(StepResult& result) {
    if (pending_application_
        || state_.volatile_state.last_applied
            >= state_.volatile_state.commit_index) {
        return;
    }
    if (next_application_request_id_ >= (1ULL << 63)) {
        throw std::overflow_error("Raft application request ID exhausted");
    }
    const LogIndex index{
        state_.volatile_state.last_applied.value + 1};
    const auto base = state_.persistent.snapshot
        ? state_.persistent.snapshot->last_included_index.value
        : 0;
    ApplyLogEntry request{
        .request_id = next_application_request_id_++,
        .entry = state_.persistent.log.at(index.value - base - 1)};
    pending_application_ = PendingApplication{.request = request};
    result.effects.emplace_back(std::move(request));
}

void Core::gate_log_persistence(StepResult& result) {
    const auto persistence = std::ranges::find_if(
        result.effects,
        [](const Effect& effect) {
            return std::holds_alternative<PersistRaftLog>(effect);
        });
    if (persistence == result.effects.end()) {
        return;
    }
    if (pending_log_) {
        throw std::logic_error("Raft log persistence is already pending");
    }
    const auto request = std::get<PersistRaftLog>(*persistence);
    auto after = persistence;
    ++after;
    std::vector<Effect> deferred(
        std::make_move_iterator(after),
        std::make_move_iterator(result.effects.end()));
    result.effects.erase(after, result.effects.end());
    pending_log_ = PendingLogPersistence{
        .request = request,
        .deferred_effects = std::move(deferred)};
}

void Core::replace_compacted_appends(StepResult& result) {
    if (!durable_snapshot_ || state_.role != RaftRole::leader
        || !state_.leader) {
        return;
    }
    for (auto& effect : result.effects) {
        auto* append = std::get_if<figure2::SendAppendEntries>(&effect);
        if (!append) {
            continue;
        }
        const auto progress = state_.leader->progress.find(append->to);
        if (progress == state_.leader->progress.end()
            || progress->second.next_index.value
                > durable_snapshot_->metadata.last_included_index.value) {
            continue;
        }
        if (next_snapshot_transfer_id_ == 0
            || next_snapshot_transfer_id_
                == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error(
                "Raft snapshot transfer ID exhausted");
        }
        const auto context = append->request.read_context;
        const auto transfer = next_snapshot_transfer_id_++;
        snapshot_transfers_[append->to] = {
            .transfer_id = transfer,
            .term = state_.persistent.current_term,
            .index = durable_snapshot_->metadata.last_included_index};
        effect = SendInstallSnapshot{
            .to = append->to,
            .request = {
                .term = state_.persistent.current_term,
                .leader = state_.node,
                .transfer_id = transfer,
                .metadata = durable_snapshot_->metadata,
                .offset = 0,
                .total_size = durable_snapshot_->state.size(),
                .chunk = durable_snapshot_->state,
                .state_checksum = crc32c(durable_snapshot_->state),
                .done = true,
                .read_context = context}};
    }
}

StepResult Core::begin_snapshot(const CreateRaftSnapshot& request) {
    auto reject = [&request](const SnapshotRejection reason) {
        static_cast<void>(request);
        return StepResult{
            .effects = {RaftSnapshotRejected{reason}}};
    };
    if (pending_snapshot_) {
        return reject(SnapshotRejection::persistence_busy);
    }
    if (request.applied_index == LogIndex{}
        || request.applied_index != state_.volatile_state.last_applied) {
        return reject(SnapshotRejection::no_applied_entry);
    }
    if (durable_snapshot_
        && request.applied_index
            <= durable_snapshot_->metadata.last_included_index) {
        return reject(SnapshotRejection::stale);
    }
    if (request.store_revision.value < 0
        || request.compaction_revision.value < 0
        || request.compaction_revision > request.store_revision) {
        return reject(SnapshotRejection::invalid_revision);
    }

    auto membership = request.membership;
    auto by_id = [](const NodeId left, const NodeId right) {
        return left.value < right.value;
    };
    std::ranges::sort(membership.voters, by_id);
    std::ranges::sort(membership.learners, by_id);
    const auto duplicate = [](const std::vector<NodeId>& members) {
        return std::adjacent_find(members.begin(), members.end())
            != members.end();
    };
    std::vector<NodeId> expected = state_.peers;
    expected.push_back(state_.node);
    std::ranges::sort(expected, by_id);
    if (membership.voters != expected
        || duplicate(membership.voters)
        || duplicate(membership.learners)
        || std::ranges::find(membership.learners, NodeId{})
            != membership.learners.end()
        || std::ranges::any_of(
            membership.learners,
            [&membership](const NodeId node) {
                return std::ranges::find(membership.voters, node)
                    != membership.voters.end();
            })
        || membership.voters.size() + membership.learners.size()
            > max_snapshot_members_) {
        return reject(SnapshotRejection::invalid_membership);
    }
    const auto overhead = 68ULL
        + 8ULL
            * (membership.voters.size() + membership.learners.size());
    if (overhead > max_snapshot_bytes_
        || request.state.size() > max_snapshot_bytes_ - overhead) {
        return reject(SnapshotRejection::too_large);
    }

    const auto base = state_.persistent.snapshot
        ? state_.persistent.snapshot->last_included_index.value
        : 0;
    Term term;
    if (request.applied_index.value == base && state_.persistent.snapshot) {
        term = state_.persistent.snapshot->last_included_term;
    } else {
        term = state_.persistent.log.at(
            request.applied_index.value - base - 1).term;
    }
    RaftSnapshot snapshot{
        .metadata = {
            .last_included_index = request.applied_index,
            .last_included_term = term,
            .store_revision = request.store_revision,
            .compaction_revision = request.compaction_revision,
            .membership = std::move(membership),
            .format_version = 1},
        .state = request.state,
        .checksum = crc32c(request.state)};
    if (next_snapshot_request_id_ == 0
        || next_snapshot_request_id_ >= (1ULL << 62)) {
        throw std::overflow_error(
            "Raft snapshot persistence request ID exhausted");
    }
    PersistRaftSnapshot persistence{
        .request_id = next_snapshot_request_id_++,
        .snapshot = std::move(snapshot)};
    pending_snapshot_ = PendingSnapshotPersistence{
        .request = persistence,
        .local_creation = true};
    return {.effects = {std::move(persistence)}};
}

StepResult Core::receive_snapshot(
    const ReceiveInstallSnapshot& event) {
    if (std::ranges::find(state_.peers, event.from)
            == state_.peers.end()
        || event.request.leader != event.from) {
        throw std::invalid_argument(
            "InstallSnapshot sender is not its voting leader");
    }
    auto rejection = [this, &event]() {
        return StepResult{
            .effects = {
                SendInstallSnapshotResponse{
                    event.from,
                    {
                        .term = state_.persistent.current_term,
                        .transfer_id = event.request.transfer_id,
                        .succeeded = false,
                        .read_context = event.request.read_context}}}};
    };
    if (event.request.term < state_.persistent.current_term) {
        return rejection();
    }
    if (event.request.term > state_.persistent.current_term) {
        install_after_hard_state_ = event;
        return translate(
            figure2::step(
                state_, figure2::ObserveTerm{event.request.term}),
            state_.role);
    }
    if (pending_snapshot_) {
        return rejection();
    }

    figure2::StepResult accepted{.state = state_};
    accepted.state.role = RaftRole::follower;
    accepted.state.leader.reset();
    accepted.state.votes_received.clear();
    accepted.state.known_leader = event.from;
    accepted.effects.emplace_back(figure2::ResetElectionTimer{});
    auto result = translate(std::move(accepted), state_.role);

    const auto& request = event.request;
    const auto& metadata = request.metadata;
    const auto voter_count = metadata.membership.voters.size();
    const auto learner_count = metadata.membership.learners.size();
    const bool member_size_valid =
        voter_count <= max_snapshot_members_
        && learner_count <= max_snapshot_members_
        && voter_count <= max_snapshot_members_ - learner_count
        && voter_count <= (max_snapshot_bytes_ - 68) / 8
        && learner_count <= (max_snapshot_bytes_ - 68) / 8
        && voter_count
            <= (max_snapshot_bytes_ - 68) / 8 - learner_count;
    const auto member_count =
        member_size_valid ? voter_count + learner_count : 0;
    const bool state_size_valid =
        request.total_size <= max_snapshot_bytes_;
    const auto encoded_size = member_size_valid && state_size_valid
        ? 68ULL + 8ULL * member_count + request.total_size
        : std::numeric_limits<std::uint64_t>::max();
    const bool valid_shape = member_size_valid && state_size_valid
        && request.transfer_id != 0
        && request.offset == 0 && request.done
        && request.total_size == request.chunk.size()
        && encoded_size >= request.total_size
        && encoded_size <= max_snapshot_bytes_
        && metadata.last_included_index.value != 0
        && metadata.last_included_term != Term{}
        && metadata.last_included_term <= request.term
        && metadata.format_version == 1
        && metadata.store_revision.value >= 0
        && metadata.compaction_revision.value >= 0
        && metadata.compaction_revision <= metadata.store_revision;
    if (!valid_shape
        || crc32c(request.chunk) != request.state_checksum) {
        auto failed = rejection();
        result.effects.insert(
            result.effects.end(),
            std::make_move_iterator(failed.effects.begin()),
            std::make_move_iterator(failed.effects.end()));
        return result;
    }
    auto voters = metadata.membership.voters;
    auto expected = state_.peers;
    expected.push_back(state_.node);
    auto by_id = [](const NodeId left, const NodeId right) {
        return left.value < right.value;
    };
    std::ranges::sort(voters, by_id);
    std::ranges::sort(expected, by_id);
    std::set<NodeId> unique;
    bool contains_zero = false;
    for (const auto voter : metadata.membership.voters) {
        contains_zero = contains_zero || voter == NodeId{};
        unique.insert(voter);
    }
    for (const auto learner : metadata.membership.learners) {
        contains_zero = contains_zero || learner == NodeId{};
        unique.insert(learner);
    }
    if (voters != expected
        || contains_zero
        || !std::ranges::is_sorted(
            metadata.membership.voters, by_id)
        || !std::ranges::is_sorted(
            metadata.membership.learners, by_id)
        || unique.size()
            != metadata.membership.voters.size()
                + metadata.membership.learners.size()) {
        auto failed = rejection();
        result.effects.insert(
            result.effects.end(),
            std::make_move_iterator(failed.effects.begin()),
            std::make_move_iterator(failed.effects.end()));
        return result;
    }

    if (durable_snapshot_
        && metadata.last_included_index
            <= durable_snapshot_->metadata.last_included_index) {
        InstallSnapshotResponse response{
            .term = state_.persistent.current_term,
            .transfer_id = request.transfer_id,
            .succeeded =
                metadata == durable_snapshot_->metadata
                && request.chunk == durable_snapshot_->state,
            .last_included_index =
                durable_snapshot_->metadata.last_included_index,
            .read_context = request.read_context};
        result.effects.emplace_back(
            SendInstallSnapshotResponse{event.from, response});
        return result;
    }
    if (metadata.last_included_index
        <= state_.volatile_state.last_applied) {
        auto failed = rejection();
        result.effects.insert(
            result.effects.end(),
            std::make_move_iterator(failed.effects.begin()),
            std::make_move_iterator(failed.effects.end()));
        return result;
    }

    RaftSnapshot snapshot{
        .metadata = metadata,
        .state = request.chunk,
        .checksum = request.state_checksum};
    if (next_snapshot_request_id_ == 0
        || next_snapshot_request_id_ >= (1ULL << 62)) {
        throw std::overflow_error(
            "Raft snapshot persistence request ID exhausted");
    }
    PersistRaftSnapshot persistence{
        .request_id = next_snapshot_request_id_++,
        .snapshot = snapshot};
    pending_snapshot_ = PendingSnapshotPersistence{
        .request = persistence,
        .reply_to = event.from,
        .response = InstallSnapshotResponse{
            .term = state_.persistent.current_term,
            .transfer_id = request.transfer_id,
            .succeeded = true,
            .last_included_index = metadata.last_included_index,
            .read_context = request.read_context}};
    result.effects.emplace_back(std::move(persistence));
    return result;
}

void Core::compact_to(const RaftSnapshot& snapshot) {
    const auto index = snapshot.metadata.last_included_index.value;
    const auto base = state_.persistent.snapshot
        ? state_.persistent.snapshot->last_included_index.value
        : 0;
    bool preserve_suffix = false;
    if (index == base && state_.persistent.snapshot) {
        preserve_suffix =
            state_.persistent.snapshot->last_included_term
            == snapshot.metadata.last_included_term;
    } else if (index > base) {
        const auto offset = index - base - 1;
        preserve_suffix = offset < state_.persistent.log.size()
            && state_.persistent.log[offset].term
                == snapshot.metadata.last_included_term;
        if (preserve_suffix) {
            state_.persistent.log.erase(
                state_.persistent.log.begin(),
                state_.persistent.log.begin()
                    + static_cast<std::ptrdiff_t>(offset + 1));
        }
    }
    if (!preserve_suffix) {
        state_.persistent.log.clear();
    }
    state_.persistent.snapshot = snapshot.metadata;
    state_.volatile_state.commit_index.value =
        std::max(state_.volatile_state.commit_index.value, index);
    state_.volatile_state.last_applied.value =
        std::max(state_.volatile_state.last_applied.value, index);
    if (pending_application_
        && pending_application_->request.entry.index.value <= index) {
        pending_application_.reset();
    }
    durable_snapshot_ = snapshot;
}

void Core::install_persisted_snapshot(
    const RaftSnapshotPersisted& completion,
    StepResult& result) {
    if (!pending_snapshot_
        || completion.request_id
            != pending_snapshot_->request.request_id
        || completion.snapshot.metadata
            != pending_snapshot_->request.snapshot.metadata
        || completion.snapshot.state
            != pending_snapshot_->request.snapshot.state) {
        throw std::invalid_argument(
            "Raft snapshot completion does not match pending request");
    }
    auto pending = std::move(*pending_snapshot_);
    pending_snapshot_.reset();
    auto snapshot = pending.request.snapshot;
    snapshot.checksum = crc32c(snapshot.state);
    if (pending.local_creation) {
        compact_to(snapshot);
        result.effects.emplace_back(TruncateRaftLogPrefix{
            snapshot.metadata.last_included_index});
        result.effects.emplace_back(RaftSnapshotCreated{
            snapshot.metadata.last_included_index});
    } else {
        pending_snapshot_restore_ = PendingSnapshotRestore{
            .request_id = completion.request_id,
            .snapshot = snapshot,
            .reply_to = *pending.reply_to,
            .response = *pending.response};
        result.effects.emplace_back(RestoreStateMachineSnapshot{
            .request_id = completion.request_id,
            .snapshot = std::move(snapshot)});
    }
}

void Core::complete_snapshot_restore(
    const RaftSnapshotRestored& completion,
    StepResult& result) {
    if (!pending_snapshot_restore_
        || completion.request_id
            != pending_snapshot_restore_->request_id
        || completion.index
            != pending_snapshot_restore_->snapshot.metadata
                   .last_included_index) {
        throw std::invalid_argument(
            "Raft snapshot restore completion does not match pending restore");
    }
    auto pending = std::move(*pending_snapshot_restore_);
    pending_snapshot_restore_.reset();
    compact_to(pending.snapshot);
    result.effects.emplace_back(TruncateRaftLogPrefix{
        pending.snapshot.metadata.last_included_index});
    result.effects.emplace_back(SendInstallSnapshotResponse{
        pending.reply_to, pending.response});
}

StepResult Core::receive_snapshot_response(
    const ReceiveInstallSnapshotResponse& event) {
    if (std::ranges::find(state_.peers, event.from)
        == state_.peers.end()) {
        throw std::invalid_argument(
            "InstallSnapshot response is from a non-peer");
    }
    if (event.response.term > state_.persistent.current_term) {
        return translate(
            figure2::step(
                state_, figure2::ObserveTerm{event.response.term}),
            state_.role);
    }
    if (state_.role != RaftRole::leader
        || event.response.term != state_.persistent.current_term
        || !event.response.succeeded || !durable_snapshot_
        || event.response.last_included_index
            != durable_snapshot_->metadata.last_included_index) {
        return {};
    }
    const auto transfer = snapshot_transfers_.find(event.from);
    if (transfer == snapshot_transfers_.end()
        || transfer->second.transfer_id != event.response.transfer_id
        || transfer->second.term != event.response.term
        || transfer->second.index
            != event.response.last_included_index) {
        return {};
    }
    snapshot_transfers_.erase(transfer);
    figure2::ReceiveAppendEntriesResponse synthetic{
        .from = event.from,
        .response = {
            .term = event.response.term,
            .succeeded = true,
            .matched_index = event.response.last_included_index,
            .read_context = event.response.read_context}};
    auto result = translate(
        figure2::step(state_, synthetic), state_.role);
    acknowledge_read(synthetic, result);
    auto resume = translate(
        figure2::step(state_, figure2::HeartbeatTimeout{}),
        state_.role);
    result.rules.insert(
        result.rules.end(), resume.rules.begin(), resume.rules.end());
    result.effects.insert(
        result.effects.end(),
        std::make_move_iterator(resume.effects.begin()),
        std::make_move_iterator(resume.effects.end()));
    return result;
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

ResetHeartbeatDeadline Core::next_heartbeat() {
    if (next_timer_id_ == 0
        || next_timer_id_ == std::numeric_limits<simulation::TimerId>::max()) {
        throw std::overflow_error("Raft timer ID exhausted");
    }
    const auto timer = next_timer_id_++;
    heartbeat_timer_ = timer;
    return {.timer_id = timer, .delay = heartbeat_interval_};
}

simulation::NodeFactory make_simulation_factory(SimulationConfig config) {
    return [config = std::move(config)](const NodeId node) {
        return std::make_unique<SimulationNode>(node, config);
    };
}

}  // namespace kura::metadata::raft::election
