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
        .request_id = {.client = 14, .sequence = sequence},
        .type_tag = 29,
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
    const std::size_t cluster_size = 3,
    const std::size_t max_pending = 128,
    const std::size_t max_history = 4'096) {
    std::vector<NodeId> peers;
    for (std::uint64_t id = 1; id <= cluster_size; ++id) {
        if (NodeId{id} != self) {
            peers.push_back(NodeId{id});
        }
    }
    return Core(
        self,
        std::move(peers),
        {},
        91,
        {10, 20},
        {},
        3,
        {},
        max_pending,
        max_history);
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

void elect(Core& node, const std::size_t cluster_size = 3) {
    const auto started = node.start();
    const auto timer = find_effect<ResetElectionDeadline>(started)->timer_id;
    static_cast<void>(node.step(ElectionDeadline{timer}));
    complete_all_storage(node);
    const auto term = node.snapshot().hard_state.current_term;
    for (std::uint64_t voter = 2;
         voter <= cluster_size / 2 + 1;
         ++voter) {
        static_cast<void>(
            node.step(figure2::ReceiveRequestVoteResponse{
                .from = {voter}, .response = {term, true}}));
    }
    expect(node.snapshot().role == RaftRole::leader, "election failed");
}

void propose_and_commit(
    Core& leader,
    const std::size_t cluster_size = 3,
    const bool apply_locally = true) {
    static_cast<void>(leader.step(
        figure2::ReceiveClientCommand{command(1)}));
    static_cast<void>(complete_storage(leader));
    const auto term = leader.snapshot().hard_state.current_term;
    for (std::uint64_t peer = 2;
         peer <= cluster_size / 2 + 1;
         ++peer) {
        static_cast<void>(
            leader.step(figure2::ReceiveAppendEntriesResponse{
                .from = {peer},
                .response = {term, true, {1}}}));
    }
    expect(
        leader.snapshot().commit_index == LogIndex{1},
        "current-term entry did not commit");
    if (apply_locally) {
        const auto pending = *leader.pending_application();
        static_cast<void>(leader.step(LogEntryApplied{
            .request_id = pending.request_id,
            .index = pending.entry.index}));
    }
}

ReadIndexContext begin_read(
    Core& leader,
    const RequestId request_id) {
    const auto result = leader.step(ReadIndexRequest{request_id});
    const auto sends = effects<figure2::SendAppendEntries>(result);
    expect(!sends.empty(), "ReadIndex did not send quorum probes");
    const auto context = sends.front().request.read_context;
    expect(context.has_value(), "ReadIndex probe has no context");
    expect(
        std::ranges::all_of(
            sends,
            [context](const figure2::SendAppendEntries& send) {
                return send.request.read_context == context;
            }),
        "ReadIndex probes did not share one context");
    return *context;
}

StepResult acknowledge(
    Core& leader,
    const NodeId peer,
    const ReadIndexContext context,
    const bool succeeded = true,
    const std::optional<Term> term = std::nullopt) {
    return leader.step(figure2::ReceiveAppendEntriesResponse{
        .from = peer,
        .response = {
            term.value_or(leader.snapshot().hard_state.current_term),
            succeeded,
            {1},
            context}});
}

void healthy_three_and_five_node_reads() {
    auto three = core({1});
    elect(three);
    propose_and_commit(three);
    const RequestId first{1, 1};
    const auto context = begin_read(three, first);
    const auto ready = acknowledge(three, {2}, context);
    const auto response = find_effect<ReadIndexResponse>(ready);
    expect(
        response != nullptr && response->request_id == first
            && response->committed_index == LogIndex{1},
        "two of three did not complete ReadIndex");

    auto five = core({1}, 5);
    elect(five, 5);
    propose_and_commit(five, 5);
    const RequestId second{1, 2};
    const auto five_context = begin_read(five, second);
    auto minority = acknowledge(five, {2}, five_context);
    expect(
        find_effect<ReadIndexResponse>(minority) == nullptr,
        "self plus one of five completed ReadIndex");
    const auto majority = acknowledge(five, {3}, five_context);
    expect(
        find_effect<ReadIndexResponse>(majority)->request_id == second,
        "self plus two of five did not complete ReadIndex");
}

void current_term_commit_is_required() {
    auto leader = core({1});
    elect(leader);
    const RequestId request{2, 1};
    const auto rejected = leader.step(ReadIndexRequest{request});
    expect(
        find_effect<ReadIndexRejected>(rejected)->reason
            == ReadIndexFailure::current_term_not_committed,
        "new leader served a read before a current-term commit");

    Core inherited(
        {1},
        {{2}, {3}},
        {.current_term = {2}},
        92,
        {10, 20},
        {entry(1, 2)},
        3,
        {1});
    elect(inherited);
    const auto old_term =
        inherited.step(ReadIndexRequest{{2, 3}});
    expect(
        find_effect<ReadIndexRejected>(old_term)->reason
            == ReadIndexFailure::current_term_not_committed,
        "leader served ReadIndex from only an inherited-term commit");

    auto follower = core({2});
    static_cast<void>(follower.start());
    const auto not_leader = follower.step(ReadIndexRequest{{2, 2}});
    expect(
        find_effect<ReadIndexRejected>(not_leader)->reason
            == ReadIndexFailure::not_leader,
        "follower accepted ReadIndex");
}

void quorum_waits_for_local_apply() {
    auto leader = core({1});
    elect(leader);
    propose_and_commit(leader, 3, false);
    const RequestId request{3, 1};
    const auto context = begin_read(leader, request);
    const auto confirmed = acknowledge(leader, {2}, context);
    expect(
        find_effect<ReadIndexResponse>(confirmed) == nullptr
            && leader.snapshot().pending_read_count == 1,
        "ReadIndex completed before local apply reached its index");

    const auto application = *leader.pending_application();
    const auto applied = leader.step(LogEntryApplied{
        .request_id = application.request_id,
        .index = application.entry.index});
    expect(
        find_effect<ReadIndexResponse>(applied)->request_id == request,
        "local apply completion did not release confirmed read");
}

void duplicate_stale_and_replayed_acknowledgements() {
    auto leader = core({1}, 5);
    elect(leader, 5);
    propose_and_commit(leader, 5);
    const RequestId request{4, 1};
    const auto context = begin_read(leader, request);

    auto failed = acknowledge(leader, {2}, context, false);
    const auto retry = find_effect<figure2::SendAppendEntries>(failed);
    expect(
        retry != nullptr && retry->request.read_context == context
            && leader.snapshot().pending_read_count == 1,
        "failed probe did not retain context on retry");
    static_cast<void>(acknowledge(leader, {2}, context));
    const auto duplicate = acknowledge(leader, {2}, context);
    expect(
        find_effect<ReadIndexResponse>(duplicate) == nullptr,
        "duplicate acknowledgement counted twice");
    const auto stale = acknowledge(
        leader, {3}, context, true, Term{});
    expect(
        find_effect<ReadIndexResponse>(stale) == nullptr,
        "stale-term acknowledgement completed a read");
    const auto wrong = acknowledge(
        leader, {3}, ReadIndexContext{context.value + 100});
    expect(
        find_effect<ReadIndexResponse>(wrong) == nullptr,
        "unknown context completed a read");

    const auto completed = acknowledge(leader, {3}, context);
    expect(
        find_effect<ReadIndexResponse>(completed)->request_id == request,
        "unique current-term quorum did not complete");
    const auto replay = acknowledge(leader, {4}, context);
    expect(
        find_effect<ReadIndexResponse>(replay) == nullptr
            && leader.snapshot().completed_read_count == 1,
        "replayed context completed twice");
    const auto reused = leader.step(ReadIndexRequest{request});
    expect(
        find_effect<ReadIndexRejected>(reused)->reason
            == ReadIndexFailure::duplicate_request,
        "completed request ID was reused");
}

void concurrent_reads_cancellation_and_bounds() {
    auto leader = core({1}, 3, 2, 2);
    elect(leader);
    propose_and_commit(leader);
    const RequestId first{5, 1};
    const RequestId second{5, 2};
    const auto first_context = begin_read(leader, first);
    const auto second_context = begin_read(leader, second);
    expect(
        first_context != second_context
            && leader.snapshot().pending_read_count == 2,
        "concurrent reads did not receive unique bounded contexts");
    const auto overflow = leader.step(ReadIndexRequest{{5, 3}});
    expect(
        find_effect<ReadIndexRejected>(overflow)->reason
            == ReadIndexFailure::capacity_exceeded,
        "pending ReadIndex bound was not enforced");

    const auto cancelled = leader.step(CancelReadIndex{first});
    expect(
        find_effect<ReadIndexRejected>(cancelled)->reason
                == ReadIndexFailure::cancelled
            && leader.snapshot().pending_read_count == 1,
        "ReadIndex cancellation did not remove pending context");
    const auto cancelled_ack = acknowledge(leader, {2}, first_context);
    expect(
        find_effect<ReadIndexResponse>(cancelled_ack) == nullptr,
        "cancelled context was later completed");
    const auto timed_out = leader.step(TimeoutReadIndex{second});
    expect(
        find_effect<ReadIndexRejected>(timed_out)->reason
            == ReadIndexFailure::timed_out,
        "read timeout did not return explicit uncertainty");
    const auto timed_out_ack = acknowledge(leader, {2}, second_context);
    expect(
        find_effect<ReadIndexResponse>(timed_out_ack) == nullptr,
        "timed-out context later returned stale success");
}

void higher_term_fails_pending_read_immediately() {
    auto leader = core({1});
    elect(leader);
    propose_and_commit(leader);
    const RequestId request{6, 1};
    const auto context = begin_read(leader, request);
    const auto stepped_down =
        leader.step(figure2::ReceiveAppendEntriesResponse{
            .from = {2},
            .response = {{9}, false, {}, context}});
    expect(
        leader.snapshot().role == RaftRole::follower
            && leader.snapshot().pending_read_count == 0
            && find_effect<ReadIndexRejected>(stepped_down)->reason
                == ReadIndexFailure::leadership_lost,
        "higher term did not immediately fail pending ReadIndex");
}

SimulationConfig simulation_config(
    const std::shared_ptr<std::map<std::uint64_t, Snapshot>>& observations) {
    return {
        .timeouts = {50, 80},
        .seed = 0x1470,
        .heartbeat_interval = 3,
        .commands_on_leadership = {command(1)},
        .reads_on_leadership = {{{70, 1}}},
        .observer =
            [observations](const NodeId node, const Snapshot& snapshot) {
                (*observations)[node.value] = snapshot;
            }};
}

void healthy_simulator_schedules() {
    for (const auto& topology :
         std::vector<std::vector<NodeId>>{
             simulation::Simulator::three_node_topology(),
             simulation::Simulator::five_node_topology()}) {
        auto observations =
            std::make_shared<std::map<std::uint64_t, Snapshot>>();
        simulation::Simulator simulator(
            topology,
            0x2580,
            make_simulation_factory(simulation_config(observations)));
        bool complete = false;
        for (std::size_t steps = 0; steps < 8'000 && !complete; ++steps) {
            expect(simulator.step(), "healthy ReadIndex schedule stopped");
            complete = std::ranges::any_of(
                *observations,
                [](const auto& observed) {
                    return observed.second.completed_read_count == 1;
                });
        }
        expect(complete, "healthy odd-node ReadIndex did not complete");
    }
}

void partitioned_former_leader_never_completes() {
    auto observations =
        std::make_shared<std::map<std::uint64_t, Snapshot>>();
    const auto topology = simulation::Simulator::three_node_topology();
    simulation::Simulator simulator(
        topology,
        0x3690,
        make_simulation_factory(simulation_config(observations)));
    NodeId former_leader;
    for (std::size_t steps = 0;
         steps < 5'000 && former_leader == NodeId{};
         ++steps) {
        expect(simulator.step(), "partition setup schedule stopped");
        for (const auto& [id, snapshot] : *observations) {
            if (snapshot.role == RaftRole::leader
                && snapshot.pending_read_count == 1) {
                former_leader = {id};
            }
        }
    }
    expect(former_leader != NodeId{}, "leader did not start ReadIndex");
    for (const auto node : topology) {
        if (node != former_leader) {
            simulator.partition(former_leader, node);
        }
    }
    for (std::size_t steps = 0; steps < 150; ++steps) {
        expect(simulator.step(), "partitioned ReadIndex schedule stopped");
    }
    expect(
        observations->at(former_leader.value).completed_read_count == 0
            && observations->at(former_leader.value).pending_read_count == 1,
        "partitioned former leader completed a linearizable read");
}

}  // namespace

int main() {
    try {
        healthy_three_and_five_node_reads();
        current_term_commit_is_required();
        quorum_waits_for_local_apply();
        duplicate_stale_and_replayed_acknowledgements();
        concurrent_reads_cancellation_and_bounds();
        higher_term_fails_pending_read_immediately();
        healthy_simulator_schedules();
        partitioned_former_leader_never_completes();
    } catch (const std::exception& error) {
        std::cerr << "raft ReadIndex test failure: "
                  << error.what() << '\n';
        return 1;
    }
    std::cout << "raft ReadIndex tests passed\n";
    return 0;
}
