#include "kura/metadata/raft/simulation.hpp"

#include <algorithm>
#include <charconv>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <utility>

namespace kura::metadata::raft::simulation {
namespace {

struct Link {
    NodeId first;
    NodeId second;

    bool operator<(const Link& other) const {
        if (first != other.first) {
            return first < other.first;
        }
        return second < other.second;
    }
};

Link directed(const NodeId from, const NodeId to) {
    return {.first = from, .second = to};
}

Link undirected(NodeId first, NodeId second) {
    if (second < first) {
        std::swap(first, second);
    }
    return {.first = first, .second = second};
}

std::uint64_t parse_number(const std::string_view text) {
    std::uint64_t value{};
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw SimulationFailure("invalid replay trace number");
    }
    return value;
}

std::string kind_name(const PendingEventKind kind) {
    switch (kind) {
    case PendingEventKind::start:
        return "start";
    case PendingEventKind::message:
        return "message";
    case PendingEventKind::timer:
        return "timer";
    case PendingEventKind::disk_completion:
        return "disk";
    }
    return "unknown";
}

}  // namespace

class Simulator::Impl {
public:
    struct NodeState {
        std::unique_ptr<NodeAdapter> adapter;
        std::vector<DurableRecord> durable;
        std::uint64_t generation{};
        bool running{true};
    };

    struct QueuedEvent {
        PendingEvent summary;
        std::uint64_t generation{};
        NodeEvent event;
        std::optional<DurableRecord> pending_record;
    };

    Impl(
        std::vector<NodeId> topology,
        const std::uint64_t seed,
        NodeFactory factory)
        : topology_(std::move(topology)),
          seed_(seed),
          random_state_(seed),
          factory_(std::move(factory)) {
        if (!factory_) {
            throw std::invalid_argument("simulation node factory is empty");
        }
        if (topology_.size() < 1) {
            throw std::invalid_argument("simulation topology is empty");
        }
        std::sort(topology_.begin(), topology_.end());
        if (std::adjacent_find(topology_.begin(), topology_.end()) !=
            topology_.end()) {
            throw std::invalid_argument("simulation topology has duplicate nodes");
        }

        std::ostringstream header;
        header << "sim-v1 seed " << seed_ << " nodes";
        for (const auto node : topology_) {
            header << ' ' << node.value;
            NodeState state;
            state.adapter = make_node(node);
            nodes_.emplace(node.value, std::move(state));
        }
        history_.push_back(header.str());
        for (const auto node : topology_) {
            enqueue_start(node);
        }
    }

    std::unique_ptr<NodeAdapter> make_node(const NodeId node) {
        auto adapter = factory_(node);
        if (!adapter) {
            throw std::invalid_argument("simulation node factory returned null");
        }
        return adapter;
    }

    NodeState& node(const NodeId id) {
        const auto found = nodes_.find(id.value);
        if (found == nodes_.end()) {
            throw std::invalid_argument("node is not in the topology");
        }
        return found->second;
    }

    const NodeState& node(const NodeId id) const {
        const auto found = nodes_.find(id.value);
        if (found == nodes_.end()) {
            throw std::invalid_argument("node is not in the topology");
        }
        return found->second;
    }

    EventId enqueue(
        const LogicalTime ready_at,
        const PendingEventKind kind,
        const std::optional<NodeId> from,
        const NodeId to,
        NodeEvent event,
        const std::uint64_t generation,
        std::optional<DurableRecord> pending_record = std::nullopt) {
        if (ready_at < now_) {
            throw SimulationFailure("attempted to schedule an event in the past");
        }
        const EventId id = next_event_id_++;
        events_.push_back(
            {.summary = {
                 .id = id,
                 .ready_at = ready_at,
                 .kind = kind,
                 .from = from,
                 .to = to},
             .generation = generation,
             .event = std::move(event),
             .pending_record = std::move(pending_record)});
        return id;
    }

    void enqueue_start(const NodeId id) {
        const auto& state = node(id);
        std::vector<NodeId> peers;
        for (const auto candidate : topology_) {
            if (candidate != id) {
                peers.push_back(candidate);
            }
        }
        enqueue(
            now_,
            PendingEventKind::start,
            std::nullopt,
            id,
            StartEvent{
                .self = id,
                .peers = std::move(peers),
                .durable_records = state.durable},
            state.generation);
    }

    std::uint64_t random() {
        random_state_ += 0x9e3779b97f4a7c15ULL;
        auto value = random_state_;
        value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31U);
    }

    std::vector<std::size_t> earliest_indices() const {
        if (events_.empty()) {
            return {};
        }
        const auto earliest = std::min_element(
            events_.begin(),
            events_.end(),
            [](const QueuedEvent& first, const QueuedEvent& second) {
                return first.summary.ready_at < second.summary.ready_at;
            })->summary.ready_at;
        std::vector<std::size_t> result;
        for (std::size_t index = 0; index < events_.size(); ++index) {
            if (events_[index].summary.ready_at == earliest) {
                result.push_back(index);
            }
        }
        return result;
    }

    void record_control(const std::string& control) {
        history_.push_back("control " + control);
    }

    [[noreturn]] void fail(const std::string_view message) const {
        throw SimulationFailure(std::string(message) + "\n" + trace());
    }

    std::string trace() const {
        std::ostringstream output;
        for (const auto& line : history_) {
            output << line << '\n';
        }
        return output.str();
    }

    void process_actions(
        const NodeId origin,
        const std::uint64_t generation,
        std::vector<NodeAction> actions) {
        auto& origin_state = node(origin);
        if (!origin_state.running || origin_state.generation != generation) {
            return;
        }
        for (auto& action : actions) {
            std::visit(
                [this, origin, generation](auto&& typed) {
                    using Action = std::decay_t<decltype(typed)>;
                    if constexpr (std::is_same_v<Action, SendAction>) {
                        node(typed.to);
                        const auto link = directed(origin, typed.to);
                        auto drop = drops_.find(link);
                        if (drop != drops_.end() && drop->second > 0) {
                            --drop->second;
                            return;
                        }
                        std::size_t copies = 1;
                        auto duplicate = duplicates_.find(link);
                        if (duplicate != duplicates_.end() &&
                            duplicate->second > 0) {
                            copies += duplicate->second;
                            duplicate->second = 0;
                        }
                        const auto delay = network_delays_[link];
                        if (delay >
                            std::numeric_limits<LogicalTime>::max() - now_) {
                            fail("network delay overflows logical time");
                        }
                        for (std::size_t copy = 0; copy < copies; ++copy) {
                            enqueue(
                                now_ + delay,
                                PendingEventKind::message,
                                origin,
                                typed.to,
                                MessageEvent{
                                    .from = origin,
                                    .bytes = typed.bytes},
                                node(typed.to).generation);
                        }
                    } else if constexpr (std::is_same_v<Action, PersistAction>) {
                        const auto duplicate = std::find_if(
                            events_.begin(),
                            events_.end(),
                            [origin, generation, &typed](
                                const QueuedEvent& queued) {
                                return queued.summary.kind ==
                                           PendingEventKind::disk_completion &&
                                    queued.summary.to == origin &&
                                    queued.generation == generation &&
                                    std::get<PersistedEvent>(queued.event)
                                            .request_id == typed.request_id;
                            });
                        if (duplicate != events_.end()) {
                            fail("duplicate outstanding persistence request id");
                        }
                        const auto delay = persistence_delays_[origin.value];
                        if (delay >
                            std::numeric_limits<LogicalTime>::max() - now_) {
                            fail("persistence delay overflows logical time");
                        }
                        DurableRecord record{
                            .request_id = typed.request_id,
                            .bytes = std::move(typed.bytes)};
                        enqueue(
                            now_ + delay,
                            PendingEventKind::disk_completion,
                            std::nullopt,
                            origin,
                            PersistedEvent{.request_id = typed.request_id},
                            generation,
                            std::move(record));
                    } else if constexpr (std::is_same_v<Action, SetTimerAction>) {
                        events_.erase(
                            std::remove_if(
                                events_.begin(),
                                events_.end(),
                                [origin, generation, &typed](
                                    const QueuedEvent& queued) {
                                    return queued.summary.kind ==
                                               PendingEventKind::timer &&
                                        queued.summary.to == origin &&
                                        queued.generation == generation &&
                                        std::get<TimerEvent>(queued.event)
                                                .timer_id == typed.timer_id;
                                }),
                            events_.end());
                        if (typed.delay >
                            std::numeric_limits<LogicalTime>::max() - now_) {
                            fail("timer delay overflows logical time");
                        }
                        enqueue(
                            now_ + typed.delay,
                            PendingEventKind::timer,
                            std::nullopt,
                            origin,
                            TimerEvent{.timer_id = typed.timer_id},
                            generation);
                    } else {
                        events_.erase(
                            std::remove_if(
                                events_.begin(),
                                events_.end(),
                                [origin, generation, &typed](
                                    const QueuedEvent& queued) {
                                    return queued.summary.kind ==
                                               PendingEventKind::timer &&
                                        queued.summary.to == origin &&
                                        queued.generation == generation &&
                                        std::get<TimerEvent>(queued.event)
                                                .timer_id == typed.timer_id;
                                }),
                            events_.end());
                    }
                },
                std::move(action));
        }
    }

    void deliver_index(const std::size_t index, const bool record_step) {
        auto queued = std::move(events_.at(index));
        events_.erase(events_.begin() + static_cast<std::ptrdiff_t>(index));
        now_ = std::max(now_, queued.summary.ready_at);
        if (record_step) {
            std::ostringstream line;
            line << "step " << queued.summary.id << ' '
                 << queued.summary.ready_at << ' '
                 << kind_name(queued.summary.kind) << ' '
                 << queued.summary.to.value;
            history_.push_back(line.str());
        }

        auto& destination = node(queued.summary.to);
        if (queued.summary.kind == PendingEventKind::message &&
            queued.summary.from &&
            partitions_.contains(
                undirected(*queued.summary.from, queued.summary.to))) {
            return;
        }
        if (!destination.running ||
            destination.generation != queued.generation) {
            return;
        }
        if (queued.pending_record) {
            destination.durable.push_back(std::move(*queued.pending_record));
        }

        std::vector<NodeAction> actions;
        try {
            actions = destination.adapter->step(queued.event);
        } catch (const std::exception& error) {
            fail(std::string("node adapter threw: ") + error.what());
        } catch (...) {
            fail("node adapter threw a non-standard exception");
        }
        process_actions(
            queued.summary.to,
            queued.generation,
            std::move(actions));
    }

    std::vector<NodeId> topology_;
    std::uint64_t seed_{};
    std::uint64_t random_state_{};
    NodeFactory factory_;
    LogicalTime now_{};
    EventId next_event_id_{1};
    std::map<std::uint64_t, NodeState> nodes_;
    std::vector<QueuedEvent> events_;
    std::set<Link> partitions_;
    std::map<Link, LogicalTime> network_delays_;
    std::map<std::uint64_t, LogicalTime> persistence_delays_;
    std::map<Link, std::size_t> drops_;
    std::map<Link, std::size_t> duplicates_;
    std::vector<std::string> history_;
};

Simulator::Simulator(
    std::vector<NodeId> topology,
    const std::uint64_t seed,
    NodeFactory factory)
    : impl_(std::make_unique<Impl>(
          std::move(topology),
          seed,
          std::move(factory))) {}

Simulator::~Simulator() = default;
Simulator::Simulator(Simulator&&) noexcept = default;
Simulator& Simulator::operator=(Simulator&&) noexcept = default;

std::vector<NodeId> Simulator::three_node_topology() {
    return {{1}, {2}, {3}};
}

std::vector<NodeId> Simulator::five_node_topology() {
    return {{1}, {2}, {3}, {4}, {5}};
}

LogicalTime Simulator::now() const noexcept {
    return impl_->now_;
}

std::uint64_t Simulator::seed() const noexcept {
    return impl_->seed_;
}

bool Simulator::running(const NodeId node) const {
    return impl_->node(node).running;
}

std::span<const DurableRecord> Simulator::durable_records(
    const NodeId node) const {
    return impl_->node(node).durable;
}

std::vector<PendingEvent> Simulator::pending_events() const {
    std::vector<PendingEvent> result;
    result.reserve(impl_->events_.size());
    for (const auto& event : impl_->events_) {
        result.push_back(event.summary);
    }
    std::sort(
        result.begin(),
        result.end(),
        [](const PendingEvent& first, const PendingEvent& second) {
            if (first.ready_at != second.ready_at) {
                return first.ready_at < second.ready_at;
            }
            return first.id < second.id;
        });
    return result;
}

bool Simulator::step() {
    const auto eligible = impl_->earliest_indices();
    if (eligible.empty()) {
        return false;
    }
    const auto selected =
        eligible[impl_->random() % static_cast<std::uint64_t>(eligible.size())];
    impl_->deliver_index(selected, true);
    return true;
}

void Simulator::run(const std::size_t max_events) {
    std::size_t processed{};
    while (!impl_->events_.empty()) {
        if (processed == max_events) {
            impl_->fail("simulation event limit exceeded");
        }
        step();
        ++processed;
    }
}

void Simulator::deliver(const EventId event) {
    const auto eligible = impl_->earliest_indices();
    const auto found = std::find_if(
        eligible.begin(),
        eligible.end(),
        [this, event](const std::size_t index) {
            return impl_->events_[index].summary.id == event;
        });
    if (found == eligible.end()) {
        throw std::invalid_argument(
            "event is not pending at the earliest logical time");
    }
    impl_->record_control("deliver " + std::to_string(event));
    impl_->deliver_index(*found, false);
}

void Simulator::delay_message(
    const EventId event,
    const LogicalTime additional_delay) {
    const auto found = std::find_if(
        impl_->events_.begin(),
        impl_->events_.end(),
        [event](const Impl::QueuedEvent& queued) {
            return queued.summary.id == event;
        });
    if (found == impl_->events_.end() ||
        found->summary.kind != PendingEventKind::message) {
        throw std::invalid_argument("event is not a pending message");
    }
    if (additional_delay >
        std::numeric_limits<LogicalTime>::max() - found->summary.ready_at) {
        throw std::overflow_error("message delay overflows logical time");
    }
    found->summary.ready_at += additional_delay;
    impl_->record_control(
        "delay-message " + std::to_string(event) + " " +
        std::to_string(additional_delay));
}

void Simulator::partition(const NodeId first, const NodeId second) {
    impl_->node(first);
    impl_->node(second);
    impl_->partitions_.insert(undirected(first, second));
    impl_->record_control(
        "partition " + std::to_string(first.value) + " " +
        std::to_string(second.value));
}

void Simulator::heal(const NodeId first, const NodeId second) {
    impl_->node(first);
    impl_->node(second);
    impl_->partitions_.erase(undirected(first, second));
    impl_->record_control(
        "heal " + std::to_string(first.value) + " " +
        std::to_string(second.value));
}

void Simulator::set_network_delay(
    const NodeId from,
    const NodeId to,
    const LogicalTime delay) {
    impl_->node(from);
    impl_->node(to);
    impl_->network_delays_[directed(from, to)] = delay;
    impl_->record_control(
        "network-delay " + std::to_string(from.value) + " " +
        std::to_string(to.value) + " " + std::to_string(delay));
}

void Simulator::set_persistence_delay(
    const NodeId node,
    const LogicalTime delay) {
    impl_->node(node);
    impl_->persistence_delays_[node.value] = delay;
    impl_->record_control(
        "persistence-delay " + std::to_string(node.value) + " " +
        std::to_string(delay));
}

void Simulator::drop_next(
    const NodeId from,
    const NodeId to,
    const std::size_t count) {
    impl_->node(from);
    impl_->node(to);
    impl_->drops_[directed(from, to)] += count;
    impl_->record_control(
        "drop-next " + std::to_string(from.value) + " " +
        std::to_string(to.value) + " " + std::to_string(count));
}

void Simulator::duplicate_next(
    const NodeId from,
    const NodeId to,
    const std::size_t additional_copies) {
    impl_->node(from);
    impl_->node(to);
    impl_->duplicates_[directed(from, to)] += additional_copies;
    impl_->record_control(
        "duplicate-next " + std::to_string(from.value) + " " +
        std::to_string(to.value) + " " +
        std::to_string(additional_copies));
}

void Simulator::crash(const NodeId node) {
    auto& state = impl_->node(node);
    if (!state.running) {
        throw std::logic_error("node is already crashed");
    }
    state.running = false;
    ++state.generation;
    state.adapter.reset();
    impl_->events_.erase(
        std::remove_if(
            impl_->events_.begin(),
            impl_->events_.end(),
            [node](const Impl::QueuedEvent& event) {
                return event.summary.to == node &&
                    event.summary.kind != PendingEventKind::message;
            }),
        impl_->events_.end());
    impl_->record_control("crash " + std::to_string(node.value));
}

void Simulator::restart(const NodeId node) {
    auto& state = impl_->node(node);
    if (state.running) {
        throw std::logic_error("node is already running");
    }
    state.adapter = impl_->make_node(node);
    state.running = true;
    impl_->enqueue_start(node);
    impl_->record_control("restart " + std::to_string(node.value));
}

void Simulator::require(
    const bool condition,
    const std::string_view message) const {
    if (!condition) {
        impl_->fail(message);
    }
}

std::string Simulator::trace() const {
    return impl_->trace();
}

Simulator Simulator::replay(
    const std::string_view trace,
    NodeFactory factory) {
    std::istringstream input{std::string(trace)};
    std::string line;
    if (!std::getline(input, line)) {
        throw SimulationFailure("empty replay trace");
    }
    std::istringstream header(line);
    std::string version;
    std::string seed_label;
    std::string seed_text;
    std::string nodes_label;
    header >> version >> seed_label >> seed_text >> nodes_label;
    if (version != "sim-v1" || seed_label != "seed" ||
        nodes_label != "nodes") {
        throw SimulationFailure("invalid replay trace header");
    }
    std::vector<NodeId> topology;
    std::string node_text;
    while (header >> node_text) {
        topology.push_back({parse_number(node_text)});
    }
    Simulator result(topology, parse_number(seed_text), std::move(factory));
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream words(line);
        std::string command;
        words >> command;
        if (command == "step") {
            std::string id;
            std::string ready_at;
            std::string kind;
            std::string destination;
            words >> id >> ready_at >> kind >> destination;
            const auto event_id = parse_number(id);
            const auto found = std::find_if(
                result.impl_->events_.begin(),
                result.impl_->events_.end(),
                [event_id](const Impl::QueuedEvent& queued) {
                    return queued.summary.id == event_id;
                });
            if (found == result.impl_->events_.end()) {
                result.impl_->fail("replay event is not pending");
            }
            const auto eligible = result.impl_->earliest_indices();
            const auto index = static_cast<std::size_t>(
                std::distance(result.impl_->events_.begin(), found));
            if (std::find(eligible.begin(), eligible.end(), index) ==
                eligible.end()) {
                result.impl_->fail(
                    "replay event is not at the earliest logical time");
            }
            const auto& summary = result.impl_->events_[index].summary;
            if (summary.ready_at != parse_number(ready_at) ||
                kind_name(summary.kind) != kind ||
                summary.to.value != parse_number(destination)) {
                result.impl_->fail("replay event metadata does not match");
            }
            static_cast<void>(result.impl_->random());
            result.impl_->deliver_index(index, true);
            continue;
        }
        if (command != "control") {
            result.impl_->fail("unknown replay trace command");
        }
        std::string control;
        std::string first;
        std::string second;
        std::string third;
        words >> control >> first;
        if (control == "crash") {
            result.crash({parse_number(first)});
        } else if (control == "restart") {
            result.restart({parse_number(first)});
        } else if (control == "deliver") {
            result.deliver(parse_number(first));
        } else {
            words >> second;
            if (control == "partition") {
                result.partition({parse_number(first)}, {parse_number(second)});
            } else if (control == "heal") {
                result.heal({parse_number(first)}, {parse_number(second)});
            } else if (control == "delay-message") {
                result.delay_message(
                    parse_number(first),
                    parse_number(second));
            } else {
                words >> third;
                if (control == "network-delay") {
                    result.set_network_delay(
                        {parse_number(first)},
                        {parse_number(second)},
                        parse_number(third));
                } else if (control == "persistence-delay") {
                    result.set_persistence_delay(
                        {parse_number(first)},
                        parse_number(second));
                } else if (control == "drop-next") {
                    result.drop_next(
                        {parse_number(first)},
                        {parse_number(second)},
                        parse_number(third));
                } else if (control == "duplicate-next") {
                    result.duplicate_next(
                        {parse_number(first)},
                        {parse_number(second)},
                        parse_number(third));
                } else {
                    result.impl_->fail("unknown replay trace control");
                }
            }
        }
    }
    return result;
}

}  // namespace kura::metadata::raft::simulation
