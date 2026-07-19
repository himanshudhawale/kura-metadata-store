#pragma once

#include "kura/metadata/raft/node_id.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace kura::metadata::raft::simulation {

using LogicalTime = std::uint64_t;
using EventId = std::uint64_t;
using RequestId = std::uint64_t;
using TimerId = std::uint64_t;
using Bytes = std::vector<std::uint8_t>;

struct DurableRecord {
    RequestId request_id{};
    Bytes bytes;

    bool operator==(const DurableRecord&) const = default;
};

struct StartEvent {
    NodeId self;
    std::vector<NodeId> peers;
    std::vector<DurableRecord> durable_records;
};

struct MessageEvent {
    NodeId from;
    Bytes bytes;
};

struct TimerEvent {
    TimerId timer_id{};
};

struct PersistedEvent {
    RequestId request_id{};
};

using NodeEvent =
    std::variant<StartEvent, MessageEvent, TimerEvent, PersistedEvent>;

struct SendAction {
    NodeId to;
    Bytes bytes;
};

struct PersistAction {
    RequestId request_id{};
    Bytes bytes;
};

struct SetTimerAction {
    TimerId timer_id{};
    LogicalTime delay{};
};

struct CancelTimerAction {
    TimerId timer_id{};
};

using NodeAction =
    std::variant<SendAction, PersistAction, SetTimerAction, CancelTimerAction>;

class NodeAdapter {
public:
    virtual ~NodeAdapter() = default;

    [[nodiscard]] virtual std::vector<NodeAction> step(
        const NodeEvent& event) = 0;
};

using NodeFactory = std::function<std::unique_ptr<NodeAdapter>(NodeId)>;

enum class PendingEventKind {
    start,
    message,
    timer,
    disk_completion,
};

struct PendingEvent {
    EventId id{};
    LogicalTime ready_at{};
    PendingEventKind kind{};
    std::optional<NodeId> from;
    NodeId to;
};

class SimulationFailure final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class Simulator {
public:
    Simulator(
        std::vector<NodeId> topology,
        std::uint64_t seed,
        NodeFactory factory);
    ~Simulator();

    Simulator(Simulator&&) noexcept;
    Simulator& operator=(Simulator&&) noexcept;
    Simulator(const Simulator&) = delete;
    Simulator& operator=(const Simulator&) = delete;

    [[nodiscard]] static std::vector<NodeId> three_node_topology();
    [[nodiscard]] static std::vector<NodeId> five_node_topology();

    [[nodiscard]] LogicalTime now() const noexcept;
    [[nodiscard]] std::uint64_t seed() const noexcept;
    [[nodiscard]] bool running(NodeId node) const;
    [[nodiscard]] std::span<const DurableRecord> durable_records(
        NodeId node) const;
    [[nodiscard]] std::vector<PendingEvent> pending_events() const;

    bool step();
    void run(std::size_t max_events = 100'000);
    void deliver(EventId event);
    void delay_message(EventId event, LogicalTime additional_delay);

    void partition(NodeId first, NodeId second);
    void heal(NodeId first, NodeId second);
    void set_network_delay(
        NodeId from,
        NodeId to,
        LogicalTime delay);
    void set_persistence_delay(NodeId node, LogicalTime delay);
    void drop_next(NodeId from, NodeId to, std::size_t count = 1);
    void duplicate_next(
        NodeId from,
        NodeId to,
        std::size_t additional_copies = 1);
    void crash(NodeId node);
    void restart(NodeId node);

    void require(bool condition, std::string_view message) const;

    [[nodiscard]] std::string trace() const;
    [[nodiscard]] static Simulator replay(
        std::string_view trace,
        NodeFactory factory);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace kura::metadata::raft::simulation
