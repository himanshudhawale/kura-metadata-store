# Design 0006: Deterministic Raft simulator

## Status and scope

Accepted as the Phase 4 simulation foundation. This change supplies a
logical-time scheduler and an adapter boundary for the Raft implementation
planned by issue #6. It deliberately supplies a deterministic probe node only:
there is no election, replication, quorum, or consensus implementation in this
change.

## Problem

Raft failures depend on order rather than merely concurrency. A vote can arrive
before or after a durable term update, a node can restart between an append and
its completion, and partitions can expose uncommon message interleavings.
Tests based on threads, sleeps, sockets, and real disks make those orders slow,
flaky, and difficult to reproduce.

The harness must control nodes, messages, timers, disk completions, crashes,
restarts, drops, duplication, partitions, and reordering without consulting a
wall clock. Every failed schedule must retain enough information to execute the
same schedule again.

## Why a separate adapter

The Raft core does not exist yet. Coupling the simulator to placeholder
election methods would either constrain issue #6 prematurely or create a fake
consensus implementation. Instead, `NodeAdapter` is the environmental boundary
that the real core can implement or be wrapped by:

```text
environment event -> NodeAdapter::step(event) -> zero or more actions
```

Events are startup/recovery, peer message, timer firing, and persistence
completion. Actions are peer send, append-to-durable-log, set timer, and cancel
timer. Payloads are opaque bytes, so the core owns Raft protocol and durable
state encoding. `NodeAdapter::step` is synchronous and must be deterministic;
the simulator owns all delayed effects.

`StartEvent` supplies the node identity, sorted peer set, and completed durable
records. A future Raft adapter can restore its term, vote, and log before
producing actions. `PersistedEvent` is the only acknowledgement that makes a
`PersistAction` durable. This is the stable seam issue #6 needs without making
claims about consensus.

## Design

### Logical time and event selection

`LogicalTime` is an unsigned integer. Events carry a ready tick and monotonic
event ID. The scheduler advances directly to the smallest pending ready tick;
it never sleeps and includes no clock API. When several events share that tick,
a local SplitMix64 generator selects one. The generator algorithm is part of
the simulator implementation, and its initial seed is recorded.

The standard topologies contain node IDs `1..3` and `1..5`. Custom unique,
nonempty topologies are also accepted. Startup events are scheduled at tick
zero. Timers, link delays, and persistence delays use checked logical-time
addition.

### Network controls

Directed link delays apply when a send action is emitted. `drop_next` consumes
the requested number of subsequent sends on a directed link.
`duplicate_next` turns the next send into the original plus the requested
number of identical copies. An undirected partition is evaluated at delivery,
so a message queued before a partition can be dropped while it is active.
Healing affects later deliveries only and does not resurrect dropped events.

`delay_message` adds delay to a particular queued message. `pending_events`
exposes stable event IDs, and `deliver` chooses a particular event among those
at the earliest tick. Together these operations permit precise reorderings;
normal `step` uses the seeded selection.

### Persistence, crashes, and restart

Persistence is modeled as an append of an opaque record followed by a delayed
completion. Bytes enter recovered durable state immediately before the
matching completion is delivered to the same live node generation. A crash
cancels that generation's timers, startup, and incomplete disk operations.
Thus an unacknowledged simulated write is conservatively lost; future work can
add an explicit ambiguous-write mode if the Raft storage adapter needs it.

A crash destroys the adapter and increments its generation. Messages addressed
to the old generation remain in the queue but are discarded rather than
entering a restarted node. Restart constructs a fresh adapter and schedules a
new startup event containing only completed durable records. Partition, delay,
drop, and duplication settings survive node restart because they belong to the
environment.

### Trace and replay

The text trace starts with:

```text
sim-v1 seed <seed> nodes <ordered ids...>
```

It then records every external fault control and every selected event ID,
ready tick, kind, and destination. Explicit deliveries are controls. Trace
format version `sim-v1` is validated by `Simulator::replay`, which reconstructs
the topology, applies controls, and forces recorded event choices against a
fresh factory. Missing, unknown, or no-longer-eligible events fail replay
rather than silently selecting another schedule.

`Simulator::require`, adapter exceptions, logical-time overflow, duplicate
outstanding persistence IDs, and event-budget exhaustion throw
`SimulationFailure` with the complete trace. The standalone test runner prints
that exception, making a CI failure directly replayable.

## Alternatives considered

- **Threads, sleeps, sockets, and temporary disk files:** more realistic at
  the operating-system boundary, but nondeterministic and too slow for broad
  schedule exploration. Integration tests can complement, not replace, this
  harness.
- **A priority queue ordered only by insertion:** reproducible but explores a
  single friendly ordering. Seeded tie selection and explicit delay/delivery
  controls explore adversarial orders.
- **A mock Raft implementation:** would falsely imply consensus behavior and
  likely be discarded by issue #6. The probe instead observes boundary
  behavior only.
- **Callbacks for send and persistence:** callbacks make ownership and crash
  cancellation implicit. Typed event/action variants keep effects visible and
  replayable.
- **Wall time converted to milliseconds:** still permits scheduler and clock
  jitter. Logical ticks have no conversion to physical time.

## Edge cases and invariants

- Empty or duplicate-node topologies and null adapters are rejected.
- A node cannot be crashed twice or restarted while running.
- Events cannot be scheduled in the past; time addition cannot wrap.
- Only a pending message can receive an extra delay.
- Explicit delivery is limited to events at the earliest logical tick.
- A timer set with an existing ID replaces the prior timer for that node
  generation; cancelling an absent timer is harmless.
- Persistence request IDs must be unique while outstanding in one node
  generation.
- Drops occur before duplication. A dropped send creates no copies.
- Messages to crashed or superseded node generations and partitioned
  deliveries are consumed without invoking the adapter.
- The event budget detects zero-delay action loops and includes their trace.

## Validation

`kura_raft_simulation_tests` uses a deterministic probe adapter, not a Raft
algorithm. It validates:

- startup and peer sets for three- and five-node topologies;
- identical histories and observations for identical seeds;
- exact trace replay and differing seeded schedules;
- partitions, directed delay, drop, duplication, and explicit reordering;
- delayed persistence, acknowledgement, crash loss, durable recovery, and
  fresh adapter generations;
- logical timers without wall-clock reads; and
- trace-bearing invariant and adapter failures.

The simulator test is wired into CTest alongside the existing state-machine
and durable-storage tests.
