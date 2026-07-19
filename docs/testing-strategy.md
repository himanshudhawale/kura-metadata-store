# Testing Strategy

## Layers

- Unit tests prove value, revision, comparison, and range semantics.
- Model tests compare optimized state machines with a simple reference model.
- Property tests generate command sequences and validate invariants.
- Recovery tests interrupt every WAL and snapshot write boundary.
- Raft hard-state tests interrupt every create/write/sync/close/replace/
  directory-sync boundary and truncate or corrupt every record byte.
- Concurrency tests exercise CAS, watch registration, lease expiry, and apply.
- Simulation tests control time, messages, partitions, and node crashes.
- Figure 2 catalog tests execute every rule and assert hard-state completion
  correlation plus persistence-before-send ordering.
- Election-core tests execute seeded deadlines, split votes, vote freshness,
  persistence delays, restart, duplicate grants, and odd-cluster schedules.
- Linearizability tests analyze complete concurrent client histories.
- Compatibility tests read every supported durable and protocol version.

## Required tool configurations

- GCC and Clang on Linux
- MSVC on Windows
- AddressSanitizer plus UndefinedBehaviorSanitizer
- ThreadSanitizer in a separate process
- Release and debug builds

No chaos result is accepted unless the random seed and operation history are
preserved for reproduction.

## Deterministic Raft simulation

The [Raft simulator design](design/0006-deterministic-raft-simulator.md)
defines the logical-time event/action boundary intended for the future Raft
core. Simulation tests must not use sleeps or read wall-clock time. A failure
must use `Simulator::require` (or propagate an adapter failure) so its `sim-v1`
seed, fault controls, and selected event history are printed and can be passed
to `Simulator::replay`.

The deterministic probe validates scheduling and every fault control. The
election adapter additionally validates the RequestVote-only slice; neither is
evidence that log replication, committed consensus, or a distributed service
is implemented.
