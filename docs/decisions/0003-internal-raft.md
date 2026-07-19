# ADR-0003: Implement Raft Internally

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

Kura Metadata Store needs consensus for leader election, durable log
replication, majority commitment, linearizable reads, snapshots, and membership
changes.

Using a C++ library such as NuRaft would reduce initial work, but consensus is a
core learning and ownership goal for this project. A library would also place
Kura's persistence ordering, failure behavior, and protocol compatibility
behind an external abstraction.

## Decision

Kura Metadata Store will implement Raft internally from the published Raft paper
and Ongaro dissertation. It will not depend on NuRaft or another consensus
implementation.

The implementation must preserve these boundaries:

1. The Raft core is deterministic and event-driven.
2. Time enters as logical tick events; the core does not read wall-clock time.
3. Network sends, WAL writes, snapshot installation, and state-machine apply are
   explicit outputs completed by adapters.
4. Persistent term, vote, and log changes complete before success responses are
   emitted.
5. State-machine commands apply only after the commit index advances.
6. Linearizable reads use ReadIndex initially; leader leases are deferred.
7. Membership changes occur one member at a time, with learners before voters.
8. Every protocol deviation from the paper requires a separate ADR.

## Required evidence

Before distributed claims:

- Deterministic election and replication simulation
- Figure 2 rule-by-rule traceability to code and tests
- Crash tests around every persistent-state transition
- Minority-partition and former-leader read tests
- Snapshot installation and lagging-follower tests
- Linearizability checking over concurrent histories
- Reproducible randomized schedules with recorded seeds

## Consequences

The project gains full control and a deeper understanding of consensus.
Delivery will be slower and correctness risk is higher than adopting a mature
library. Phase ordering, simulation, persistence tests, and external review are
therefore mandatory rather than optional.

## References

- [Raft paper](https://raft.github.io/raft.pdf)
- [Ongaro dissertation](https://github.com/ongardie/dissertation/blob/master/stanford.pdf)
