# ADR-0001: Capability Claims Follow Tested Phases

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

The project is intended to become a distributed metadata store, but its first
implementation is a single-node deterministic state machine. Describing that
foundation as distributed would imply replication, quorum, failover, and
partition behavior that do not exist.

## Decision

Documentation, releases, and examples will identify the phase that supplies
each guarantee:

- Phase 1: in-memory state-machine semantics
- Phase 2: full transactions, watches, and leases
- Phase 3: single-node durability
- Phase 4: Raft replication and distributed linearizability
- Phase 5+: operations, Kura integration, and production maturity

The project may describe its long-term goal as a distributed metadata store,
but current-status text must state missing guarantees prominently.

## Consequences

Users can distinguish implemented behavior from roadmap intent. Early releases
may sound less impressive, but they will not invite reliance on nonexistent
safety or availability.
