# Scaffolding Status

The repository contains header-level boundaries for future phases so API and
ownership decisions can be reviewed early.

Implemented now:

- Immutable binary keys and values
- Current versioned key state
- Point and half-open range reads
- Put, erase, and modification-revision compare-and-set
- In-memory If/Then/Else transactions with typed range, put, and delete results
- Atomic multi-key transaction publication using one revision
- Resumable in-process watches with bounded history and backpressure
- Deterministic lease grant, keepalive, TTL, revoke, and expiry
- Fenced transaction ownership, key attachment, and watched atomic cascade
  deletion
- Canonical in-memory key/lease snapshot representation
- Synchronized concurrent access
- Dependency-free CRC32C, segmented WAL, and atomic snapshot storage boundary
- Synchronized WAL append, strict prefix recovery, snapshot integrity
  discovery, and snapshot-covered conservative segment deletion
- Executable Raft Figure 2 rule catalog, typed deterministic transitions,
  ordered persistence/network/apply effects, and state-invariant validation
- Atomic, checksummed Raft term/vote storage with explicit durable-completion
  events and deterministic write-boundary fault injection
- Deterministic follower/candidate election core with seeded logical
  deadlines, RequestVote, persistence-gated sends, and simulator adapter
- C++23 Kura helper with RAII writer/reader leases, fenced publication,
  reader-protected metadata collection, and watch compaction resynchronization
- In-process backend adapter and deterministic uncertain-response fault seam
- Deterministic AppendEntries heartbeats, conflict repair, leader
  nextIndex/matchIndex retry, and persistence-gated follower success
- Current-term majority commitment, safe prior-term indirect commitment, and
  ordered completion-driven state-machine apply
- Context-correlated ReadIndex with current-term quorum confirmation,
  applied-index gating, explicit uncertainty, and bounded pending reads
- Canonical applied-state Raft snapshots, publication-gated log compaction,
  whole-snapshot InstallSnapshot, and lagging-follower suffix resumption
- Sound bounded get/put/erase/CAS history checking with strict validation,
  deterministic search, explicit inconclusive results, and replayable
  one-minimal counterexamples
- End-to-end deterministic Raft-core acceptance under election, persistence,
  partition, delay, duplication, reordering, crash, restart, ReadIndex, and
  snapshot catch-up schedules in three- and five-voter clusters

Declared but not implemented:

- Historical MVCC
- Integration of state-machine commands with the WAL and snapshot body
- Embedded durable backend
- Production Raft transport and membership changes
- Network server, authentication, and metrics
- Remote client transport and real leader-failover integration
- Production client-history capture and full transaction/watch/lease
  linearizability models

Scaffolding must not be used as evidence of availability, durability,
linearizability across nodes, or production readiness.
