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

Declared but not implemented:

- Historical MVCC
- Integration of state-machine commands with the WAL and snapshot body
- Embedded durable backend
- Production Raft node, membership, snapshots, and ReadIndex
- Network server, authentication, and metrics
- Remote client and Kura helper

Scaffolding must not be used as evidence of availability, durability,
linearizability across nodes, or production readiness.
