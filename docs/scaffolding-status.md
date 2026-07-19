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
- In-process exact-key and range watches with bounded resumable event history
- Atomic transaction watch batches, filters, progress, and cancellation
- Synchronized concurrent access

Declared but not implemented:

- Historical MVCC
- Leases
- Network watch streaming and persistent watch history
- WAL, snapshots, and durable backend
- Raft and membership
- Network server, authentication, and metrics
- Remote client and Kura helper

Scaffolding must not be used as evidence of availability, durability,
linearizability across nodes, or production readiness.
