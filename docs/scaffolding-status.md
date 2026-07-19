# Scaffolding Status

The repository contains header-level boundaries for future phases so API and
ownership decisions can be reviewed early.

Implemented now:

- Immutable binary keys and values
- Current versioned key state
- Point and half-open range reads
- Put, erase, and modification-revision compare-and-set
- Synchronized concurrent access

Declared but not implemented:

- Full transactions
- Historical MVCC
- Watches and leases
- WAL, snapshots, and durable backend
- Raft and membership
- Network server, authentication, and metrics
- Remote client and Kura helper

Scaffolding must not be used as evidence of availability, durability,
linearizability across nodes, or production readiness.
