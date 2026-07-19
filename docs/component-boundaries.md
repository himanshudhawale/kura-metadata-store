# Component Boundaries

The initial headers reserve stable conceptual boundaries without claiming that
future phases are implemented.

| Directory | Responsibility | First implementation phase |
|---|---|---|
| `core/` | Status, revisions, commands, limits, state-machine contract | 1-2 |
| `kv/` | Range, put, delete, compare, and transaction request models | 2 |
| `watch/` | Resumable ordered change streams | 2 |
| `lease/` | Replicated TTL records and expiration proposals | 2 |
| `storage/` | WAL, backend, checksums, snapshots, and quotas | 3 |
| `raft/` | Election, replication, commitment, ReadIndex, snapshots, membership | 4 |
| `testing/` | Bounded linearizability history models and replay | 4 |
| `server/` | Process configuration, auth context, health, and metrics | 3-5 |
| `client/` | Endpoint selection, retry, transactions, watch resume | 3-5 |
| `kura/` | Safe Kura Engine snapshot-publication composition | 6 |

Headers are contracts for review. A declaration is not a completed feature.
README and release notes continue to describe only tested behavior.
