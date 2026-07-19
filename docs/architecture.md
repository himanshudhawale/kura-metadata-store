# Architecture

## 1. Goal and scope

Kura Metadata Store holds small control-plane values that require stronger
consistency than ordinary caches:

- Cluster configuration
- Service registration
- Leader and job ownership
- Schema and policy versions
- Immutable snapshot pointers

It is not intended for large analytical table data, application blobs, or
high-throughput event streams. Kura Engine stores table segments in object
storage and places only compact authoritative metadata here.

## 2. Safety target

The eventual distributed service targets strict serializability for key-value
transactions:

- Every completed operation appears to occur at one instant between invocation
  and response.
- Transactions form one total order consistent with real-time ordering.
- A successful write is not acknowledged until a quorum has durably accepted
  its Raft log entry.

Serializable local reads may be offered explicitly for callers that accept
staleness. The default critical path remains linearizable.

## 3. Component boundaries

```text
+---------------------------------------------------+
| Client libraries and Kura helper                  |
+---------------------------------------------------+
| KV | Transaction | Watch | Lease | Cluster APIs   |
+---------------------------------------------------+
| Request validation, auth, quotas, idempotency     |
+---------------------------------------------------+
| Raft: election, replication, commit, snapshots    |
+---------------------------------------------------+
| Deterministic state-machine apply                 |
+---------------------------------------------------+
| MVCC backend | event history | lease records      |
+---------------------------------------------------+
| Raft WAL | snapshots | checksums | fsync          |
+---------------------------------------------------+
```

### State machine

The state machine accepts an ordered command and returns a deterministic result.
It does not elect leaders, contact peers, inspect wall-clock time, or decide
whether a command is committed.

### Raft

Raft owns terms, votes, log replication, commitment, and membership. It applies
an entry only after the commit index includes that entry.

### Persistence

The atomic Raft hard-state record persists `currentTerm` and `votedFor`; the
write-ahead log persists entries. The Raft core receives an explicit durable
completion event before it may emit a granted vote or a higher-term success
response. State-machine snapshots bound recovery time. A crash must leave
either the old complete hard state/snapshot or the new complete one, never a
partially accepted publication.

## 4. Revision model

A cluster-wide 64-bit revision advances once for each successful atomic
mutation. A transaction touching several keys produces one revision.

Each visible key contains:

```text
key
value
version
createRevision
modRevision
leaseId
```

- `version` counts writes in the key's current generation.
- `createRevision` identifies when the current generation began.
- `modRevision` identifies its latest mutation.
- Deletion ends a generation; recreating the key starts at version 1.

The current Phase 1 implementation retains only current values. Historical MVCC
reads and compaction arrive with the persistent backend.

## 5. Write path

The final distributed write path is:

1. Authenticate and validate request.
2. Forward to or reject with the current leader.
3. Convert request into one deterministic command.
4. Append command to the leader's WAL.
5. Replicate it to followers.
6. Wait for a voting majority to persist it.
7. Advance commit index.
8. Apply committed entries in index order.
9. Persist state-machine apply position atomically with state changes.
10. Return the deterministic response.

A dropped client connection creates ambiguity: the command may have committed.
Mutating clients therefore supply an idempotency token whose result is stored in
replicated state for a bounded period.

## 6. Read path

### Linearizable

The leader performs a ReadIndex quorum confirmation, records a committed index,
waits until the state machine has applied through that index, and then reads.
A partitioned former leader cannot safely answer from local state.

### Serializable

Any member may read its applied local state. This avoids a quorum round trip but
can return stale data. It must be an explicit caller choice.

Lease-based leader reads are deferred until clock-drift bounds and a safety
argument exist.

## 7. Snapshots and compaction

A Raft snapshot contains:

- State-machine state at one applied index
- Last included Raft index and term
- Membership configuration
- Store revision and compaction revision
- Integrity hash and format version

After durable snapshot publication, log entries through the included index may
be removed. A follower missing removed entries catches up through
`InstallSnapshot`.

Compacting MVCC history makes old revisions unavailable. A watch that asks for
a compacted revision receives an explicit compaction error and must resynchronize
with a current range read.

## 8. Membership

Membership changes happen one node at a time. New nodes enter as non-voting
learners, receive a snapshot and log tail, and become voting members only after
catching up. This avoids increasing quorum before a new node can participate.

Each cluster has a unique cluster ID and bootstrap token to prevent accidental
cross-cluster joining.

## 9. Security boundary

Production mode requires:

- Mutual TLS for peer traffic
- Authenticated TLS for clients
- Separate peer and client listeners
- Key-range role permissions
- Audit events for administrative and mutation requests
- Secrets supplied externally rather than committed configuration

Security is part of protocol design, not a proxy added after production.

## 10. Observability

Every request carries a request ID and trace context. Required diagnostics
include:

- Current term, leader, commit index, and applied index
- WAL fsync and backend commit latency
- Proposal, transaction, watch, and lease counts
- Log and backend sizes
- Watch lag and compaction errors
- Leader changes and peer round-trip latency
- `/livez`, `/readyz`, and Prometheus-compatible `/metrics`

Readiness includes a successful linearizable read check; process liveness alone
does not prove the node can safely serve traffic.
