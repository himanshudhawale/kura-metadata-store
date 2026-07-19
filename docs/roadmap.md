# Implementation Roadmap

The roadmap advances only when each phase's guarantees are tested. Feature
requests are welcome at any time, but implementation must respect dependencies.

## Phase 1: Deterministic in-memory state machine

**Status:** Started. **Not distributed, durable, replicated, or highly
available.**

Deliver:

- Immutable binary key/value model
- Cluster revision and per-key version semantics
- Point get and half-open range reads
- Put and delete
- Modification-revision compare-and-set
- Deterministic unit tests

Exit criteria:

- The same command sequence always produces equal state and responses.
- Failed comparisons and no-op deletes do not advance revision.
- Mutable caller byte arrays cannot change stored values.
- Range ordering uses unsigned lexicographic bytes.

## Phase 2: Full transactional state machine

**Status:** Started. The current in-memory slice implements general
If/Then/Else transactions, multi-key single-revision mutations, and deterministic
lease lifecycle with fenced ownership. Resumable in-process watches and their
bounded event history are implemented. Historical MVCC and request
deduplication remain unimplemented.

Deliver:

- General If/Then/Else transactions
- Compare version, create revision, modification revision, value, and lease
- Multi-key atomic mutations using one revision
- Historical MVCC reads
- Watch event history and resume
- Lease grant, keepalive, revoke, expiry, and cascade delete
- Replicated request-deduplication model

Exit criteria:

- Transaction events are atomic and share one revision.
- Watches are ordered, unique, reliable, resumable, and bookmarkable inside the
  retained history window.
- Lease expiry is deterministic and does not depend on follower wall clocks.

## Phase 3: Durable single-node service

Deliver:

- Checksummed, segmented write-ahead log
- `fsync` before successful mutation response
- Embedded transactional backend
- Atomic snapshots and WAL truncation
- Compaction, quota, backup, and restore
- Crash-recovery and torn-write tests
- Client API server with TLS

Exit criteria:

- Acknowledged writes survive process and machine restart within the documented
  storage durability assumptions.
- Recovery either rejects corruption explicitly or restores one valid prefix;
  it never fabricates success.
- Restore preserves or safely bumps revisions.

This phase is durable but remains single-node and not highly available.

## Phase 4: Raft replication

**Status:** Deterministic core acceptance complete for issue #6. Elections,
RequestVote, AppendEntries, majority commitment, ordered application,
quorum-confirmed ReadIndex, snapshot installation, faulted three-/five-node
simulation, and bounded history checking are implemented. Phase 4 production
service integration remains in progress and no availability claim is made.

Deliver:

- Internal Raft implementation derived from the paper and dissertation
- Terms, votes, elections, and AppendEntries
- Persistent Raft state before peer acknowledgement
- Majority commit and ordered apply
- ReadIndex linearizable reads
- Three-node integration environment
- Snapshot installation for lagging followers
- Leader-failure, partition, retry, and restart testing

Exit criteria:

- A three-node cluster remains safe under minority failure.
- A partitioned old leader cannot serve a linearizable read.
- Histories pass a linearizability checker.
- Only this phase may introduce distributed and high-availability claims.

The first three exit criteria are accepted for the deterministic core test
boundary. The last remains intentionally unmet for the product: production
transport, server integration, durable state-machine wiring, membership, and
operations must exist before distributed or high-availability claims.

The consensus core will not depend on NuRaft or another Raft implementation.
It will expose deterministic input/output events so election, replication,
persistence ordering, and partitions can be simulated without real timing.

## Phase 5: Membership and operations

Deliver:

- Learner add, catch-up, and promotion
- One-at-a-time membership changes
- Strict quorum-safety validation
- Authenticated client and mutual-TLS peer traffic
- Key-range RBAC
- Metrics, health, traces, alerts, and runbooks
- Online compaction and controlled defragmentation

Exit criteria:

- Adding and removing members cannot silently destroy quorum.
- Unauthorized identities cannot access another namespace.
- Operators can diagnose leader, disk, lag, quota, and watch problems.

## Phase 6: Kura integration

**Status:** Started. The in-process C++23 helper, fenced writer publication,
leased reader registration, atomic adapter-local collection check, and
compaction resynchronization are implemented. Remote transport and real
leader-failure coverage depend on Phases 3 and 4.

Deliver:

- Versioned `/kura/v1/` namespace
- Writer guards with lease and fencing checks
- Atomic snapshot publication helper
- Reader registration and cleanup protection
- Watch-based cache invalidation
- Kura end-to-end compatibility tests

Implemented in this slice:

- Move-only writer and reader guards with automatic keepalive and cleanup
- Single-transaction fenced snapshot publication
- Reader-protected snapshot metadata collection
- Retained watch replay and compacted-watch full resynchronization
- Deterministic post-commit uncertain-response fault testing

Exit criteria:

- All acceptance scenarios in `kura-integration.md` pass under injected leader
  failure, network delay, retries, and process pauses.

## Phase 7: Production maturity

Deliver:

- Rolling protocol and storage upgrades
- Disaster-recovery exercises
- Automated snapshots and restore verification
- Multi-architecture artifacts
- Container and orchestration examples
- Sustained performance and chaos suites

No phase is complete solely because its normal path works.
