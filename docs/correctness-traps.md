# Correctness Traps

## Calling Phase 1 distributed

An in-memory state machine has no replication or failover. Documentation and
release notes must identify exactly which phase supplies each guarantee.

## Applying an uncommitted Raft entry

Only entries at or below the commit index may reach the state machine. Applying
a leader's uncommitted proposal can expose a value that disappears after a
leader change.

## Acknowledging before WAL synchronization

A follower that acknowledges an entry it has not durably stored can lose the
entry on restart while the leader counted it toward quorum.

## Serving a stale leader read

Leadership belief is not proof of current leadership. Linearizable reads require
ReadIndex quorum confirmation and waiting until the corresponding index is
applied.

## Applying one entry twice

Crashing between state mutation and persisted apply position can repeat a
command on recovery. The backend update and applied index must commit
atomically, or replay must be provably idempotent.

## Treating a lease as a lock

A paused client may continue after its lease expires and another client takes
ownership. Protected mutations require server-side lease and fencing checks in
the same transaction.

## Generating watch events on a side path

Watch events generated outside ordered state-machine apply can be lost,
duplicated, or reordered. Events belong to the same atomic revision as the
mutation.

## Ignoring watch compaction

A reconnecting watcher may request history the server has removed. It must
perform a full current read, record that revision, and establish a new watch.

## Restoring a lower revision

Clients remember revisions. Restoring an older snapshot without a safe revision
bump can make new events appear older than previously observed events.

## Changing multiple members at once

Unsafe membership transitions can create non-overlapping quorums or increase
quorum before new nodes are ready. Add learners and promote one member at a
time.

## Depending on unbounded clocks

Lease-based leader reads require a proven clock-drift bound. ReadIndex is the
default until that proof and operational enforcement exist.

## Hiding an uncertain write

If a connection drops after proposal, the client cannot know whether the write
committed. Return an uncertainty-aware error and use replicated idempotency
tokens for safe retry.
