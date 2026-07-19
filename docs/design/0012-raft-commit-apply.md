# Design 0012: Raft majority commit and ordered apply

## Status and scope

Accepted as slice 3 of issue #6 and the implementation of issue #13. This
slice extends the deterministic Raft core with majority commitment, follower
commit propagation, one-at-a-time ordered application, explicit application
completion/failure inputs, client completion correlation, and simulator
recovery of represented applied state.

It does not implement ReadIndex, snapshots, membership changes, production
transport, or state-machine snapshot installation.

## Problem and why

Replication is not commitment. A leader in a minority can durably copy an
entry without making it safe to apply. Raft also forbids a leader from directly
committing an entry from an older term merely by counting replicas. Once
committed, every server must apply entries in index order, without passing
`commitIndex`, applying an unpersisted log, or replaying state already
represented after restart.

Application can be slower or fail while elections and replication must
continue. The core therefore treats application as another explicit
event/effect boundary rather than calling a state machine inline.

## Commit rules

The executable Figure 2 transition remains the source of commit behavior.
For a leader, candidate index `N` advances `commitIndex` only when:

- `N > commitIndex`;
- the local entry at `N` is from `currentTerm`; and
- the leader plus peers whose `matchIndex >= N` form
  `(cluster size / 2) + 1`.

The current-term restriction prevents the old-term commitment trap. When a
current-term entry commits, every preceding entry is indirectly committed by
the log-prefix property. Tests exercise quorum two in a three-node cluster and
quorum three in a five-node cluster.

A follower advances only after successful AppendEntries and caps the new value
at:

```text
min(leaderCommit, previousLogIndex + entries.size())
```

This is the last entry established by that request, not the follower's possibly
longer unverified suffix. `commitIndex` never decreases. Conflict repair
rejects any attempt to replace an index at or below the committed prefix.

## Application state and effects

The core exposes:

- `ApplyLogEntry{requestId, entry}`;
- `LogEntryApplied{requestId, index}`;
- `LogEntryApplyFailed{requestId, index}`;
- `RetryApplication`; and
- `ApplicationBackpressured{index}`.

At most one entry is in flight. The selected index is always
`lastApplied + 1` and no greater than `commitIndex`. A matching successful
completion advances `lastApplied` and emits the next entry, if any. Mismatched,
out-of-order, unsolicited, and duplicate completions fail closed.

A definitive failure retains the same entry and emits backpressure. Raft RPC,
term, and heartbeat inputs continue normally while application is blocked.
An explicit retry uses a fresh request ID for the same entry. Thus a commit
batch is deterministically serialized by index rather than exposed as an
unordered vector.

When the local leader still owns the pending proposal, successful application
also emits `CompleteClientCommand{index}` before the next apply effect. Losing
leadership drops leader-local client ownership but does not cancel committed
application work.

## Persistence ordering

Commit indexes are volatile and derived from match progress or `leaderCommit`.
Log bytes are durable before a leader sends them because slice 2 gates local
replication on `RaftLogPersisted`.

For a follower request that both appends and commits:

1. update the in-memory log and commit bound;
2. emit `PersistRaftLog`;
3. receive the matching `RaftLogPersisted`;
4. release AppendEntries success; and
5. emit the first `ApplyLogEntry`.

The core does not even create a pending application request until log
persistence completes, so a fabricated early application completion cannot
advance `lastApplied`.

## Crash and restart

The constructor accepts a recovered applied index representing state already
durably reflected by the state machine. Recovery initializes both
`lastApplied` and the minimum known committed prefix to that index and validates
that it does not exceed the recovered contiguous log.

The simulator maps each apply effect to a durable applied-index record and
returns `LogEntryApplied` only on its persistence completion. Restart loads the
greatest non-regressing completed marker. It does not re-emit an effect for
that represented prefix; later leader commit propagation resumes at the first
unapplied index.

This marker is simulator infrastructure, not a production snapshot format.
The future snapshot/state-machine storage driver must atomically represent the
state change and applied index before returning the same completion input.
An application without completion may be retried after crash because it is not
known to be represented.

## Duplicates, ordering, and leadership changes

- Duplicate/out-of-order replication responses cannot count a peer twice
  because quorum uses one `matchIndex` per peer.
- Duplicate AppendEntries cannot regress commit or create a second apply while
  one is pending.
- A follower never commits beyond the prefix established by that request.
- A leader in a partitioned minority can append locally but cannot advance
  commit or apply.
- Higher-term step-down clears leader progress but retains committed
  application work.
- A new leader cannot directly commit old-term entries; replicating a
  current-term entry safely commits their prefix.

## Alternatives

- **Commit any majority-replicated index:** rejected because old-term entries
  could be incorrectly committed across leadership changes.
- **Use follower last-log index as the commit bound:** rejected because an
  unverified divergent suffix is not established by the heartbeat.
- **Apply an entire vector at once:** rejected because partial failure makes
  exactly ordered completion ambiguous.
- **Block Raft while applying:** rejected because state-machine backpressure
  must not stop heartbeats, elections, or replication.
- **Advance `lastApplied` when emitting an effect:** rejected because failure
  or crash would make unrepresented state appear applied.
- **Replay all committed entries after restart:** rejected because already
  represented non-idempotent commands could execute twice.

## Edge cases

- Recovered applied index zero is bootstrap; a nonzero value requires that
  many recovered log entries.
- Application request IDs occupy a range distinct from hard-state and log
  persistence IDs and fail before exhaustion.
- Application failure and completion must match both request ID and index.
- Retry is accepted only for a blocked entry.
- Application never passes commit, even when commit jumps across many entries.
- A committed-prefix conflict is rejected without log persistence or mutation.
- Client completion is emitted only by the leader that still owns the pending
  proposal.

## Validation

`kura_raft_commit_apply_tests` deterministically covers:

- three- and five-node majority thresholds;
- minority partitions that cannot commit;
- old-term direct-commit rejection and safe indirect commitment;
- follower last-new-entry commit bounds;
- committed-prefix conflict rejection;
- log durability before follower application;
- strictly ordered one-at-a-time application;
- duplicate/out-of-order completion rejection;
- failure, backpressure, retry, and continued heartbeat progress;
- client completion correlation;
- application across leader step-down;
- restart without replay of represented applied state; and
- complete commit/apply schedules on three- and five-node simulations.
