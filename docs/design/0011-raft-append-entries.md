# Design 0011: Deterministic Raft AppendEntries replication

## Status and scope

Accepted as slice 2 of issue #6 and the implementation of issue #12. This
slice extends the election core with leader/follower AppendEntries, heartbeat
timers, local typed log proposals, follower log persistence, and deterministic
simulator transport/recovery.

It deliberately does not advance `commitIndex`, apply commands, complete client
requests, install snapshots, change membership, or provide a production peer
transport. Those guarantees require later slices, beginning with issue #13.

## Problem and why

Election alone cannot make another node's log match the leader. Replication
must reject an incorrect prefix, delete only a conflicting suffix, retry from
an earlier index, and never tell a leader that changed entries are present
before they are durable. A crash between in-memory append and disk completion
must recover the old log and must not leave behind a success response.

The implementation continues to use typed, synchronous event transitions.
The environment owns message delivery, logical time, and persistence
completion, so duplicates, reordering, delay, and crashes are reproducible.

## State, inputs, and effects

The existing `raft::election::Core` is extended in place to preserve the slice
1 API. It still owns the executable `figure2::State`, now exposing the log and
leader peer progress in its diagnostic snapshot.

New inputs are:

- `ReceiveAppendEntries` and `ReceiveAppendEntriesResponse`;
- `ReceiveClientCommand`, which creates one typed current-term `LogEntry`;
- `HeartbeatDeadline`; and
- `RaftLogPersisted`, correlated by a nonzero request ID and complete log.

New effects are:

- `PersistRaftLog`, containing the complete intended durable log;
- typed AppendEntries requests and responses; and
- heartbeat deadline reset/cancel operations.

Effects retain deterministic vector order. Role changes and timer operations
remain explicit. The core has at most one outstanding hard-state or log
persistence request and rejects unrelated direct input while one is pending.
The simulator adapter queues such inputs in arrival order.

## Leader behavior

On election, Figure 2 initializes each peer to:

```text
nextIndex  = local last index + 1
matchIndex = 0
```

The leader immediately sends AppendEntries and starts a fixed logical
heartbeat interval. An empty suffix is a heartbeat. A local proposal appends
an entry with the leader's current term and next contiguous index, emits
`PersistRaftLog`, and withholds all replication carrying that entry until the
matching completion.

A failed current-term response decrements that peer's `nextIndex`, never below
one, and sends the suffix beginning there. Repeated failures therefore
backtrack deterministically. A success raises `matchIndex` monotonically and
sets `nextIndex=matchIndex+1`. Duplicate or older successes cannot reduce
progress.

Any higher-term request or response uses the existing hard-state gate, clears
leader state, cancels the heartbeat, becomes a follower, and persists the new
term before dependent output.

## Follower consistency and conflicts

The follower first rejects a stale leader. For a current or durably adopted
term, it requires both `previousLogIndex` and `previousLogTerm` to match. Index
zero has term zero. A missing previous index or wrong previous term returns
failure without changing the log.

After a matching prefix:

1. entries must have contiguous indexes beginning at `previousLogIndex+1`;
2. existing entries with equal index and term are retained;
3. at the first different term, the complete local suffix is deleted; and
4. all remaining incoming entries are appended in order.

If this changes the log, success is deferred behind `PersistRaftLog`. If it
does not change the log, as for a duplicate request or heartbeat, success is
immediate because the matching prefix/suffix is already durable. A request
that arrives out of order beyond the local suffix fails and cannot create a
gap.

`leaderCommit` remains encoded for protocol compatibility, but this slice
restores the prior `commitIndex` after every Figure 2 transition and removes
commit-advance rule observations. No command is applied.

## Persistence and crash ordering

Hard state and log use separate explicit completion events. A higher-term
AppendEntries that changes the log can therefore require:

1. persist the new term;
2. receive `RaftHardStatePersisted`;
3. persist the new complete log;
4. receive `RaftLogPersisted`; and
5. emit successful AppendEntriesResponse.

This conservative sequence has a valid restart state at either crash point.
The simulator persists tagged hard-state or complete-log records. Restart
replays only completed records, validates the resulting contiguous log against
the recovered current term, and starts as a follower. An incomplete log record
is absent after restart.

The simulator codec is private test infrastructure, not a network or
production disk format. The filesystem hard-state store remains unchanged;
production log storage will implement the same typed completion boundary.

## Deterministic simulator integration

The adapter now encodes and decodes all RequestVote and AppendEntries messages,
entries, commands, hard state, and log snapshots with strict length/tag
validation. Timer IDs distinguish election and heartbeat deadlines.

`commands_on_leadership` is an explicit deterministic simulator input used to
drive replication schedules without adding a client transport. Each adapter
submits the configured sequence only after it becomes leader and pauses at
each persistence boundary.

## Alternatives

- **A success response in the same step as an in-memory append:** rejected
  because a crash could acknowledge a log that recovery loses.
- **Persisting only appended bytes in the core effect:** rejected for this
  slice because conflict truncation also needs a durable operation; a complete
  intended log makes recovery semantics unambiguous.
- **Blind overwrite from index one:** rejected because a matching prefix must
  be retained and leader retry state would be meaningless.
- **Term-only prefix checks:** rejected because an index is also required to
  identify the preceding entry.
- **Immediate jump-to-conflict hints:** deferred; one-index backtracking is the
  executable Figure 2 baseline and is deterministic and correct.
- **Commit/apply in this slice:** rejected to keep durability, majority
  commitment, and client completion reviewable in issue #13.

## Edge cases

- Empty logs use previous index/term zero and initialize `nextIndex` to one.
- Noncontiguous incoming entries, unknown senders, and sender/leader mismatch
  fail closed.
- A stale leader cannot reset the election timer.
- Duplicate requests do not rewrite an already matching log.
- Out-of-order requests cannot create holes or truncate on prefix mismatch.
- Retry never decrements `nextIndex` below one.
- Log and timer request IDs throw on exhaustion rather than wrapping.
- A leader stepping down cannot retain leader progress or a heartbeat timer.
- Log entry terms above recovered `currentTerm` and noncontiguous recovered
  indexes are rejected.

## Validation

`kura_raft_append_entries_tests` covers:

- empty-log initialization and recurring heartbeats;
- leader persistence before sending a local entry;
- divergent suffix truncation and replacement;
- malformed term/index sequence rejection;
- duplicate and out-of-order follower requests;
- stale and higher-term leader handling;
- two-stage hard-state/log completion ordering;
- leader retry/backtracking and progress updates;
- lagging divergent-follower convergence under reordered requests;
- explicit absence of commit advancement;
- higher-term leader step-down;
- crash before follower log completion; and
- complete one-entry replication schedules on three- and five-node clusters.

All earlier election, Figure 2, simulator, storage, and state-machine tests
remain in the same CTest suite.
