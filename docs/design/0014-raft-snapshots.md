# Design 0014: Raft snapshots and InstallSnapshot

## Status and scope

Accepted as slice 5 of issue #6 and the implementation of issue #15. This
slice adds deterministic snapshot creation, prefix compaction, whole-snapshot
InstallSnapshot, lagging-follower catch-up, and crash recovery to the existing
election, AppendEntries, commit/apply, and ReadIndex core.

It does not add membership changes, a production peer transport, incremental
snapshot streaming, or a distributed service. Membership in a snapshot is
validated and preserved, not changed by installation.

## Problem and why

An indefinitely growing Raft log is not operationally viable. Once the state
machine represents an applied prefix, a canonical durable snapshot can replace
that prefix. Deleting the log first would lose the only durable representation
of committed state. A follower behind a compacted prefix also cannot be repaired
with AppendEntries alone: the leader no longer has the commands needed to walk
that follower back to index zero.

Snapshot safety therefore requires two ordered protocols:

1. publish a complete, checksummed snapshot before allowing prefix deletion;
2. install and restore a received snapshot before acknowledging it.

## State and typed effects

`PersistentRaftState` carries optional `SnapshotMetadata` plus the contiguous
log suffix beginning at `lastIncludedIndex + 1`. Figure 2 index/term helpers
treat the snapshot boundary as a real predecessor. RequestVote freshness,
AppendEntries matching, commitment, and apply use absolute indexes.

The core accepts:

- `CreateRaftSnapshot`, only at the current `lastApplied`;
- `ReceiveInstallSnapshot` and `ReceiveInstallSnapshotResponse`;
- `RaftSnapshotPersisted`; and
- `RaftSnapshotRestored`.

It emits ordered:

- `PersistRaftSnapshot`;
- `RestoreStateMachineSnapshot`;
- `TruncateRaftLogPrefix`;
- InstallSnapshot request/response messages; and
- typed creation/rejection outcomes.

The diagnostic snapshot exposes durable snapshot metadata and the remaining
suffix. Persistence and restore each block incompatible inputs.

## Canonical creation and durable storage

Creation derives the last-included term from the executable log/snapshot
boundary. It requires a nonzero applied index, nonnegative revisions,
`compactionRevision <= storeRevision`, the configured voter set, unique
nonzero voter/learner IDs, and the configured byte limit. Membership is sorted
before publication for reproducible bytes.

`RaftSnapshot` is the consensus-core value. A storage driver maps it directly
to the existing `storage::Snapshot`, calls `SnapshotStore::publish`, and sends
the matching completion only after atomic publication. The core emits
`TruncateRaftLogPrefix` only after that completion. The WAL driver then calls
`WriteAheadLog::truncate_through(index, snapshotStore)`, whose existing gate
independently verifies durable coverage.

The durable file remains
[snapshot-v1](../formats/snapshot-v1.md); this slice introduces no production
network encoding.

## InstallSnapshot transfer and integrity

The first implementation transfers one complete snapshot but retains explicit
stream semantics: nonzero transfer ID, offset zero, declared total size,
`done=true`, metadata, state bytes, and CRC32C of the state bytes. Nonzero
offsets, partial transfers, impossible sizes, corrupt state, invalid revisions,
foreign/duplicate membership, stale terms, and stale indexes fail closed.

The simulator's binary codec is private test infrastructure. A future
production transport may chunk the same typed contract, but partial assembly
must add its own bounded staging storage and checksum before success.

An identical already-installed snapshot is idempotently acknowledged. A stale
or conflicting snapshot is rejected. A newly published snapshot first emits a
restore effect. Only matching `RaftSnapshotRestored` permits prefix truncation
and a successful response. Thus delayed publication or restore cannot fabricate
success.

## Leader fallback and resumption

Leader progress remains absolute. When a peer's `nextIndex` reaches or crosses
the durable snapshot boundary, an otherwise generated AppendEntries effect is
replaced by InstallSnapshot. At most one current transfer identity is tracked
per peer. Stale, duplicate, wrong-term, or wrong-index acknowledgements cannot
advance progress.

A matching success sets that peer's `matchIndex` to the last-included index and
`nextIndex` to the following index. The leader immediately runs the normal
AppendEntries path, resuming replication of the retained suffix.

ReadIndex context is carried through a snapshot fallback. Only the exact
current transfer can acknowledge that peer. A higher-term snapshot request or
response performs the normal durable term transition, steps down, and fails
all pending reads before further processing.

## Crash and restart boundary

Before snapshot publication completes, restart uses the previous snapshot/log
and no success exists. After publication but before live restore completes,
restart loads the newly durable snapshot and restores its state bytes before
starting the core. A published snapshot is therefore always a valid recovery
point.

Recovery sets `commitIndex` and `lastApplied` to at least the snapshot index,
then retains a suffix only if its boundary term matches. This prevents replay
of state already represented by the snapshot and prevents a conflicting suffix
from surviving installation. Interrupted temporary snapshot files remain
ignored by `FileSnapshotStore`.

## Ordering summary

Local creation:

```text
CreateRaftSnapshot
  -> PersistRaftSnapshot
  -> RaftSnapshotPersisted
  -> TruncateRaftLogPrefix
```

Follower installation:

```text
InstallSnapshot(higher term)
  -> PersistRaftHardState -> completion
  -> PersistRaftSnapshot -> completion
  -> RestoreStateMachineSnapshot -> completion
  -> TruncateRaftLogPrefix
  -> successful InstallSnapshotResponse
```

Every list is deterministic. A crash at any arrow restarts from the last
completed durable representation.

## Alternatives

- **Retain the complete in-memory log:** rejected because it hides compaction
  indexing bugs and does not model restart from snapshot plus suffix.
- **Delete WAL before publication:** rejected because a crash could lose the
  only representation of committed state.
- **Acknowledge after receiving bytes:** rejected because neither bytes nor
  restored state would yet be durable.
- **Accept any success for the current snapshot index:** rejected because a
  delayed response from an older transfer could advance peer progress.
- **Implement chunk staging now:** deferred; bounded whole transfer satisfies
  the current slice without inventing a second durable temporary format.
- **Change membership during install:** rejected; membership changes require
  their own joint-consensus reviewable slice.

## Edge cases and limits

- Snapshot indexes and terms are nonzero and cannot exceed current durable
  term/index facts.
- The snapshot boundary itself supplies `previousLogTerm` for the first suffix
  AppendEntries.
- Duplicate AppendEntries after installation does not rewrite the suffix.
- An installed index never regresses commit or applied state.
- Snapshot byte and membership arithmetic is overflow checked before copying.
- A local snapshot cannot advance beyond `lastApplied`.
- One pending publication/restore and one transfer identity per peer bound
  live protocol state.
- No response is emitted while hard-state, snapshot publication, or restore
  completion is outstanding.

## Validation

Deterministic tests cover local creation, membership preservation, real
SnapshotStore/WAL truncation gates, delayed hard-state/publication/restore,
crash before and after publication, corrupt/out-of-order/stale/duplicate
installs, post-install AppendEntries, stale and duplicate acknowledgements,
higher-term stepdown, ReadIndex cancellation, suffix resumption, and lagging
follower catch-up/restart in three- and five-node simulator schedules.
