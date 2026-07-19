# Design 0008: Raft hard-state persistence

## Status and scope

Accepted as the persistence boundary for Raft `currentTerm` and `votedFor`.
This change does not implement elections or RPC handling. It supplies the
durable operation and explicit event that the deterministic Raft core in
issue #6 can use.

The byte format is specified in
[Raft hard-state format v1](../formats/raft-hard-state-v1.md).

## Problem and why it matters

Raft requires a server to persist its current term and vote before sending any
response that depends on the change. If a granted vote is sent first and the
server crashes before persistence, restart can grant a different candidate a
second vote in the same term. Similarly, acknowledging a higher-term request
before persisting that term can let restart act in an obsolete term.

`currentTerm` and `votedFor` therefore form one indivisible hard-state record.
They must never be updated as independent files or writes.

## Event-driven API

The core emits:

```cpp
PersistRaftHardState{request_id, state}
```

The storage driver calls `RaftHardStateStore::persist` and, only after durable
publication, receives:

```cpp
RaftHardStatePersisted{request_id, state}
```

That completion is a new Raft-core input event, not an implicit callback or an
RPC response. The executable Figure 2 specification retains pending effects,
blocks other core inputs, and emits them only after consuming the matching
completion. Request IDs are nonzero correlation tokens; stale, unsolicited,
and mismatched completions are rejected.

Persistence errors throw `StorageError` (or propagate an injected crash in
tests), produce no completion, and leave the object unusable until reopened.
There is no default state, weak-durability acknowledgement, or
success-shaped error fallback. Reopening resolves an ambiguous failure by
reading the only atomically published record.

An identical state may complete without another write because it is already
the loaded durable state. This does not weaken ordering: the completion still
enters the core explicitly.

## State invariants

The store enforces transitions in addition to the future Raft core:

- `currentTerm` never decreases;
- a node ID is either absent or nonzero;
- bootstrap term zero cannot contain a vote;
- within one term, no recorded vote may be cleared or changed;
- an absent vote may become one candidate vote;
- a higher term may begin with no vote or with its first vote;
- the private file generation is positive and never wraps.

These checks make accidental retry or integration errors fail closed. The file
generation detects external replacement while a store object is live; it is
not part of Raft semantics.

## Durable publication and ordering

The store has one owner and serializes calls. Each non-identical transition is
published as follows:

1. validate the transition, request ID, generation, and configured size limit;
2. encode the complete v1 record and checksum in memory;
3. create a unique temporary file in the destination directory;
4. write the complete record with short-write and interruption handling;
5. synchronize the temporary file;
6. close it;
7. atomically replace `raft-hard-state.krhs`;
8. synchronize the parent directory where supported;
9. update the in-memory state and return the explicit completion event.

POSIX uses `fsync`, `rename`, and parent-directory `fsync`. Windows uses
`FlushFileBuffers`, `ReplaceFileW` for replacement (or `MoveFileExW` for first
publication), write-through flags, and the same best-effort directory flush
policy as the WAL/snapshot implementation. Unsupported Windows/POSIX directory
flushes are documented platform limitations; ordinary failures are errors.

A failure before replacement leaves the old final record. A failure after
replacement may leave either old or new state after a real machine crash, but
no completion was emitted. On process-only failure, reopen sees the new
complete record. The caller must not retry through the failed object.

## Recovery and corruption policy

An absent final file means bootstrap state `(term 0, no vote)`. If a final file
exists, its size must be exactly 48 bytes and within the configured limit.
Magic, version, header size, flags, reserved bytes, generation, and CRC32C are
all checked. A short, long, unsupported, or checksum-bad record is an error.

Temporary files are never candidates for recovery. A corrupt final file is not
replaced by, or silently skipped in favor of, a temporary file. This strict
policy avoids inventing which vote was durable. Operators retain corrupt bytes
for diagnosis and restore.

## Crash and edge cases

- First publication: before atomic publication restart sees bootstrap; after
  publication it sees the complete requested state.
- Existing publication: atomic replacement exposes the old or new complete
  record, never a deliberately accepted partial record.
- Crash after replacement but before completion: restart observes the new vote
  and refuses another candidate in that term, even though no response was sent.
- Repeated request for already durable state: safe explicit completion.
- Same-term vote change/clear, lower term, zero node/request ID, and generation
  overflow: rejected before I/O.
- Disk full, permissions, short write, synchronization, close, replace, and
  directory errors: no completion; reopen required.
- Concurrent processes or external directory mutation: unsupported. A live
  object detects a changed final generation/state on `load`.
- CRC32C detects accidental corruption but is not authentication and cannot
  rule out collisions or dishonest storage hardware.

## Alternatives

- Two files for term and vote were rejected because crashes could expose an
  impossible combination.
- In-place overwrite was rejected because a torn sector could destroy both old
  and new state.
- An append-only journal was rejected for this bounded record because it adds
  torn-tail selection, compaction, and unbounded-recovery concerns.
- Two alternating slots plus a pointer were rejected as a larger recovery
  protocol than same-directory atomic replacement.
- Treating checksum failure as bootstrap was rejected because it can grant a
  second vote.
- Returning an RPC-ready boolean from storage was rejected because it hides
  the required Raft-core input ordering.
- SHA-256 was not selected: CRC32C matches existing durable formats and the
  threat model is accidental corruption, not hostile tampering.

## Validation

Deterministic tests:

- crash after each create, write, file-sync, close, replace, and directory-sync
  boundary;
- truncate at every byte boundary of the v1 record;
- corrupt every byte independently and append trailing bytes;
- verify restart exposes only old or new complete state;
- verify no persistence completion exists on every injected crash;
- verify restart cannot grant a second candidate in a recorded term;
- reject term regression, vote changes/clears, zero IDs, impossible limits,
  stale temporaries, and corrupt-final fallback;
- round-trip and explicit completion ordering on the real platform filesystem.

The tests use no sleeps, random scheduling, or power-loss claims. Build and
sanitizer matrices remain defined by the repository CI strategy.
