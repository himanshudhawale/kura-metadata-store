# Design 0005: Durable WAL and snapshots

## Status and scope

Accepted for the Phase 3 durable-storage foundation. This design introduces a
standalone durable boundary: ordered opaque command bytes enter a segmented
write-ahead log (WAL), and opaque state-machine bytes enter snapshots. It does
not connect persistence to `InMemoryMetadataStore` and is not a transactional
KV backend. Raft consensus, networking, MVCC history, leases, and watches are
outside this change.

The byte formats are specified separately:

- [WAL format v1](../formats/wal-v1.md)
- [snapshot format v1](../formats/snapshot-v1.md)

## Problem and durability contract

The in-memory state machine loses all data at process exit. The first safe
increment is a reusable persistence layer whose acknowledgement boundary is
unambiguous.

`SegmentedWriteAheadLog::append(..., Durability::synchronize)` returns only
after all supplied records have been written and the containing file has
passed the operating-system durability primitive. A returned append therefore
survives process restart and is intended to survive machine restart, subject
to the storage device, filesystem, driver, controller, and power-loss
guarantees actually honoring that primitive. `flush` pushes language/runtime
buffers only (the implementation is unbuffered, so it completes writes but
does not promise stable media); `memory_only` has the same current behavior and
must not be treated as durable. No mutation API is wired to this boundary yet,
so this change does not claim durable KV responses.

A successful snapshot publication means the complete snapshot file was
synchronized, atomically renamed to its final name, and the parent directory
was synchronized where the platform supports it. A previous complete snapshot
is retained. If directory synchronization is unsupported, publication has the
platform's best rename durability and that limitation is not upgraded into a
stronger claim.

## Threat and crash model

Handled:

- process termination at any byte boundary during a record or snapshot write;
- short and interrupted writes;
- crash between file synchronization, rename, and directory synchronization;
- stale unique snapshot temporary files;
- checksum-detectable bit corruption;
- malformed lengths, versions, types, names, ordering, and integer overflow.

Not handled:

- malicious concurrent writers or directory mutation;
- filesystems/devices that acknowledge synchronization without durable media;
- undetected CRC32C collisions;
- arbitrary corruption repaired by guessing or by silently skipping records;
- atomic sector size assumptions;
- disk exhaustion beyond reporting an error.

The WAL and snapshot directories have one process owner. Public payloads are
opaque bytes; interpretation and state-machine compatibility belong to the
caller.

## File and synchronization abstraction

A private move-only RAII file owns exactly one POSIX descriptor or Windows
`HANDLE`. It provides full-write loops, positioned reads, size, truncation, and
synchronization. Destruction closes without throwing.

On POSIX, synchronized append uses `fsync`; publication orders:

1. create a unique file in the destination directory;
2. write all bytes;
3. `fsync(file)`;
4. close;
5. `rename` to a unique final name;
6. `fsync(parent directory)`.

On Windows, the equivalents are `WriteFile`, `FlushFileBuffers`, close, and
`MoveFileExW` without replacement and with `MOVEFILE_WRITE_THROUGH`.
Best-effort parent-directory synchronization opens the directory with
`FILE_FLAG_BACKUP_SEMANTICS` and calls `FlushFileBuffers`; Windows filesystems
may reject directory flushing. Unsupported directory flush is tolerated and
documented, while ordinary file flush failures are errors.

Segment creation uses the same write-header, file-sync, rename, directory-sync
sequence before records are appended. A segment with a final name is never
recreated or overwritten.

## WAL lifecycle and rotation

Segments use monotonically increasing sequence numbers and final names
`wal-<20 decimal digits>.kwal`. A segment starts with the v1 segment header.
Records are framed independently and include version, type, Raft term/index,
payload length, and CRC32C.

Before appending a record, the implementation checks all configured and format
limits without allocation. If adding it would exceed `wal_segment_bytes`, a
new segment is atomically created first. A nonempty record must fit in an empty
segment; otherwise append is rejected. The former segment is sealed by policy:
it is never opened for append again. A batch can durably leave a valid prefix
if a later write fails; the call reports failure, and retry/recovery must use
the recovered last index.

Only the highest-numbered segment is appendable after recovery. Rotation is
serialized within one WAL object. Concurrent processes are forbidden.

## Restore and replay

Construction/recovery enumerates exact final segment names, sorts by sequence,
and requires sequences to be contiguous among retained files. Segment headers,
first-index declarations, record framing, versions, types, limits, and
checksums are validated before payload allocation.

Records form one ordered prefix. The first retained index may be any positive
value because older segments may have been snapshot-truncated. Every later
index must be exactly previous plus one. Regression, duplication, gap, and an
attempt to continue after `UINT64_MAX` are explicit errors. Segment
`first_index` must equal its first record; an empty segment must declare the
next expected index.

Only these suffixes are provably torn and recoverable:

- fewer than a complete record header remain at EOF of the final segment; or
- a structurally valid final record header declares a permitted length but EOF
  occurs before all declared payload bytes.

That suffix is truncated, the segment is synchronized, and replay continues.
A short segment header, any short record in a non-final segment, bad magic,
bad version/type/length, or checksum mismatch is corruption and is rejected.
In particular, a bad final checksum is not guessed to be a torn write.

An empty WAL recovers to an empty vector. Recovery never invents records or
silently skips an interior byte range.

## Snapshot publication and discovery

Snapshots contain Raft last-included term/index, store and compaction
revisions, membership, opaque state bytes, and one CRC32C over the defined
content. Publication validates counts, lengths, revision invariants, and
configured limits before allocation or I/O.

The writer uses `snapshot-<20 digit index>.ksnap.tmp.<unique>` in the snapshot
directory. After file synchronization it renames to
`snapshot-<20 digit index>.ksnap`. Existing final names are never replaced, and
publication requires an index newer than the latest valid snapshot. Thus an
error cannot remove the prior complete snapshot; after an ambiguous
post-rename directory-flush error, both the prior file and possibly the new
complete file remain.

Discovery ignores temporary and unrelated files, examines final snapshots from
highest index down, and returns the first fully valid one. Explicit validation
reports why a named snapshot is invalid. Invalid newer snapshots are not
deleted automatically.

Restore is deliberately split: the caller obtains the latest valid snapshot,
restores its opaque state, then replays WAL entries strictly after the
snapshot's last-included index. This library validates storage ordering but
does not interpret or apply commands.

## Truncation safety

WAL truncation accepts a target index and a `SnapshotStore`. It reloads that
store's durably published latest valid snapshot and refuses unless the
snapshot covers the target. It deletes only complete, sealed segments whose
maximum record index is at or below the target. It never deletes the current
highest segment, never edits a segment prefix, and never uses wildcard
deletion. Specific removals are followed by best-available directory
synchronization. A crash midway leaves extra covered segments, which is safe.

This conservative policy can retain a segment containing both covered and
uncovered records. Repacking is intentionally absent.

## Ownership, limits, and path safety

WAL and snapshot objects are move-disabled directory owners and serialize
their own operations. Callers must not mutate their directories concurrently.
Generated names contain only fixed ASCII prefixes and decimal integers; caller
payloads never influence paths.

Defaults and configurable bounds cover segment bytes, payload bytes, record
count, snapshot bytes, and membership count. Fixed-width additions and
multiplications are checked before conversion to `size_t` or allocation.
Indices must be nonzero and revisions nonnegative; ordering at numeric maxima
is checked without wraparound.

## Corruption policy

Malformed durable data raises `StorageError` with contextual text. The library
does not quarantine, delete, rewrite, or continue past corruption. The only
automatic repair is truncating the narrowly defined torn suffix of the final
WAL segment. Operators retain the files for diagnosis and restore.

## Alternatives

- A database dependency was rejected because this foundation must remain
  dependency-free and no complete transactional backend can be added safely in
  this issue.
- One WAL file makes deletion and bounded recovery awkward.
- Per-record files multiply directory operations and metadata overhead.
- Double-buffered snapshot pointer files add another recovery protocol;
  immutable index-named snapshots already preserve old generations.
- SHA-256 offers stronger adversarial integrity, but implementing cryptography
  here would enlarge the trusted code. CRC32C is standardized, fast, and
  sufficient for accidental corruption detection, not authentication.
- Silently accepting a checksum-bad last record was rejected because it is not
  provably torn.

## Compatibility and versioning

Both formats carry independent version fields and fixed header sizes.
Unknown versions, header sizes, flags, or record types are rejected. Readers
do not reinterpret future data as v1. Format evolution requires a new format
document and either an explicit migration or a reader supporting both
versions. Endianness is always little-endian and independent of host ABI.

## Fault-test strategy

Deterministic tests cover CRC32C vectors; record framing; empty, multi-record,
and multi-segment replay; exact rotation; real-directory reopen; truncated
header/payload; invalid length; checksum damage; interior corruption;
index gaps/regressions and overflow; configured record/payload/snapshot limits;
snapshot round-trip and checksum rejection; stale/partial temporary files;
refused replacement; and truncation without snapshot coverage. Tests construct
byte-boundary faults directly and never depend on sleeps or timing.

