# Kura snapshot format, version 1

All integers are little-endian. Revisions are signed two's-complement 64-bit
integers and must be nonnegative. CRC32C parameters match
[WAL v1](wal-v1.md).

Final snapshots are named `snapshot-%020llu.ksnap`, using the last-included
index. Temporary names add `.tmp.<unique>` and are not snapshots.

## Header

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic ASCII `KURASNP1` |
| 8 | 2 | format version, `1` |
| 10 | 2 | header size, `68` |
| 12 | 4 | flags, `0` |
| 16 | 8 | last-included Raft index |
| 24 | 8 | last-included Raft term |
| 32 | 8 | store revision (signed) |
| 40 | 8 | compaction revision (signed) |
| 48 | 4 | voter count |
| 52 | 4 | learner count |
| 56 | 8 | state length |
| 64 | 4 | content CRC32C |

The body immediately follows the header:

1. `voter count` node IDs, each 8 bytes;
2. `learner count` node IDs, each 8 bytes;
3. exactly `state length` opaque state bytes.

The checksum covers header bytes `[0, 64)` followed by the complete body; the
checksum field is excluded. The file must end after the declared body.
Last-included index is nonzero, compaction revision cannot exceed store
revision, membership counts and state length must satisfy configured limits,
and voter/learner node IDs must be nonzero and unique across both lists.

Unknown versions, flags, header sizes, impossible lengths, trailing bytes, and
checksum mismatches are invalid. Readers do not partially restore a snapshot.

The Raft core's `PersistRaftSnapshot` effect maps metadata and state directly
to this format. `SnapshotStore::publish` completion must precede any
snapshot-covered WAL truncation. InstallSnapshot is a typed consensus-core
contract and private simulator encoding, not a new durable or production wire
format.
