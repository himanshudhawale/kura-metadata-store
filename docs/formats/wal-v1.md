# Kura segmented WAL format, version 1

All integers are unsigned little-endian unless stated otherwise. Sizes and
offsets are bytes. CRC32C uses the Castagnoli polynomial, reflected polynomial
`0x82f63b78`, initial value `0xffffffff`, and final XOR `0xffffffff`.

## Segment naming and header

Final segments are named `wal-%020llu.kwal`. Sequence numbers are positive and
contiguous among retained segments.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic ASCII `KURAWAL1` |
| 8 | 2 | format version, `1` |
| 10 | 2 | header size, `40` |
| 12 | 4 | flags, `0` |
| 16 | 8 | segment sequence |
| 24 | 8 | first record index |
| 32 | 4 | CRC32C of bytes `[0, 32)` |
| 36 | 4 | reserved, `0` |

The first index is nonzero. It equals the first record index, or the expected
next index when the segment is empty.

## Record

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic ASCII `KREC` |
| 4 | 2 | record format version, `1` |
| 6 | 2 | record type (`1` = opaque command) |
| 8 | 4 | header size, `40` |
| 12 | 8 | Raft term |
| 20 | 8 | Raft log index |
| 28 | 8 | payload length |
| 36 | 4 | CRC32C |
| 40 | payload length | opaque payload |

The checksum covers header bytes `[0, 36)` followed immediately by the payload;
the checksum field itself is excluded. The record index is nonzero. Payload
interpretation is outside this format.

## Reader rules

Unknown versions, types, flags, header sizes, bad checksums, invalid configured
lengths, and ordering violations are errors. The only tolerated torn suffix is
an incomplete record header, or an incomplete payload following a complete
structurally valid header, at EOF of the highest-numbered segment. Segment
headers are never treated as tear-recoverable. No resynchronization scan is
performed.
