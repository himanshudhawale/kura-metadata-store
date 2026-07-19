# Kura Raft hard-state format, version 1

The file is named `raft-hard-state.krhs`. Writers use unique sibling temporary
files named `raft-hard-state.krhs.tmp.<unique>` and atomically replace the
final file only after synchronizing the complete temporary file.

All integers are unsigned little-endian. Sizes and offsets are bytes. CRC32C
uses the Castagnoli polynomial, reflected polynomial `0x82f63b78`, initial
value `0xffffffff`, and final XOR `0xffffffff`.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic ASCII `KURAHST1` |
| 8 | 2 | format version, `1` |
| 10 | 2 | record size, `48` |
| 12 | 4 | flags, `0` |
| 16 | 8 | storage generation |
| 24 | 8 | Raft `currentTerm` |
| 32 | 8 | Raft `votedFor`; `0` means absent |
| 40 | 4 | CRC32C of bytes `[0, 40)` |
| 44 | 4 | reserved, `0` |

Generation starts at one and increments for each changed state. It is local
storage metadata, not a Raft term or protocol value. Node ID zero is reserved,
so it unambiguously encodes no vote. Term zero is bootstrap-only and must have
no vote.

## Reader rules

An absent final file is bootstrap term zero with no vote. An existing file must
be exactly 48 bytes and no larger than the configured hard-state limit.
Readers reject unknown versions, sizes, flags, nonzero reserved bytes, zero
generation, bad magic, checksum mismatch, truncation, and trailing bytes.

Temporary files are ignored. Readers never fall back from a malformed final
file to bootstrap state, a temporary file, or a previous value.

## Publication rules

The complete temporary file is written, synchronized, and closed before atomic
same-directory replacement. The parent directory is then synchronized where
the platform supports it. A persistence completion event may be created only
after those steps finish successfully.
