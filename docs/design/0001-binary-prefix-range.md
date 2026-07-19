# Design 0001: Binary Prefix Range

- **Status:** Implemented
- **Issue:** [#1](https://github.com/himanshudhawale/kura-metadata-store/issues/1)
- **Pull request:** [#24](https://github.com/himanshudhawale/kura-metadata-store/pull/24)

## Problem

Kura Metadata Store uses a flat binary key space and half-open ranges:

```text
[start, end)
```

Callers frequently need every key beginning with a prefix. Manually calculating
the exclusive upper bound is easy to get wrong, especially when the prefix ends
in one or more `0xff` bytes.

An incorrect bound can omit metadata, include another tenant's keys, or make a
watch silently miss events.

## Why solve it in the core library?

Prefix scans will be used by transactions, watches, lease cleanup, Kura reader
registrations, and administrative tools. One canonical helper gives every
caller the same unsigned ordering and edge-case behavior.

The helper belongs beside `KeyRange`, not in individual clients, because range
encoding is part of the store's API contract.

## Requirements

The helper must:

1. Use unsigned lexicographic byte ordering.
2. Return the smallest finite key greater than every key sharing the prefix.
3. Propagate carry across trailing `0xff` bytes.
4. Never mutate caller-owned input.
5. Represent the absence of a finite upper bound explicitly.
6. Avoid sentinel values that could be interpreted as real keys.

## Chosen design

```cpp
std::optional<ByteSequence> prefix_range_end(
    const ByteSequence& prefix);
```

The algorithm:

1. Copy the prefix bytes.
2. Scan from right to left.
3. Find the first byte smaller than `0xff`.
4. Increment that byte.
5. Remove every byte after it.
6. Return `std::nullopt` if no incrementable byte exists.

Examples:

```text
prefix                      result
"foo"                       "fop"
[0x01, 0x80]                [0x01, 0x81]
[0x12, 0xfe, 0xff, 0xff]    [0x12, 0xff]
[]                           no finite upper bound
[0xff, 0xff]                no finite upper bound
```

The caller forms `[prefix, result)` when a result exists. An unbounded result
must use a future range API that explicitly supports an absent upper bound.

## Correctness argument

Let `i` be the rightmost prefix position whose value is less than `0xff`.
Incrementing byte `i` makes the result greater than every key that matches the
original prefix through `i`. Removing the suffix produces the smallest such
key; retaining any suffix would create a larger bound.

If no such position exists, every prefix byte is maximal. No finite byte string
can be both the immediate lexicographic successor and an exclusive bound for
all longer keys sharing that prefix. The same is true for the empty prefix,
which represents the entire key space.

## Alternatives rejected

### Append `0xff`

`prefix + 0xff` is not greater than keys beginning with
`prefix + 0xff + ...`, so it can exclude valid matches.

### Increment only the final byte

This fails when the final byte is `0xff` and does not propagate carry.

### Return an empty-byte sentinel

An empty sequence is ambiguous: it can mean the beginning of the key space,
all keys, or an absent bound depending on the API. `std::optional` makes the
unbounded case explicit.

### Mutate the input buffer

Mutation would violate `ByteSequence` ownership guarantees and could change a
key already used by another request.

## Complexity

- Time: `O(n)` in the prefix length
- Additional memory: `O(n)` for the owned result copy

Prefix lengths are bounded by store limits, so both costs are predictable.

## Edge cases

- Empty prefix: `std::nullopt`
- One non-maximal byte: increment it
- One `0xff` byte: `std::nullopt`
- Multiple trailing `0xff` bytes: carry left and truncate
- Embedded zero bytes: handled as ordinary unsigned values
- Non-UTF-8 keys: handled without text conversion

## Compatibility

This adds a helper without changing existing range or key representations.
Callers must not translate `std::nullopt` into an arbitrary byte sentinel.

## Test strategy

Tests cover:

- ASCII prefixes
- Multi-byte binary prefixes
- Carry across trailing `0xff` bytes
- Empty and all-`0xff` prefixes
- Input immutability
- Exact expected byte sequences
