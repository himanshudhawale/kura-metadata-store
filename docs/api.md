# API and Data Model

## 1. Keys and values

Keys and values are opaque byte sequences. Keys are non-empty and ordered using
unsigned lexicographic byte order.

Ranges are half-open:

```text
[start, end)
```

`prefix_range_end(prefix)` returns the smallest key strictly greater than every
key with the requested prefix, using unsigned lexicographic byte ordering. The
caller can use the prefix and returned value as `[prefix, end)`.

The helper copies before incrementing and never modifies the supplied
`ByteSequence`. Carry propagates across trailing `0xff` bytes, which are then
removed:

```text
prefix                      end
"foo"                       "fop"
[0x01, 0x80]                [0x01, 0x81]
[0x12, 0xfe, 0xff, 0xff]    [0x12, 0xff]
```

The return type is `std::optional<ByteSequence>`. An empty prefix or a prefix
made entirely of `0xff` bytes has no finite upper bound, so the helper returns
`std::nullopt`; callers must handle that case explicitly rather than passing a
sentinel as a range end.

Design rationale and rejected alternatives are documented in
[Design 0001: Binary prefix range](design/0001-binary-prefix-range.md).

## 2. Key-value metadata

```text
KeyValue {
  key: bytes
  value: bytes
  version: int64
  create_revision: int64
  mod_revision: int64
  lease_id: int64
}
```

Values are immutable at the API boundary. Implementations must copy or safely
own mutable byte arrays.

## 3. Read operations

### Get

Returns the current key and the store revision observed by the read.

An absent key is a normal empty result, not an error.

### Range

Returns keys in unsigned lexicographic order for `[start, end)`, plus one
revision shared by the response. A future historical `revision` parameter
selects a consistent prior snapshot unless it has been compacted.

### Read consistency

- `LINEARIZABLE`: default after Raft exists; requires leader/quorum confirmation.
- `SERIALIZABLE`: reads local applied state and may be stale.

Phase 1 has one process and therefore does not advertise distributed
linearizability.

## 4. Mutation operations

### Put

Creating an absent key:

```text
revision       = previous store revision + 1
version        = 1
createRevision = revision
modRevision    = revision
```

Updating an existing key:

```text
revision       = previous store revision + 1
version        = previous version + 1
createRevision = previous createRevision
modRevision    = revision
```

### Delete

Deleting an existing key advances the revision once. Deleting an absent key is
a successful no-op and does not advance the revision.

Historical storage will record a tombstone. The initial in-memory slice removes
the current value.

### Compare and set

The point helper provides:

```text
compare modRevision(key) == expected
then put(key, newValue)
```

Expected revision `0` means the key must be absent. A failed comparison does not
modify state or advance the revision.

### If/Then/Else transactions

The in-memory transaction API generalizes compare-and-set into:

```text
if all comparisons:
    execute success operations atomically
else:
    execute failure operations atomically
```

Comparison targets:

- Key version
- Create revision
- Modification revision
- Value
- Lease ID

All comparisons are conjunctive and observe one pre-transaction state. Missing
keys compare as version, create revision, modification revision, and lease ID
zero with an empty value. Numeric comparisons use signed integer ordering;
value comparisons use unsigned lexicographic byte ordering. Every target
supports equal, not-equal, greater, and less.

Exactly one branch executes. The selected branch supports `RangeRequest`,
`PutRequest`, and `DeleteRequest`; nested transactions are not representable.
The complete selected branch is validated before any mutation. The unselected
branch is not validated or executed.

No selected branch may write the same possible key twice. This rejects
duplicate puts, overlapping delete ranges, and put/delete overlap even when a
delete would currently find no key.

Selected operations execute in order. A range sees writes from earlier
operations in its branch. Results preserve request order and are typed as
`RangeRead`, `PutResult`, or `DeleteRangeResult`. A zero range limit means
unlimited. Historical range revisions are rejected because historical MVCC is not
implemented. A nonzero put lease ID requires a matching live lease ownership
comparison in the same transaction.

If at least one put or effective delete occurs, the store revision advances
exactly once and every mutation uses it. Read-only branches and branches whose
deletes are all no-ops do not advance revision. Validation and signed 64-bit
counter overflow fail atomically without changing state.

The exclusive in-memory transaction boundary makes concurrent comparisons,
branch execution, and publication atomic within one process. It does not claim
cross-node linearizability, durability, or watch delivery.

Design and edge cases are documented in
[Design 0002: In-memory If/Then/Else Transactions](design/0002-if-then-else-transactions.md).

## 5. Watch

A watch subscribes to a key or range starting at a revision.

Required guarantees:

- Ordered by revision
- No duplicate event within one watch
- No missing event inside retained history
- One atomic event batch per transaction revision
- Resumable from `lastSeenRevision + 1`
- Progress notifications that bookmark delivered history

If the requested history was compacted, the server reports the compaction
revision. The client performs a full current range read and resumes watching
from that response's revision.

## 6. Lease

A lease is currently an in-memory deterministic TTL record. It is not yet
replicated or durable. The implemented operations are:

```text
grantLease(requestedId, ttl, tick)
keepAlive(leaseId, fencingToken, tick)
timeToLive(leaseId, tick)
revokeLease(leaseId, fencingToken, tick)
expireLeases(tick)
```

Time is a caller-supplied unsigned logical tick; deterministic core logic never
reads a wall clock. TTL is positive ticks, expiry occurs at the deadline, and
applied command ticks cannot move backwards. Zero lease ID means unattached.
Safe allocated IDs are positive signed 64-bit values below `INT64_MAX`.

Each grant receives a globally increasing `FencingToken`. Lifecycle lookup
distinguishes `ok`, `not_found`, `expired`, and `fencing_token_mismatch`.
Keepalive resets the original TTL only for the current live generation.

`expireLeases` processes every due lease in lease-ID order. Revoke and expiry
remove all attached keys in unsigned key order and publish one atomic revision
per nonempty lease batch. Reattaching a key removes it from the old lease before
adding it to the new lease, so old-lease expiry cannot delete it.

`TransactionRequest::lease_ownership` contains `(lease ID, fencing token)`
comparisons evaluated at `lease_tick` with ordinary key comparisons. A stale or
expired generation selects the failure branch. Any selected put with a nonzero
lease ID requires verified ownership for that lease.

A client believing it holds a lease is not sufficient for mutual exclusion: a
paused client can resume after expiry and replacement. Protected writes must
verify the live generation in the same server-side transaction, and external
resources must likewise honor the fencing token.

`InMemoryStoreSnapshot` includes lease records, ordered attachments, logical
tick, and allocation counters for later serialization and replicated command
application. It is an in-memory representation, not durable snapshot storage.

Full rationale and edge cases are documented in
[Design 0004: Lease Lifecycle and Fenced Ownership](design/0004-lease-lifecycle-and-fencing.md).

## 7. Request idempotency

Mutating requests may contain `(clientId, sequenceNumber)`. The state machine
stores the original response for a bounded period. Retrying the same identifier
returns that response rather than applying the mutation twice.

Deduplication state must be replicated and included in snapshots.

## 8. Errors

Public errors will distinguish:

- Invalid key, range, or transaction
- Comparison failure as a normal transaction result
- Compacted historical revision
- Future revision not yet available
- Unknown or expired lease
- Not leader, with safe redirect information
- No quorum or deadline exceeded
- Authentication or authorization failure
- Backend quota exhausted
- Corrupt WAL or snapshot

Errors must never turn an uncertain write into a success-shaped response.
