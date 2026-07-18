# API and Data Model

## 1. Keys and values

Keys and values are opaque byte sequences. Keys are non-empty and ordered using
unsigned lexicographic byte order.

Ranges are half-open:

```text
[start, end)
```

Prefix helpers calculate the smallest key strictly greater than all values with
the requested prefix.

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

The first implementation provides:

```text
compare modRevision(key) == expected
then put(key, newValue)
```

Expected revision `0` means the key must be absent. A failed comparison does not
modify state or advance the revision.

The full transaction API generalizes this into:

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

No transaction may write the same key twice. All mutations and watch events in
one successful branch share one revision.

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

A lease is a replicated TTL record. Keys attached to an expired or revoked
lease are deleted in one deterministic state-machine operation.

Planned operations:

```text
grantLease(ttl)
keepAlive(leaseId)
timeToLive(leaseId)
revokeLease(leaseId)
```

A client believing it holds a lease is not sufficient for mutual exclusion.
Protected writes must execute a server-side transaction that verifies the live
lease or fencing revision.

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
