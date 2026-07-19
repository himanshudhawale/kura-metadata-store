# Design 0004: Lease Lifecycle and Fenced Ownership

- **Status:** Implemented
- **Issue:** [#4](https://github.com/himanshudhawale/kura-metadata-store/issues/4)

## Problem and importance

Kura needs ephemeral writer ownership and reader registrations. When a client
stops renewing a lease, every key attached to that lease must disappear as one
atomic change. The result must not depend on a process clock or container
iteration order because the same operation will later be applied by every Raft
replica.

Lease possession is not mutual exclusion. A client can pause for longer than
its TTL, another client can acquire ownership, and the first client can then
resume with stale local state. Every protected mutation therefore needs a
server-side fencing check in the same transaction as the mutation.

This design implements only the single-node, in-memory deterministic state
machine. It does not provide replication, durable snapshots, cross-node
linearizability, or an expiry-driving service.

## Chosen design

### Logical time

The core uses `LeaseTick`, an unsigned 64-bit logical value supplied explicitly
on every lease command and fenced transaction. It never reads a wall clock.
`LeaseDuration` is a positive number of ticks. A lease granted at tick `t` with
TTL `d` is live exactly while `now < t + d`; it is expired at the deadline.

Applied command ticks may stay equal but cannot move backwards. The snapshot
contains the latest applied logical tick. An eventual leader may translate its
timer decisions into ordered commands, but followers will apply only the tick
and lease IDs carried by those commands.

### IDs and fencing tokens

`LeaseId` is a signed 64-bit protocol field because key metadata already uses
that representation. Zero means “no lease.” Valid IDs are
`[1, INT64_MAX - 1]`; zero requests automatic allocation, negative IDs and
`INT64_MAX` are rejected, and allocation exhaustion is explicit.

Every successful grant receives a positive, globally increasing unsigned
64-bit `FencingToken`. The token is never reset when an explicit lease ID is
reused. `UINT64_MAX` is reserved as the exhausted state so increment never
wraps. A pair `(lease ID, fencing token)` identifies one ownership generation.

### State representation

The state machine stores:

- current key values and key revision;
- leases ordered by ID;
- each lease's fencing token, granted TTL, expiry tick, and ordered attached
  key set;
- latest logical tick;
- next automatic lease ID; and
- next fencing token.

`InMemoryStoreSnapshot` represents those fields using fixed-width integers and
ordered vectors. `snapshot()` emits canonical lease-ID and key order, and the
snapshot constructor validates counters, key metadata, duplicate records, and
both sides of the attachment index. This is serialization-ready state, not a
durable snapshot implementation.

### Lifecycle API

- `grant_lease` validates the requested ID, positive TTL, time, and deadline
  arithmetic, then creates a new generation.
- `keep_alive` requires the matching ID and fencing token. A live lease's
  deadline becomes `command tick + original TTL`; keepalive never revives an
  expired lease.
- `time_to_live` returns the record and exact remaining logical ticks for a
  live lease. It distinguishes missing and expired records.
- `revoke_lease` requires the current fencing token and removes the lease and
  every attached key.
- `expire_leases(tick)` removes every lease whose deadline is at or before the
  supplied tick, ordered by lease ID.

Grant and keepalive alter lease state without changing the key revision.
TTL lookup is read-only. A nonempty revoke or expiry batch advances the revision
exactly once, including a lease with no attached keys. Every attached-key
deletion belongs to that one revision. An empty expiry batch and unknown or
fencing-mismatched revoke are no-ops.

### Result and error model

Lifecycle results use `LeaseResultCode`:

- `ok`;
- `not_found`;
- `expired`; and
- `fencing_token_mismatch`.

Expected lifecycle races return these values rather than exceptions. Malformed
IDs, zero TTL, backward ticks, duplicate requested live IDs, and arithmetic or
counter exhaustion throw `std::invalid_argument` or `std::overflow_error`,
matching existing store validation conventions. No broad catches translate
allocation or invariant failures into successful results.

### Ownership comparisons in transactions

`TransactionRequest` carries zero or more `LeaseOwnership` comparisons plus the
logical tick at which to evaluate them. Each comparison checks, against the
same pre-transaction state, that:

1. the lease ID exists;
2. the deadline is after the transaction tick; and
3. the fencing token identifies the current generation.

Lease comparisons are conjunctive with existing key comparisons. A stale,
expired, or unknown generation selects the failure branch as a normal
comparison failure. A `PutRequest` with a nonzero lease ID is valid only when
that lease has a verified ownership comparison in the request. Thus validation,
ownership checking, attachment, and protected writes occur under one exclusive
transaction boundary.

The lease comparison proves authority over the new attachment. Applications
must also compare protected key state when replacing existing ownership. For
example, reattachment can compare `lease_id(key) == oldId` and verify the new
lease generation before putting the key with `newId`.

### Attachment and reattachment

A leased put stores the lease ID in `KeyValue` and inserts the key into the
lease's ordered attachment set. Updating a key:

1. removes its old attachment, if any;
2. applies the normal version/create/modification revision rules; and
3. adds the new attachment, if any.

Deletes and legacy point writes also remove the old attachment. Reattaching a
key from lease A to lease B is one normal put revision. Later expiry of A cannot
delete the key because A's attachment index no longer contains it. Expiry of B
does delete it.

### Deterministic cleanup

The expiry set is ordered by lease ID and deleted keys are globally sorted by
unsigned byte ordering before publication and response construction. Cleanup
is prepared against private copies and published by map swaps. Either every
lease and key in the batch disappears at one revision or the old state remains.
This preserves the transaction implementation's strong exception guarantee.

## Invariants

- No stored key has a nonzero lease ID without exactly one matching attachment.
- Every attachment names an existing key whose `lease_id` names that lease.
- Lease IDs and fencing tokens never wrap or silently reuse a generation.
- A lease cannot be renewed or verified at or after its expiry tick.
- Applied logical time never moves backwards.
- A nonempty expiry/revoke batch consumes exactly one revision.
- Cleanup key and lease order is independent of allocation or request order.
- Reattachment removes old ownership before new ownership is published.
- Any protected leased put has a live, generation-matching comparison in the
  same transaction.

## Edge cases

- A scheduler pause is harmless to determinism: one later `expire_leases(tick)`
  command collects every overdue lease into a sorted batch.
- Keepalive at exactly the deadline returns `expired`.
- A wrong fencing token cannot renew or revoke a reused lease ID.
- Revoke may clean up an existing lease after its deadline if expiry has not
  yet been applied.
- Expiry of an unattached lease still records one lifecycle revision; an empty
  scan does not.
- Revisions, lease IDs, fencing tokens, and expiry addition are checked before
  mutation.
- A leased put without a matching ownership comparison is invalid and leaves
  state unchanged.
- A stale ownership comparison may safely select a read-only or recovery
  failure branch.

## Why a lease is not a lock

The server cannot revoke knowledge already held by a paused process. Suppose
generation 7 pauses, expires, and generation 8 starts work. Generation 7 can
still wake and issue requests. Checking only a remembered lease ID—or checking
nothing because the client once received a grant—would allow concurrent stale
work. The monotonically increasing fencing token lets the state machine reject
generation 7 inside the protected transaction. External resources must also
honor that token or be updated only through fenced metadata transactions.

## Alternatives rejected

### Read `steady_clock` in the state machine

Different replicas would observe different instants and could expire different
leases. Explicit ordered ticks make command application deterministic and
testable.

### Use only a lease ID

An explicit ID can be reused. Without a generation, a delayed keepalive or
write from the old holder is indistinguishable from the new holder.

### Delete each attached key separately

This exposes partial cleanup and assigns several revisions to one lifecycle
event. One private-state batch gives atomic visibility and one revision.

### Scan all key values during every expiry

A canonical scan is deterministic but makes cleanup proportional to the whole
store. The validated reverse attachment index limits work to affected keys and
is included in snapshots.

### Treat keepalive as implicit expiry cleanup

Combining failed renewal and cleanup makes retry outcomes harder to reason
about. A failed keepalive fences immediately by deadline; the explicit expiry
command owns deterministic cascade deletion.

### Let attachment imply ownership

Possession of an ID is forgeable and stale. Requiring the current fencing token
in the transaction makes attachment an authenticated state-machine decision.

## Compatibility

Existing unleased keys, point operations, comparisons, versions, and transaction
revision rules remain unchanged. `lease_id == 0` retains its prior meaning.
Nonzero transaction puts, previously rejected as unimplemented, are now
accepted only with verified ownership. Existing custom `MetadataStore`
implementations must implement the additive lifecycle virtual methods.

The older initial-values constructor still accepts only unleased values. Lease
state restoration uses the validating `InMemoryStoreSnapshot` constructor.
There is still no historical MVCC, watch dependency, request deduplication,
network service, durable storage, or Raft behavior.

## Validation and testing

Targeted tests cover:

- automatic and requested IDs, grant deadlines, TTL lookup, and keepalive;
- backward time, zero TTL, missing leases, and fencing mismatch;
- multi-key revoke cleanup at one deterministic revision;
- a paused owner fenced after expiry and explicit ID reuse;
- multiple overdue leases expiring in one sorted batch after a pause;
- leased-put validation and expired transaction ownership;
- reattachment followed by old and new lease expiry; and
- canonical snapshot round-trip with lease and attachment state.

The complete existing MSVC Release build and CTest suite also verifies that
point key, revision, version, range, CAS, and If/Then/Else semantics remain
unchanged.
