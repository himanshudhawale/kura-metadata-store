# Design 0004: Lease lifecycle and fenced ownership

- **Status:** Implemented
- **Issue:** [#4](https://github.com/himanshudhawale/kura-metadata-store/issues/4)

## Problem and motivation

Kura writer ownership, reader protection, and service registration need records
that disappear when their owner stops renewing. Deleting only the lease would
leave stale registrations, while trusting a client's local lease belief would
allow a paused process to resume after ownership has moved.

This design adds the deterministic in-memory lease slice: grant, keepalive, TTL
lookup, revoke, expiry, key attachment, cascade deletion, and transaction
fencing. It does not add background timers, persistence, request
deduplication, networking, or Raft.

## State and API

Each lease stores a positive signed 64-bit ID, its granted TTL, and a logical
deadline. A zero requested ID allocates the next deterministic positive ID;
callers may instead request an unused positive ID. TTLs must be positive.

The concrete store exposes:

```text
grant_lease(request, now)
keep_alive(request, now)
time_to_live(id, now)
revoke_lease(request)
expire_leases(now)
```

Time is explicit rather than read from a follower wall clock. A future leader
or deterministic test scheduler chooses `now` and replicates the resulting
lease command. Followers apply that ordered command without consulting their
own clocks. Keepalive moves the deadline to `now + granted_ttl`; TTL lookup
rounds a positive fractional second upward and returns zero at or after the
deadline.

Grant, keepalive, and TTL lookup do not change the key-value revision because
they publish no key mutation. Lease state remains part of state-machine state
and must be included in future snapshots.

## Attachment and fencing

A selected transaction `PutRequest` may carry a live nonzero lease ID. Unknown
lease IDs fail the complete selected branch with `lease_not_found`; zero means
no lease. Updating a key replaces its prior attachment, so revoking the old
lease cannot remove a key that has moved to another lease.

The existing `CompareTarget::lease_id` executes against the pre-transaction
snapshot. Kura protects publication by comparing both the owner key's lease ID
and its fencing modification revision in the same transaction as the
publication writes. Lease possession alone is not mutual exclusion: a paused
client can remember credentials after its lease has been removed.

## Revoke and expiry

Revoke removes one lease. `expire_leases(now)` selects every lease whose
deadline is at or before the supplied time, ordered by lease ID. It removes all
selected leases and scans keys in unsigned lexicographic order for matching
attachments.

All attached keys are erased in one state-machine revision and one atomic watch
batch. Multiple leases expiring together still consume one revision. If no
selected lease owns a key, lease removal does not manufacture a key-value
revision or watch event. Allocation and revision overflow are checked before
state publication, so failure leaves both key and lease state unchanged.

Expiry is applied explicitly. Merely asking for TTL zero does not mutate state,
and followers never infer expiry independently from local time.

## Limits and errors

`StoreLimits::max_active_leases` bounds lease state. Exhaustion reports
`quota_exceeded`. Invalid IDs or TTLs report `invalid_argument`; lookup,
keepalive, revoke, and transaction attachment of an absent lease report
`lease_not_found`.

## Alternatives rejected

### Follower-local timers

Clock skew and pauses would make replicas delete different keys at different
log positions. Explicit ordered expiry input preserves deterministic apply.

### Lease-to-key pointer sets as the only ownership index

Maintaining a second mutable index complicates transaction rollback and
reattachment. The in-memory phase scans the ordered key map during revoke and
expiry; a durable backend may add a transactional index without changing
semantics.

### One revision per expired lease

Simultaneous due leases would become sensitive to scheduler iteration and
expose partial cleanup. One command, revision, and watch batch is deterministic
and atomic.

## Edge cases and validation

Tests cover automatic and requested IDs, TTL observation, keepalive, unknown
lease attachment, transaction fencing, reattachment, revoke, multiple attached
keys, exact-deadline expiry, one-revision cascade deletion, watch batching,
post-expiry renewal failure, and active-lease limits.
