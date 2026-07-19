# Design 0010: Kura Metadata Helper Client

## Problem

Kura Engine publishes immutable snapshots, pins them for readers, and follows
the current pointer. Building those operations from raw transactions, leases,
and watches is unsafe: a stale process can omit fencing, a reader can forget
cleanup, and a compacted watch can silently leave a cache stale.

## Motivation

The helper should make the safe composition the normal API while preserving
the store's current scope. The implementation is C++23 and works against the
in-process state machine. It is not a network client and does not claim
replication, durable request deduplication, or distributed leader failover.

## Design

### Boundary

`KuraMetadataBackend` is the narrow transport boundary used by `KuraClient`.
`InProcessKuraMetadataBackend` serializes calls through one adapter and maps
seconds to caller-visible lease ticks. A future RPC backend must preserve the
same atomic transaction and collection contracts.

The test-only uncertain-response wrapper models a transaction that committed
before a leader-style response failure. It is a deterministic fault fake, not
a Raft integration.

### Namespace and encoding

The client uses:

```text
/kura/v1/catalogs/<catalog>/tables/<table>/current
/kura/v1/catalogs/<catalog>/tables/<table>/snapshots/<snapshot>
/kura/v1/catalogs/<catalog>/tables/<table>/writer
/kura/v1/catalogs/<catalog>/tables/<table>/readers/<client>-<sequence>
```

Identifiers are restricted to `[A-Za-z0-9._-]`. Snapshot pointers use a
versioned, length-prefixed binary encoding containing snapshot ID, manifest
URI, schema ID, and integrity hash. Malformed stored values report corruption.

### Writer lifecycle

`acquire_writer` reads the current pointer revision, grants a lease, and
atomically creates the table's single writer key attached to that lease. A
move-only `WriterGuard` owns the lease generation, periodically keeps it alive,
and best-effort revokes it on `close` or destruction.

`publish_snapshot` executes one transaction that checks:

1. the current key modification revision equals the caller's base revision;
2. the writer key is attached to the guard's lease; and
3. the lease ID and fencing token are live at the backend's current tick.

It then writes the current pointer and snapshot metadata at one revision. Thus
one writer owns a table and one publication can consume an optimistic base
revision. Lease expiry removes the writer key, and a paused generation cannot
publish after resuming.

If a backend reports `not_leader`, `no_quorum`, or `deadline_exceeded` after an
uncertain transaction, the client reads current. An exact pointer match is
reported as `recovered_after_uncertain_response`; otherwise the error remains
uncertain. This is result resolution, not general request deduplication.

### Reader lifecycle and collection

`register_reader` grants a lease and transactionally verifies the exact stored
snapshot pointer before attaching a reader key containing its snapshot ID.
`ReaderGuard` owns keepalive and cleanup as the writer guard does.

`collect_snapshot` removes only non-current snapshot metadata with no live
reader key referencing it. The in-process backend performs current checking,
reader scanning, and conditional deletion under its adapter lock. This
guarantee applies only when all related operations use that adapter. A remote
backend needs an equivalent server-side atomic primitive before this API can
make the same claim. Object-store deletion remains Kura's responsibility.

### Watch resumption

`await_snapshot_change` starts an exact-key watch at
`from_revision + 1`. A retained put returns its decoded pointer and event
revision. A compacted start performs a full current read and sets
`full_resynchronization`; callers must replace cached state rather than apply
an incremental event. A timeout returns no update.

### Guard behavior

Guards are move-only so lease ownership has one cleanup owner. Their worker
keeps the original generation alive at one third of its TTL. Any failed
keepalive marks the guard inactive; publication then fails closed. Destruction
still attempts revoke, while lease expiry is the fallback when cleanup cannot
reach the backend.

## Alternatives

- **Raw store calls in Kura:** rejected because fencing and cleanup omissions
  remain easy and review-dependent.
- **Lease ID without a fencing token:** rejected because a paused process can
  resume after expiry and ID reuse.
- **Watch-only cache recovery:** rejected because compacted history cannot
  reconstruct current state.
- **Background wall-clock expiry inside the state machine:** rejected; the
  deterministic store continues to use explicit logical ticks.
- **Claiming a production failover client now:** rejected because no RPC,
  replicated deduplication, or Raft leader exists yet.
- **Reader scan followed by an ordinary delete:** rejected because a new
  registration could race between the two operations.

## Edge cases

- Empty, invalid, or oversized pointer fields are rejected before mutation.
- Reader registration fails if the snapshot is absent or its content differs.
- Current snapshots are never collected, even without readers.
- Cleanup is idempotent and guard destructors do not throw.
- A failed compare does not advance the revision.
- An uncertain response is considered committed only after exact pointer
  equality; a different or missing pointer does not become success.
- Compacted watches return the revision of the full read, not the compaction
  boundary.
- This slice does not protect object data after a reader guard becomes
  inactive.

## Validation

`kura_client_tests` deterministically covers concurrent writer acquisition,
optimistic publication, paused-writer lease loss, live reader protection,
cleanup, retained watch replay, compacted-watch full resynchronization, exact
snapshot validation, and an injected post-commit leader-style response loss.
The latter verifies that retrying from the consumed base revision leaves one
current-key version.

The existing state-machine and durable-storage suites remain part of CTest.
