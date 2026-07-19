# Design 0003: Resumable in-process watch streams

- **Status:** Implemented
- **Issue:** [#3](https://github.com/himanshudhawale/kura-metadata-store/issues/3)

## Problem and motivation

Polling current metadata wastes work and leaves an unavoidable interval between
observations. Kura readers need prompt cache invalidation when snapshot
pointers or descriptors change, while coordinators need an ordered signal when
another participant publishes state. A reconnect must not silently lose those
signals.

This design adds the Phase 2 in-process watch boundary over the transactional
in-memory state machine. It does not add network streaming, leases, persistent
MVCC, historical value reads, Raft, or durable delivery.

## API semantics

`InMemoryMetadataStore` creates, polls, requests progress for, and cancels
watches. A watch ID is a caller-owned positive signed 64-bit value and must be
unique among active watches. The store owns all retained requests, events, and
responses; byte sequences are copied through immutable value types.

A request whose range end is empty watches its exact non-empty start key.
Otherwise it watches the non-empty half-open interval `[start, end)`. Filters
may include all mutations, exclude puts, or exclude erases.

`start_revision` is inclusive. Zero means the next revision after watch
creation. A positive revision at or below the compaction boundary fails with
`StatusCode::compacted` and reports that boundary. A revision greater than the
current revision plus one fails with `StatusCode::future_revision`; exactly
current plus one is accepted as the normal live-only position. Arithmetic
checks avoid incrementing `INT64_MAX`.

Creation returns an empty response at the current revision and queues retained
matching batches beginning at the requested revision. Polling is non-blocking:
no response means that the watch remains active but has no delivery ready.
Cancellation returns one final cancelled response and releases the watch
immediately.

An explicit progress request queues an empty response behind everything
already pending for that watch. Its header revision is a bookmark: every
matching event through that revision precedes it. With
`progress_notifications` enabled, an applied revision with no matching event
also queues an empty progress response. No timer or background thread is
introduced in this in-process phase.

## State-machine generation and revision batching

Mutation events are constructed while holding the same exclusive lock used to
compare, validate, and apply state-machine mutations. A put carries current and
optional previous state; an erase carries previous state and no current value.
Point put, effective erase, successful CAS, and effective selected transaction
branches all publish events. Failed CAS, absent erase, rejected transactions,
read-only branches, and operations in the unselected transaction branch do
not.

One retained batch corresponds to one store revision. Every effective mutation
in a transaction is collected in selected-operation order; keys removed by one
range delete are ordered lexicographically. The complete batch is staged with
the new key state and then published under the lock. Watchers therefore never
observe a partially applied transaction. Allocation or validation failure
publishes neither state nor events.

The invariants for one watch are:

- responses and batches are ordered by increasing revision;
- an event is unique because each state-machine mutation enters one batch once;
- all matching events remain deliverable while their revision is retained;
- a transaction revision is delivered as one indivisible response;
- replay and live delivery use the same range and filter rules; and
- progress never overtakes an earlier event.

## History, resume, and compaction

The store retains a bounded deque of revision batches. Once its configured
revision count is exceeded, it discards whole oldest batches and advances
`compact_revision` to the highest discarded revision. Initial state supplied
without event history is treated as compacted through its initial revision. A
zero history limit is valid: live watchers can receive a mutation, but its
batch is immediately compacted for future watch creation.

A client records the header revision of each event or progress response. After
disconnecting it creates a watch at `lastSeen + 1`. If that revision is still
retained, replay is reliable. On a compacted error the client performs a
current range read, rebuilds its cache, and starts at the read revision plus
one. A future error is deterministic and requires the caller to correct its
cursor rather than waiting ambiguously.

This is an explicit event-history contract only. It does not make old values
readable and is not the persistent MVCC compaction planned by issue #17.

## Concurrency, backpressure, and limits

The store lock totally orders writers, event history, watch creation, replay,
progress, cancellation, and polling. No callback runs under that lock and no
watch has its own worker thread.

`StoreLimits` bounds active watchers, retained history revisions, and pending
responses per watcher. Creation fails with `quota_exceeded` if the watcher
limit or replay queue limit would be exceeded. Live delivery never blocks a
writer: if one more atomic batch or progress response would exceed a watch's
pending-response limit, that slow watch is cancelled, its pending queue is
discarded, and one bounded terminal `quota_exceeded` response becomes
pollable. Batches are never split and queues are never unbounded.

Watch IDs and all revisions remain signed 64-bit values. IDs must be positive;
revision validation and next-revision checks preserve overflow safety.

## Ownership and lifetime

The store exclusively owns watch registrations, history, and queues.
Responses returned by value own immutable copies of keys and values, so callers
cannot mutate state-machine or retained-history storage. Destroying the store
destroys every watch. Cancellation and terminal backpressure delivery release
registrations deterministically.

## Alternatives rejected

### Publish after releasing the state-machine lock

Another writer could publish first, reordering revisions, and a transaction
could become visible before its watch batch exists.

### One event per queue item

This permits partial transaction delivery and makes backpressure split an
atomic revision. Whole revision batches are the unit of history and delivery.

### Unbounded queues or blocking writers

Unbounded queues allow a stalled consumer to exhaust memory. Blocking the
ordered apply path lets one watcher stop all metadata mutation. Bounded
cancellation makes overload explicit.

### Drop old pending events and continue

Silent dropping violates reliable delivery and makes a progress bookmark
unsafe. A consumer must receive a terminal error and resume from retained
history or rebuild after compaction.

### Wait on arbitrary future revisions

Without a network deadline or cancellation protocol this can wait forever.
Only the immediately following live revision is accepted.

## Compatibility

Existing reads, point mutations, CAS, and transactions retain their results and
revision behavior. Watch methods and limits are additive to the concrete
in-memory store; the generic metadata-store interface remains synchronous.
The existing watch data structures gain executable semantics, including an
optional current value for erase events and a response status. No distributed
linearizability, durability, or remote streaming claim is added.

## Test strategy

Focused tests cover exact-key and half-open range selection, ordered
multi-revision delivery, one multi-key transaction batch, resume from
`lastSeen + 1`, empty progress bookmarks, filters, cancellation, compacted and
future starts, watcher/history limits, backpressure cancellation, concurrent
writers without duplicates or reordering, immutable event values, and the
absence of events for failed CAS, failed comparison branches, absent deletes,
and rejected transactions.
