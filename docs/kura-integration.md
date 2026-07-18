# Kura Engine Integration

## 1. Responsibility boundary

Kura Engine stores immutable table segments and manifests in object storage.
Kura Metadata Store holds only small authoritative control-plane records:

- Current table snapshot pointer
- Snapshot metadata and lifecycle state
- Writer ownership
- Active reader registrations
- Schema and catalog version
- Compute-node invalidation events

Large manifests and table data do not belong in the metadata store.

## 2. Namespace

Keys use stable object identifiers rather than display names:

```text
/kura/v1/catalogs/<catalogId>/tables/<tableId>/current
/kura/v1/catalogs/<catalogId>/tables/<tableId>/snapshots/<snapshotId>
/kura/v1/catalogs/<catalogId>/tables/<tableId>/writer
/kura/v1/catalogs/<catalogId>/tables/<tableId>/readers/<readerId>
/kura/v1/catalogs/<catalogId>/schemas/<schemaId>
```

The namespace version allows future migration without interpreting old keys
under new semantics.

## 3. Atomic snapshot publication

A writer first uploads and validates immutable data and a manifest in object
storage. It then submits one metadata transaction:

```text
IF
  modRevision(currentKey) == expectedRevision
  AND writerKey is attached to my live lease
THEN
  put(currentKey, newSnapshotPointer)
  put(snapshotMetadataKey, newSnapshotMetadata)
ELSE
  return conflict
```

The pointer contains immutable object identity, integrity hash, schema ID, and
snapshot ID. Publishing metadata before object validation is prohibited.

Two writers based on the same old snapshot cannot both win. The loser reloads
current state and either rebases or reports a conflict.

## 4. Fenced writer ownership

A lease alone cannot protect publication because a paused writer may resume
after its lease expires. Every publication verifies ownership inside the same
transaction. The winning lock generation or modification revision acts as a
fencing token.

Kura must reject stale writers even if they still hold local credentials or
remember an old lease.

## 5. Reader protection

A query:

1. Reads the current pointer using a linearizable read.
2. Grants a short lease.
3. Registers `/readers/<readerId>` containing the selected snapshot and fencing
   revision, attached to that lease.
4. Keeps the lease alive while reading.
5. Deletes the registration and revokes the lease when complete.

Garbage collection reads active registrations at a known revision. It may
delete a snapshot only after no protected reader references it and a configured
safety interval has elapsed.

A reader that loses its lease must not assume protection continues. It either
re-registers before accessing more objects or safely fails the query.

## 6. Cache invalidation

Compute nodes watch each relevant `/current` key:

```text
startRevision = lastObservedRevision + 1
progressNotifications = true
```

On update, a node loads the new immutable pointer and invalidates only cache
entries whose identity changed. If watch history was compacted, it performs a
current range read and resumes from that response revision.

Watch delivery improves freshness; correctness still depends on a linearizable
read at operations that require the latest pointer.

## 7. Availability behavior

When metadata quorum is unavailable:

- New snapshot publication stops.
- New linearizable catalog reads fail or wait until their deadline.
- Queries already holding a pinned immutable snapshot may continue only while
  reader protection remains valid.
- Kura must not invent a current pointer from stale cache.

This favors metadata correctness over write availability during partitions.

## 8. Required helper client

A future Java helper hides unsafe composition:

```text
acquireWriter(tableId, ttl) -> WriterGuard
publishSnapshot(guard, expectedRevision, pointer) -> PublishResult
registerReader(tableId, snapshot, ttl) -> ReaderGuard
awaitSnapshotChange(tableId, fromRevision) -> SnapshotPointer
```

Guards own keepalive and cleanup behavior. Publishing always includes the
server-side lease/fencing comparison.

## 9. Compatibility acceptance tests

Before Kura depends on the service:

1. Two writers racing from one base revision produce one winner.
2. A paused writer cannot publish after lease loss.
3. A reader's snapshot is not collected while registration remains live.
4. Watch reconnect delivers every retained update exactly once per watch.
5. Watch compaction triggers full resynchronization.
6. Leader failure during publication returns either committed success on retry
   or an uncommitted result—never two visible pointers.
7. Snapshot pointers remain monotonic after backup restoration.
