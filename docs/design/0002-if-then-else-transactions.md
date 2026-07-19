# Design 0002: In-memory If/Then/Else Transactions

- **Status:** Implemented
- **Issue:** [#2](https://github.com/himanshudhawale/kura-metadata-store/issues/2)

## Problem

Kura snapshot publication updates several keys, such as a snapshot descriptor
and its current pointer. A sequence of independent compare-and-set operations
can expose a partially published snapshot or let two publishers both believe
they won. Kura needs one atomic decision against one state and one atomic
publication of the selected changes.

This design covers only the Phase 2 in-memory transaction slice. It does not
add historical MVCC, watches, leases, persistence, request deduplication, or
Raft.

## Chosen API and semantics

A transaction contains comparisons and success and failure operation lists.
While holding the store's exclusive lock, the store:

1. validates and evaluates every comparison against the same pre-transaction
   state;
2. selects exactly one branch: success when all comparisons match, otherwise
   failure;
3. validates the complete selected branch before changing state;
4. executes that branch atomically; and
5. returns one typed result for every selected operation, in request order.

The non-selected branch is neither validated nor executed. This permits a
caller to place condition-dependent operations in either branch without an
irrelevant branch causing failure. Comparisons themselves are always
validated.

`RangeRequest`, `PutRequest`, and `DeleteRequest` are supported. Transactions
cannot be nested because `RequestOperation` has no transaction alternative.
Ranges are non-empty half-open intervals `[start, end)`. A range revision of
zero means current state; nonzero historical reads are rejected because this
slice has no MVCC. A zero limit means unlimited. Nonzero lease IDs are rejected
until leases exist.

The response is a variant of range, put, and delete-range results rather than
an opaque byte vector. Range values and deleted previous values are in unsigned
lexicographic key order. Optional previous values are populated only when the
corresponding request asks for them.

## Comparisons

The targets are version, create revision, modification revision, value, and
lease ID. Numeric targets require an `int64` expected value. Value requires a
`ByteSequence`. Equal, not-equal, greater, and less use signed integer ordering
for numeric targets and unsigned lexicographic byte ordering for values.

An absent key has version, create revision, modification revision, and lease ID
zero and an empty value. This makes an equality comparison with revision or
version zero an explicit absence test. All comparisons are conjunctive and
short-circuiting does not change their common pre-transaction snapshot.

## Invariants

- Exactly one branch is selected.
- Every comparison observes the state before any selected operation.
- No selected branch may write the same possible key twice. Duplicate puts,
  overlapping delete ranges, and put/delete overlap are invalid, including
  conflicts whose deletes would currently be no-ops.
- The entire selected branch is validated before mutation.
- Any exception leaves key state and store revision unchanged.
- A transaction with effective mutations advances the store revision exactly
  once; every created, updated, or deleted key uses that revision.
- A branch containing only reads and/or deletes that find no keys does not
  advance the revision.
- Existing-key puts increment version and preserve create revision. New or
  recreated keys start at version one and use the transaction revision for
  both create and modification revisions.

## Read and write visibility

Selected operations execute in request order on a private working state.
Ranges see writes from earlier operations in the same branch and do not see
later writes. Results nevertheless report the transaction's single final
revision. Duplicate-write validation prevents order-dependent second writes to
one key. Other threads see either the complete old state or complete new state,
never the working state.

## Revision allocation and overflow

The store first determines whether at least one selected write is effective.
Only then does it check and allocate `store revision + 1`. The candidate
revision is assigned to every effective mutation and published once with the
new state. Read-only and entirely no-op branches retain the old revision.

All protocol counters are signed 64-bit integers. A mutating transaction at
`INT64_MAX` fails before mutation. Updating a key at maximum version likewise
fails before mutation. Comparisons perform no subtraction, avoiding signed
overflow.

## Atomic failure and concurrency

The exclusive store lock covers comparison, branch selection, validation,
working-state construction, and publication. Potentially throwing allocation
and result construction happen against a private copy. Publication uses a
non-throwing map swap only after every operation succeeds. Invalid input,
duplicate writes, allocation failure, and counter overflow therefore cannot
publish a partial branch.

Concurrent Kura-style publishers comparing the same pointer revision are
serialized at this boundary. The first can publish its complete multi-key
branch; the second then compares against the new state and selects its failure
branch.

## Alternatives rejected

### Lock each operation separately

This permits another thread to change comparison inputs or observe partial
publication between operations.

### Validate while mutating

A late invalid range, duplicate key, or overflow could leave earlier writes
visible. Full selected-branch validation is required.

### Reserve one revision per operation

That loses the atomic-batch identity needed by future watches and Kura readers.

### Make all reads observe the pre-transaction state

That is simple but prevents a selected branch from reading a value it just
created. Ordered visibility is more useful and remains deterministic.

### Validate both branches

The unselected branch has no effect. Rejecting a transaction because that
branch is invalid conflicts with selected-branch execution and complicates
condition-dependent requests.

## Complexity

With `n` stored keys, `c` comparisons, and `o` selected operations:

- comparison and point writes cost `O(log n)` each;
- a range read/delete costs `O(log n + k)` for `k` visited keys;
- duplicate-range validation costs `O(o^2)`, bounded by transaction limits;
- the private state copy costs `O(n)` time and memory.

The full copy deliberately favors strong exception safety and clarity in this
initial in-memory implementation. A later MVCC backend can use a write batch
while preserving these semantics.

## Compatibility

Existing point get, range, put, erase, and modification-revision CAS behavior
is unchanged. Transaction calls and typed result structures are additive for
store callers; custom `MetadataStore` implementations must implement the new
virtual transaction method. Scaffold transaction request types become
executable only in the in-memory store. Lease ID remains zero-only, and no
distributed-linearizability or durability claim is introduced.

## Test strategy

Focused tests cover:

- every target with equal, not-equal, greater, and less outcomes;
- success and failure branch selection;
- multi-key writes sharing one revision;
- ordered read visibility and typed results;
- absent range deletes and read-only revision stability;
- duplicate put, delete overlap, and put/delete rejection;
- invalid selected-branch atomicity and ignored invalid unselected branches;
- store-revision and per-key-version overflow;
- preservation of version/create/modification revision semantics; and
- two concurrent Kura-style publishers producing exactly one winner without a
  partial snapshot publication.
