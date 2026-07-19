# Design 0013: Quorum-confirmed linearizable ReadIndex

## Status and scope

Accepted as slice 4 of issue #6 and the implementation of issue #14. This
slice adds a leader-only, quorum-confirmed ReadIndex protocol to the
deterministic election, replication, commit, and apply core.

It does not use clock leases and does not implement snapshot installation,
membership changes, a production transport, or the final client read path.
`ReadIndexResponse` is permission to read state at an applied index, not the
key/value read itself.

## Problem and why

Being the last elected leader is not enough to serve a linearizable read. A
partitioned former leader can remain unaware that a majority elected a newer
term. A safe read must confirm current-term authority with a voting quorum and
must not observe local state before all commands through the selected index
have been applied.

Raft also requires a leader to have committed an entry from its own term
before using this procedure. Without that condition, commitment inherited from
an older term does not establish the leader-completeness facts needed by
ReadIndex.

## Typed contract

Client inputs:

- `ReadIndexRequest{requestId}`;
- `CancelReadIndex{requestId}`; and
- `TimeoutReadIndex{requestId}`.

Client effects:

- `ReadIndexResponse{requestId, committedIndex}`; or
- `ReadIndexRejected{requestId, reason}`.

Rejection distinguishes not-leader, no current-term commit, capacity, duplicate
request, cancellation, timeout, leadership loss, and context exhaustion.
Timeout and leadership loss are explicit uncertainty; neither can produce a
stale success later.

Each accepted request receives a monotonically allocated nonzero
`ReadIndexContext`. AppendEntries request/response types carry an optional
context. Followers echo it on success or failure. A retry after prefix failure
retains the same context until the follower can acknowledge a successful
current-term AppendEntries.

## Preconditions and quorum

The core accepts a new ReadIndex only when:

- the local role is leader;
- `commitIndex` is nonzero;
- `log[commitIndex].term == currentTerm`;
- the request ID has never been accepted in this core generation;
- pending and history limits permit it; and
- a fresh context can be allocated without wrapping.

The request snapshots the current `commitIndex` as its read index, counts the
leader's self vote, and sends a contextual AppendEntries to every peer.
Requests may carry entries for a lagging follower; success remains gated on
that follower's durable log completion.

A response counts only when it is successful, from a configured peer, in the
leader's current term, and carries a still-pending context. Acknowledgements
are stored in a node-ID set. Duplicate responses cannot count twice, different
contexts cannot be combined, failed responses trigger contextual retry, and
stale/replayed contexts are ignored.

Quorum is `(cluster size / 2) + 1`, including the leader. Tests exercise two of
three and three of five.

## Applied-index gate and ordering

Quorum confirmation alone does not complete a read. The pending request
remains until:

```text
lastApplied >= captured read index
```

Application completion checks all confirmed requests in context allocation
order and emits deterministic responses for those now ready. A later commit
does not raise an existing request's captured index; the read can linearize at
its completed quorum confirmation after all earlier committed work is visible.

Multiple requests have separate contexts and acknowledgement sets. Completion,
cancellation, or timeout removes only that request. Accepted request IDs remain
in bounded history, preventing reuse after completion or cancellation.

## Bounds and replay policy

`maxPendingReads` bounds live contexts. `maxReadHistory` bounds accepted
request IDs and must be at least the pending limit. Once bounded history is
full, new requests are explicitly rejected rather than evicting an ID and
allowing replay. The default limits are 128 pending and 4096 accepted IDs.

Contexts are never reused. Allocation fails before unsigned wrap. Unknown,
cancelled, timed-out, completed, and replayed contexts cannot produce another
response.

## Leadership changes and partitions

Any transition from leader to follower rejects every pending request with
`leadership_lost` in deterministic context order. A higher-term request or
response changes role and fails reads immediately; dependent Raft responses
still obey hard-state persistence ordering.

A former leader isolated from a quorum receives too few contextual
acknowledgements and cannot complete. The other partition may elect a new
leader, but the isolated node's lack of term knowledge does not weaken safety:
its read remains pending until cancellation/timeout and never returns success.

## Simulator integration

The private simulator codec now serializes optional contexts in AppendEntries
requests and responses with strict presence/value validation.
`reads_on_leadership` is an explicit deterministic test input. The adapter
submits configured reads only after a current-term entry is committed; core
effects and observer counters expose pending, completed, and rejected reads.

The simulator controls partitions, message delay, duplication, and ordering.
No wall clock or lease duration participates in read safety.

## Alternatives

- **Serve directly from a leader role:** rejected because an isolated former
  leader may still believe it is leader.
- **Leader lease:** deferred because safe leases require explicit clock-drift
  assumptions absent from this deterministic core.
- **Count any heartbeat response:** rejected because acknowledgements must be
  correlated to activity initiated after this read.
- **Reuse one context for concurrent reads:** rejected because cancellation,
  replay, and independent completion become ambiguous.
- **Complete at `commitIndex` before apply:** rejected because the local state
  machine may still expose older data.
- **Evict request history silently:** rejected because an old request could be
  accepted as new; bounded exhaustion is explicit.

## Edge cases

- Zero request IDs fail closed.
- A newly elected leader without a current-term commit rejects reads.
- Failed contextual AppendEntries preserves the context on retry.
- Duplicate acknowledgements from one peer count once.
- A current-term response for an unknown or completed context is harmless.
- Quorum-confirmed reads remain pending during application backpressure.
- Cancellation and timeout remove the context and make later acknowledgements
  stale.
- Leader step-down fails confirmed-but-not-yet-applied reads as well as
  unconfirmed reads.

## Validation

`kura_raft_read_index_tests` covers:

- healthy three- and five-node quorum confirmation;
- the current-term commit and leader-role preconditions;
- delayed local application after quorum;
- duplicate, stale, failed, unknown, and replayed acknowledgements;
- request/context reuse rejection;
- concurrent pending requests, cancellation, timeout, and bounds;
- immediate higher-term leadership-loss failure;
- healthy deterministic simulator schedules on three and five nodes; and
- a partitioned former leader that never completes its pending read.
