# Design 0015: Deterministic Raft acceptance and linearizability checking

## Status and scope

Accepted as the final reviewable slice of issue #6 and the implementation of
issue #19. This slice completes the stated deterministic Raft-core acceptance:
the real node adapter is exercised end to end in three- and five-voter
simulations, and generated metadata histories are checked against a sequential
model.

This is not a production Raft service. There is still no network server, peer
authentication, production RPC encoding, durable embedded state-machine
backend, dynamic membership protocol, operational control plane, or
linearizability claim for external clients.

## Problem and why

Unit tests for individual Raft rules do not prove that the assembled adapter
preserves those rules through persistence completions, faults, restart, reads,
and snapshots. Conversely, a successful simulator schedule is not enough if
the externally observed operation history cannot be explained by one legal
sequential metadata execution.

The final acceptance layer therefore combines:

1. deterministic fault schedules over the real event-driven Raft core; and
2. a sound, bounded finite-history linearizability checker.

## Typed history contract

`testing::linearizability::History` contains separate typed invocation and
completion records. Every invocation has a nonzero unique operation ID,
nonzero client ID, logical invocation time, and one operation:

- `Get{key}`;
- `Put{key, value}`;
- `Erase{key}`; or
- `CompareAndSwap{key, expected, desired}`.

Completions identify the operation, logical completion time, and one outcome:

- `succeeded`, with the operation's typed result;
- `failed`, known not to have changed state;
- `timed_out`, known not to have changed state in this test contract; or
- `indeterminate`, which may or may not have taken effect.

An invocation without a completion is also uncertain. Successful results carry
the response revision. Reads additionally carry an optional value and its
modification revision. Erase and CAS carry their observed boolean outcome.

The checker starts from an empty store at revision zero. Put always advances
the global revision. Effective erase advances it; absent erase does not. CAS
compares the current optional value, reports false without mutation on
mismatch, and advances revision for an effective set/erase. This deliberately
small specification covers the metadata operations used by acceptance tests;
transactions, watches, leases, and historical reads require future model
extensions.

## Strict validation

Malformed histories throw `InvalidHistory` before search. Validation rejects:

- zero or duplicate operation IDs;
- zero client IDs;
- empty keys;
- orphan or duplicate completions;
- completion before invocation;
- success without a matching typed result;
- non-success outcomes carrying fabricated results;
- negative response revisions; and
- impossible read modification revisions.

The `linear-history-v1` text encoding is canonical, deterministic, and parsed
with the same validation. Empty bytes and absent optional values have distinct
tokens. A counterexample can therefore be copied directly into a test and
replayed.

## Search and soundness

For each operation, the checker derives real-time predecessors: a completion
at a strictly earlier logical time than another invocation must linearize
first. Equal logical ticks remain concurrent because the history has no
sub-tick order.

The depth-first search considers eligible operations in operation-ID order and
applies the sequential specification exactly. Successful operations are
required. Failed and timed-out operations are definite no-ops. Indeterminate
and pending mutations branch between omission and one legal execution.

Search states are memoized by remaining operation set plus complete model
state. Dead-state memoization and real-time predecessor masks prune equivalent
orders without accepting an invalid history. If any branch is valid, the
history is linearizable. A violation is reported only after every branch is
definitively rejected.

`CheckerLimits` bounds total operations, explored states, and memo entries.
Reaching any limit returns `inconclusive`; resource exhaustion is never
reported as success or as a violation.

## Counterexamples

For a definitive violation, deterministic deletion minimization repeatedly
removes an operation only when the remaining history is still definitively
non-linearizable. The result is one-operation-minimal under the configured
limits. `Counterexample` includes both the typed reduced history and canonical
replay text.

Inconclusive searches do not emit counterexamples because doing so would imply
a proof the bounded search did not establish.

## End-to-end acceptance

The Raft simulator adapter now has optional typed client-input and effect
observers. They do not alter, acknowledge, or bypass inputs/effects: the input
observer marks actual configured proposal/ReadIndex submission, and the effect
observer records the same persistence, apply, client-completion, ReadIndex, and
InstallSnapshot effects translated for the simulator.

Acceptance schedules use the real `Core` and explicit durable completion
events to cover:

- election, three metadata command proposals, majority commit, and ordered
  apply in three- and five-voter clusters;
- contextual ReadIndex completion and checker-valid client histories;
- asymmetric delay, dropped messages, duplication, and deliberate reordering;
- isolation of a former leader with an already-pending read;
- leader crash after commitment and election of a replacement retaining the
  committed prefix;
- a minority partition unable to commit its unreplicated command; and
- a crashed lagging follower recovering through durable InstallSnapshot,
  then recovering that snapshot through another restart.

All acceptance failures use `Simulator::require`, which includes the `sim-v1`
seed, topology, fault controls, and selected event trace. Checker violations
include `linear-history-v1` replay text.

## Alternatives

- **Only run existing slice tests:** rejected because they do not establish
  the parent issue's assembled acceptance criteria in one faulted workflow.
- **Use a test-only fake state machine or fake quorum:** rejected because it
  would bypass persistence, commit, ReadIndex, and snapshot invariants.
- **Enumerate every permutation:** rejected because it repeats equivalent
  states and has no bounded inconclusive result.
- **Return success when the search budget expires:** rejected as unsound.
- **Treat all timeouts as uncertain:** rejected for this typed contract;
  callers must record `indeterminate` when completion is genuinely unknown.
- **Claim production linearizability:** rejected until the real server,
  transport, durable state machine, and client history instrumentation exist.

## Validation

Checker tests cover sequential and overlapping get/put/erase/CAS histories,
revision semantics, failures, timeouts, indeterminate and pending mutations,
strict malformed input, deterministic replay, one-minimal counterexamples, and
resource exhaustion.

Raft acceptance tests cover healthy and faulted three-/five-node schedules,
durability effects, ordered application, history checking, former-leader read
safety, minority safety, committed-write survival, snapshot catch-up, and
restart recovery. Existing slice suites remain the detailed regression proof
for each protocol boundary.
