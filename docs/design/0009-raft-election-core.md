# Design 0009: Deterministic Raft election core

## Status and scope

Accepted as slice 1 of issue #6 and the implementation of issue #11. This
slice implements follower/candidate elections and RequestVote for fixed,
odd-sized clusters. It does not implement AppendEntries, log replication,
commitment, state-machine application, membership changes, peer transport, or
production service integration.

AppendEntries was subsequently added by
[Design 0011](0011-raft-append-entries.md); this document retains the reviewed
slice 1 boundary and rationale.

## Problem and why

An election must make the same decision for the same ordered inputs, while
preserving the durable `currentTerm`/`votedFor` rules across crashes. Wall
clocks, implicit disk callbacks, and immediate vote sends make uncommon
interleavings difficult to reproduce and can permit a restarted server to vote
twice.

The core therefore consumes typed events and returns ordered typed effects.
The environment owns logical time, persistence, and delivery. The existing
Figure 2 transition specification remains the source of RequestVote and term
rules rather than creating a second election algorithm.

## State, inputs, and effects

`raft::election::Core` owns a `figure2::State`, seeded SplitMix64 state, and the
active election timer identity. Construction loads a durable
`RaftHardState`, fixed peers, and the local log used only for RequestVote
freshness. `start()` emits the initial deadline.

Inputs are:

- an election deadline carrying its timer identity;
- a typed RequestVote request or response; and
- `RaftHardStatePersisted`, the existing explicit durable-completion event.

Effects are ordered:

- `RoleTransition`;
- `PersistRaftHardState`;
- typed RequestVote request/response sends;
- `ResetElectionDeadline`; and
- `CancelElectionDeadline`.

The core reports leader election but filters the Figure 2 leader's initial
AppendEntries effects. Replication belongs to a later slice.

Each reset selects an inclusive delay in `[minimum, maximum]` from the supplied
seed. There are no clock reads or duration conversions. Equal seeds and inputs
produce equal deadlines; stale timer identities are ignored.

## RequestVote and quorum behavior

Figure 2 transitions provide:

- stale-term rejection with the current term;
- adoption of any higher request or response term and follower step-down;
- lexicographic log freshness by last term and then last index;
- at most one candidate vote per term, including recovery;
- a durable self-vote before election requests;
- vote tracking in a set, so duplicate responses do not increase the count;
  and
- majority `(cluster size / 2) + 1`, including the self-vote.

The current constructor accepts fixed odd clusters of at least three nodes.
Tests exercise three-node quorum two and five-node quorum three. Membership is
not changed by this slice.

## Persistence ordering and crash behavior

The Figure 2 state gates all dependent effects behind
`PersistRaftHardState`. Only a matching completion releases them. A higher-term
RequestVote can require two explicit completions: first the new term with no
vote, then the selected vote. This is conservative and leaves a valid durable
state at either crash point.

No granted vote, self-vote RequestVote send, or response carrying an adopted
higher term is emitted early. The simulation adapter queues other environment
events while a hard-state operation is pending and resumes them in arrival
order after completion.

The simulator encodes each completed hard state as an internal, fixed-width
opaque record. Restart creates a fresh adapter, reloads only completed records,
and starts as a follower. A crash before completion loses the pending record.
A crash after completion reloads the vote and cannot grant another candidate
in that term. This adapter encoding is not a network or disk format; the real
filesystem boundary remains `RaftHardStateStore`.

## Simulator adapter

`make_simulation_factory` connects the core to the existing simulator:

- `StartEvent` loads completed hard-state records and sets the first timer;
- `MessageEvent` decodes RequestVote only;
- `TimerEvent` supplies a logical deadline;
- `PersistedEvent` becomes the matching typed completion;
- sends, persistence requests, and timer changes become simulator actions.

The adapter's compact codec is deliberately private and rejects unknown,
truncated, and malformed payloads. A test observer exposes immutable snapshots
without giving the simulator control over core state.

## Alternatives

- **Wall-clock timers and sleeps:** rejected because scheduling would be slow
  and irreproducible.
- **Random device inside the core:** rejected because a failure could not be
  replayed from explicit inputs.
- **Sending before disk completion:** rejected because restart could double
  vote or regress a term.
- **A separate election implementation:** rejected because it could diverge
  from the executable Figure 2 rules.
- **Counting response messages:** rejected because duplicated grants would
  manufacture a quorum; voters are stored in a set.
- **Adding AppendEntries now:** rejected to keep the first slice reviewable and
  avoid implying replication or availability.

## Edge cases

- Zero/self/duplicate peers, even clusters, and invalid timeout ranges fail
  construction.
- Bootstrap term zero with a vote and recovered votes outside membership fail.
- Unknown RPC senders and candidate/sender mismatches fail closed.
- Stale responses cannot elect; higher responses always step down.
- Timer and hard-state request ID exhaustion throw instead of wrapping.
- Inputs reaching the direct core while persistence is pending are rejected by
  the Figure 2 gate; the simulator adapter queues them deterministically.
- Simultaneous timeouts may split a vote; a later candidate timeout increments
  the term and retries with a newly persisted self-vote.

## Validation

`kura_raft_election_tests` deterministically covers:

- seeded deadline reproduction and effect ordering;
- simultaneous timeout split votes;
- stale terms and log freshness;
- delayed term/vote persistence and response release;
- crash between persistence request and completion;
- recovered vote protection against double voting;
- duplicate responses and three-/five-node quorum calculations;
- higher-term candidate step-down; and
- complete three- and five-node simulator election schedules.

All repository tests are also run with the MSVC configuration used by CI.
