# Design 0007: Executable Raft Figure 2 specification

## Status and scope

Accepted as the rule-level foundation for Phase 4. This change maps every rule
in Raft Figure 2 to deterministic C++23 state transitions. It is deliberately
not a production Raft node: networking, timer scheduling, WAL adapters,
snapshots, membership changes, ReadIndex, and randomized elections remain
future work.

The executable API is `kura::metadata::figure2` in
`raft/figure2_spec.hpp`. `catalog()` is the review catalog; `step()` is the
executable transition function; `validate()` checks state invariants.

## Problem and motivation

Figure 2 is compact enough that implementations often scatter its conditions
among callbacks and accidentally weaken them. Particularly dangerous errors
include acknowledging a vote or append before stable storage, comparing log
indexes without comparing last terms, committing an old-term entry directly,
or applying beyond `commitIndex`.

A prose-only checklist can drift from code. Kura instead needs one typed model
that reviewers can inspect, tests can execute, and later production adapters
can use as an oracle. Each transition records the exact catalog rule(s) it
used and emits ordered effects rather than performing I/O.

## Chosen design

### State ownership

| Class | Figure 2 fields | Current C++ representation |
|---|---|---|
| Persistent on all servers | `currentTerm`, `votedFor`, `log[]` | `PersistentRaftState` in `State::persistent` |
| Volatile on all servers | `commitIndex`, `lastApplied` | `VolatileState` in `State::volatile_state` |
| Volatile on candidates | votes received | `State::votes_received` |
| Volatile on leaders | `nextIndex[]`, `matchIndex[]` | `LeaderState::progress` / `PeerProgress` |
| Volatile request bookkeeping | client entries awaiting apply | `LeaderState::pending_clients` |
| Role/context | role, known leader, voting peers | `State` |

`LeaderState` must exist exactly when the role is leader. The Figure 2 model
uses an unsnapshotted, one-based contiguous log; index zero and term zero are
sentinels. Snapshot offsets will require a separately reviewed extension.

### Events, transitions, and effects

`Event` is a closed `std::variant` of logical timeouts, RPC requests and
responses, hard-state persistence completions, client commands, and one-entry
apply opportunities. `step(old, event)` copies the input and returns:

1. the next state;
2. every `RuleId` that fired, in order;
3. ordered typed `Effect` values.

The core reads no clock and performs no network, disk, or state-machine I/O.
For `currentTerm`/`votedFor`, it emits `PersistRaftHardState`, records the
request and remaining effects as pending, and accepts no other input until the
matching `RaftHardStatePersisted` event arrives. Only that explicit input
releases a granted vote, higher-term response, or election RPC. Mismatched and
unsolicited completions are rejected.

`PersistState(log)` remains an ordered adapter barrier for the separately
versioned WAL: all named log fields must reach stable storage before the next
effect is started. Thus successful append replies and replication RPCs cannot
overtake their required log update.

`ApplyCommitted` advances exactly one index and emits `ApplyCommand`. A pending
leader client receives `CompleteClientCommand` only after that apply effect.
One-entry steps make ordering observable and easy to interrupt in tests.

### Terms, freshness, and commitment

Every incoming Raft RPC request or response first passes the all-server
higher-term rule. A greater term is persisted together with a cleared vote,
and candidate/leader volatile state is discarded before further processing.
Requests with a lower term receive a typed negative response. Stale responses
cannot mutate candidate or leader progress.

RequestVote freshness is lexicographic:

```text
candidateLastTerm > localLastTerm
or
candidateLastTerm == localLastTerm
    and candidateLastIndex >= localLastIndex
```

AppendEntries requires both the previous index and term. On a conflict, the
follower removes that entry and its suffix, appends the new suffix, persists
the changed log, and only then emits success. Leader commit advancement chooses
the greatest majority-replicated `N` whose entry term equals `currentTerm`.
Older entries can become committed indirectly when that current-term entry is
committed.

### Executable rule traceability

All scenarios below are deterministic cases in
`tests/figure2_spec_test.cpp`. The final test iterates `catalog()` and fails if
any catalog `RuleId` was not observed.

| Figure 2 rule / `RuleId` | Transition types and effects | Deterministic scenario |
|---|---|---|
| All: higher RPC term / `observe_higher_term` | all four `Receive*` RPC events; `PersistRaftHardState`, then matching completion | higher RequestVote and higher leader response force follower |
| All: apply committed / `apply_committed` | `ApplyCommitted`; `ApplyCommand`, optional `CompleteClientCommand` | committed leader entry applies before client completion |
| Follower: election timeout / `follower_election_timeout` | `ElectionTimeout` | follower timeout enters candidate |
| RequestVote: stale term / `request_vote_reject_stale` | `ReceiveRequestVote`; negative response | term 1 request at term 2 |
| RequestVote: grant / `request_vote_grant` | vote mutation; hard-state request, completion, timer reset, positive response | fresher higher-term candidate |
| RequestVote: deny / `request_vote_deny` | negative response | stale log and already-voted cases |
| AppendEntries: stale term / `append_entries_reject_stale` | `ReceiveAppendEntries`; negative response | term 2 leader at term 3 |
| AppendEntries: missing/mismatched previous entry / `append_entries_reject_log_mismatch` | negative response | wrong `prevLogTerm` |
| AppendEntries: conflict/delete/append / `append_entries_accept` | log mutation; persist before success | term-2 suffix replaced by term-3 suffix |
| AppendEntries: follower commit / `append_entries_advance_commit` | volatile commit-index update | `leaderCommit=3` after accepted append |
| Candidate: start election / `candidate_start_election` | term/self-vote; completion before RequestVote sends | first follower timeout |
| Candidate: restart election / `candidate_restart_election` | `ElectionTimeout` in candidate | second timeout increments term |
| Candidate: majority / `candidate_win_election` | granted `ReceiveRequestVoteResponse` | self plus peer vote in three-node cluster |
| Candidate: valid AppendEntries / `candidate_accept_append_entries` | role/leader update then append receiver rules | current-term heartbeat makes candidate follower |
| Leader: initialize / `leader_initialize` | `LeaderState` and initial sends | election win initializes both peers |
| Leader: client command / `leader_append_client` | append/pending state; persist before sends | command at index one |
| Leader: send entries / `leader_replicate` | `SendAppendEntries` | initialization, heartbeat, append, and retry |
| Leader: successful append / `leader_record_replication` | update `matchIndex`/`nextIndex` | peer confirms index one |
| Leader: failed append / `leader_retry_replication` | decrement `nextIndex`, retry send | failure at lower bound one |
| Leader: majority commit / `leader_advance_commit` | volatile commit-index update | current-term index one reaches two of three |
| Leader: heartbeat / `leader_heartbeat` | `HeartbeatTimeout`; sends to all peers | idle leader heartbeat |

The descriptor for each row also contains executable-review text for
preconditions, state changes, emitted effects, and persistence ordering.

## Invariants and edge cases

`validate()` runs before and after every step and rejects:

- duplicate peers or the local node in its peer list;
- non-contiguous logs or entry terms beyond `currentTerm`;
- `lastApplied > commitIndex > lastLogIndex`;
- leader-only state on non-leaders or missing leader state on leaders;
- incomplete or out-of-range peer progress;
- votes from outside the voting set and invalid pending-client indexes.

RPC actor fields must match their sender, and senders must be voting peers.
Duplicate vote responses are idempotent because votes are a set. `nextIndex`
never drops below one. Term and log-index overflow are errors. A same-term
AppendEntries causes a candidate (and any competing leader) to follow; a
lower-term request cannot reset the election timer. Empty AppendEntries uses
the same previous-entry and commit rules as replication.

The model intentionally ignores malformed byte protocols, transport
authentication, joint consensus, learners, snapshot indexes, client
redirection, and durable apply-position integration. These are not Figure 2
rules and must not be inferred from this specification.

## Alternatives considered

- **Documentation table only:** rejected because it cannot detect drift,
  persistence reordering, or missing test coverage.
- **Directly implement a complete `RaftNode`:** rejected because issue #8 is a
  specification task and premature I/O/timer policy would obscure the rules.
- **Virtual rule hierarchy:** rejected in favor of variants and values, which
  are closed, inspectable, deterministic, and require no ownership protocol.
- **Callbacks inside `step()`:** rejected because hidden disk/network work
  makes crash-boundary ordering untestable.
- **One monolithic “persist everything” effect:** rejected because named fields
  make the durability obligation reviewable and minimize future writes.
- **A synchronous term/vote barrier:** replaced by an explicit completion
  input so a simulator or production event loop can crash or delay persistence
  without accidentally releasing a success response.

## Validation

`kura_figure2_spec_tests` checks the catalog schema, invariant failures, term
and freshness comparisons, every receiver branch, conflict replacement,
candidate transitions, leader progress, majority commitment, apply/client
ordering, blocked input while hard state is pending, completion correlation,
and the relative positions of persistence and send effects. It also proves
every current catalog row has an observed deterministic scenario.

The existing state-machine and durable-storage suites remain unchanged and are
run with the new suite through CTest.
