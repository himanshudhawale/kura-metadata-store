<p align="center">
  <img src="assets/kura-metadata-store-icon.svg" width="180" alt="Kura Metadata Store: a K monogram inside a data cube">
</p>

<h1 align="center">Kura Metadata Store</h1>

Kura Metadata Store is a small, strongly consistent metadata service being
built from first principles. Its long-term role is to provide reusable
coordination primitives for distributed systems and serve as
[Kura Engine](https://github.com/himanshudhawale/kura-engine)'s metadata
authority.

The service is implemented in modern **C++23** for predictable latency,
explicit memory ownership, efficient binary protocols, and direct control over
WAL, networking, and state-machine execution.

> **Current status:** Phase 1 is a single-node, in-memory deterministic state
> machine. Phase 3 WAL/snapshot storage and atomic Raft term/vote persistence
> boundaries exist but are not connected to mutation responses. A deterministic
> logical-time Raft simulator, executable Figure 2 specification, and internal
> election, AppendEntries, majority-commit, and ordered-apply slices exist, but
> quorum-confirmed ReadIndex is now included. Snapshots and the production Raft
> service do not exist. An in-process Kura helper
> safely composes snapshot transactions, leases, and watches without claiming a
> remote or distributed client. The service is not distributed,
> replicated, durably integrated, or highly available. Do not use it for
> production metadata.

## Why build it?

Distributed systems need a trustworthy place for small, important decisions:

- Which node is the leader?
- Which service instances are alive?
- Which configuration version is current?
- Who owns a job or lease?
- Which immutable database snapshot is canonical?

The final service will expose a compact key-value API with revisions,
compare-and-set transactions, watches, and leases. Raft consensus will replicate
the deterministic state machine across an odd-sized cluster.

## Planned API

```text
get(key)
range(start, end)
put(key, value)
delete(key)
transaction(if comparisons, then operations, else operations)
watch(prefix, fromRevision)
grantLease(requestedId, ttl, tick)
keepAlive(lease, fencingToken, tick)
timeToLive(lease, tick)
revokeLease(lease, fencingToken, tick)
expireLeases(tick)
```

The initial code implements versioned key operations, atomic If/Then/Else
transactions, and deterministic lease lifecycle with fenced ownership and
atomic attached-key cleanup. It establishes semantics for later persistence and
replication; it does not pretend those guarantees already exist.

## Architecture

```text
Client API
    |
    v
+-----------------------------+
| Raft consensus              |  Phase 4
| leader, log, quorum, reads  |
+-----------------------------+
    |
    v committed commands
+-----------------------------+
| Deterministic state machine |  Phases 1-2
| KV, revisions, Txn, Watch   |
+-----------------------------+
    |
    v
+-----------------------------+
| WAL + snapshots + backend   |  Phase 3
+-----------------------------+
```

Only committed Raft commands may eventually reach the state machine. The same
ordered command sequence must produce byte-for-byte equivalent state on every
node.

## Kura Engine compatibility

Kura Engine needs atomic snapshot publication:

```text
compare current table revision == expected revision
then set current snapshot pointer = new immutable manifest
else report a conflict
```

The in-process C++23 helper now provides fenced writer publication,
lease-backed reader registrations, reader-protected metadata collection, and
compaction-aware watch resynchronization. Move-only guards own automatic
keepalive and cleanup. A remote transport and real leader-failover integration
still depend on later phases. See [Kura integration](docs/kura-integration.md).

Lease time is currently supplied as logical ticks by the caller. The core does
not read a wall clock, and no background expiry driver is implemented.

## Documentation

- [Architecture](docs/architecture.md)
- [API and data model](docs/api.md)
- [Kura Engine integration](docs/kura-integration.md)
- [Implementation roadmap](docs/roadmap.md)
- [Correctness traps](docs/correctness-traps.md)
- [Research sources](docs/research-sources.md)
- [ADR-0001: phase claims](docs/decisions/0001-phase-claims.md)
- [ADR-0002: C++23 implementation](docs/decisions/0002-cpp23-implementation.md)
- [ADR-0003: internal Raft implementation](docs/decisions/0003-internal-raft.md)
- [Brand assets and usage](docs/brand.md)
- [Durable WAL and snapshot design](docs/design/0005-durable-wal-snapshots.md)
- [Deterministic Raft simulator design](docs/design/0006-deterministic-raft-simulator.md)
- [Executable Raft Figure 2 design](docs/design/0007-executable-raft-figure-2.md)
- [Raft hard-state persistence design](docs/design/0008-raft-hard-state-persistence.md)
- [Deterministic Raft election design](docs/design/0009-raft-election-core.md)
- [Kura metadata helper design](docs/design/0010-kura-metadata-helper-client.md)
- [Deterministic AppendEntries design](docs/design/0011-raft-append-entries.md)
- [Raft majority commit and apply design](docs/design/0012-raft-commit-apply.md)
- [Raft ReadIndex design](docs/design/0013-raft-read-index.md)
- [WAL format v1](docs/formats/wal-v1.md)
- [Snapshot format v1](docs/formats/snapshot-v1.md)
- [Raft hard-state format v1](docs/formats/raft-hard-state-v1.md)

## Build

Requirements:

- A C++23 compiler
- CMake 3.25+

```shell
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

`--config Release` and `-C Release` are required by multi-configuration
generators such as Visual Studio and harmless for single-configuration builds.

## Contributing

Feature requests and bug reports use structured GitHub forms. Design discussion
is welcome in GitHub Discussions. Start with [CONTRIBUTING.md](CONTRIBUTING.md).

Kura Metadata Store is licensed under [Apache-2.0](LICENSE).
