# Kura Metadata Store

Kura Metadata Store is a small, strongly consistent metadata service being
built from first principles. Its long-term role is to provide reusable
coordination primitives for distributed systems and serve as
[Kura Engine](https://github.com/himanshudhawale/kura-engine)'s metadata
authority.

> **Current status:** Phase 1 is a single-node, in-memory deterministic state
> machine. It is not distributed, replicated, durable, or highly available.
> Do not use it for production metadata.

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
grantLease(ttl)
keepAlive(lease)
revokeLease(lease)
```

The initial code implements versioned `get`, `range`, `put`, `delete`, and one
compare-and-set operation. It establishes semantics for later persistence and
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

Later, lease-backed reader registrations prevent Kura from garbage-collecting a
snapshot while a query still reads it. Watches notify compute nodes when a new
snapshot becomes current. See [Kura integration](docs/kura-integration.md).

## Documentation

- [Architecture](docs/architecture.md)
- [API and data model](docs/api.md)
- [Kura Engine integration](docs/kura-integration.md)
- [Implementation roadmap](docs/roadmap.md)
- [Correctness traps](docs/correctness-traps.md)
- [Research sources](docs/research-sources.md)
- [ADR-0001: phase claims](docs/decisions/0001-phase-claims.md)

## Build

Requirements:

- Java 21
- Maven 3.9+

```shell
mvn test
```

## Contributing

Feature requests and bug reports use structured GitHub forms. Design discussion
is welcome in GitHub Discussions. Start with [CONTRIBUTING.md](CONTRIBUTING.md).

Kura Metadata Store is licensed under [Apache-2.0](LICENSE).
