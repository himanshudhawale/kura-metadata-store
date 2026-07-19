# ADR-0002: Implement Kura Metadata Store in C++23

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

The metadata store will eventually run consensus, persist a checksummed WAL,
serve watches, manage leases, and coordinate high-scale systems. Its primary
latency floor remains quorum and durable storage, but native implementation
provides explicit control over allocation, binary protocols, scheduling, and
tail latency while aligning with Kura Engine.

## Decision

Kura Metadata Store will use C++23 and CMake. The previous Java Phase 1
foundation is replaced rather than maintained as a second implementation.

Required practices:

1. RAII for files, sockets, locks, and allocated memory.
2. No owning raw pointers.
3. Immutable or value-owned request and result boundaries.
4. Explicit integer overflow checks for revisions, versions, terms, and indexes.
5. Address, undefined-behavior, and thread sanitizers in continuous integration.
6. Fuzz tests for WAL, snapshot, protocol, and recovery input.
7. Deterministic state-machine code must not read wall-clock time.
8. Platform-specific I/O remains behind tested interfaces.

## Consequences

C++ enables predictable memory behavior and native systems integration. It also
increases memory-safety and portability risk. Sanitizers, fuzzing, ownership
rules, and multi-platform CI are therefore part of the product definition.
