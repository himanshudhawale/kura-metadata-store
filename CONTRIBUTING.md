# Contributing to Kura Metadata Store

Thank you for helping build a small, understandable distributed metadata store.
Correctness is more important than feature count or benchmark speed.

## Ways to contribute

- Propose a feature through the feature-request form.
- Improve specifications, diagrams, examples, or failure analysis.
- Add deterministic tests and fault-injection scenarios.
- Implement an accepted roadmap issue.
- Review API, storage-format, protocol, and compatibility decisions.

Good first contributions will be labeled `good first issue`. Larger design work
will be labeled `design needed` until its specification is accepted.

## Design-first workflow

Before implementation, an issue must describe:

1. User or system problem
2. Public behavior
3. Correctness invariants
4. Failure and retry semantics
5. Compatibility impact
6. Test strategy
7. Alternatives and tradeoffs

Small corrections and tests do not require a separate design document. Durable
formats, protocols, consistency guarantees, and public APIs do.

## Development

Requirements:

- A C++23 compiler
- CMake 3.25 or newer

Run:

```shell
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

Pull requests should:

- Address one coherent change.
- Link the corresponding issue.
- Include tests for changed behavior.
- Update documentation with public behavior.
- Avoid unrelated formatting or refactoring.
- Explain failure behavior and compatibility impact.

C++ contributions must use RAII, avoid owning raw pointers, preserve explicit
ownership at API boundaries, and include sanitizer-friendly tests for unsafe
input or concurrency behavior.

By submitting a contribution, you agree that it is licensed under Apache-2.0.
