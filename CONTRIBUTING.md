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

- Java 21
- Maven 3.9 or newer

Run:

```shell
mvn test
```

Pull requests should:

- Address one coherent change.
- Link the corresponding issue.
- Include tests for changed behavior.
- Update documentation with public behavior.
- Avoid unrelated formatting or refactoring.
- Explain failure behavior and compatibility impact.

By submitting a contribution, you agree that it is licensed under Apache-2.0.
