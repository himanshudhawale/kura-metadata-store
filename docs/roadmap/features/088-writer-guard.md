# Feature 088: Writer Guard

- **Phase:** 6
- **Problem:** Snapshot publication needs renewable ownership and fencing.
- **Deliverable:** Provide an RAII guard owning lease and fencing revision.
- **Exit criterion:** Lost ownership prevents every subsequent publish.
