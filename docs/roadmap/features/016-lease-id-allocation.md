# Feature 016: Lease ID Allocation

- **Phase:** 2
- **Problem:** Lease identifiers must remain unique across leaders and restore.
- **Deliverable:** Allocate deterministic nonzero signed identifiers.
- **Exit criterion:** Snapshot restore and leader change cannot reuse a live ID.
