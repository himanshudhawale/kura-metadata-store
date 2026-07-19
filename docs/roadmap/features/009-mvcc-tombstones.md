# Feature 009: MVCC Tombstones

- **Phase:** 2
- **Problem:** Historical reads and watches must observe deletions.
- **Deliverable:** Store versioned tombstones without exposing them as current values.
- **Exit criterion:** Retained revisions distinguish absent, deleted, and recreated keys.
