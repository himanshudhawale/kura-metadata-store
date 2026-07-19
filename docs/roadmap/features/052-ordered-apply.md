# Feature 052: Ordered Apply

- **Phase:** 4
- **Problem:** Replicas must execute committed commands in the same order.
- **Deliverable:** Apply every index exactly once from `lastApplied + 1`.
- **Exit criterion:** Gaps, duplicates, and apply beyond commit are impossible.
