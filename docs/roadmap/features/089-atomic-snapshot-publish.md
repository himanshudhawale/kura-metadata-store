# Feature 089: Atomic Snapshot Publish

- **Phase:** 6
- **Problem:** Two writers must not both replace one current pointer.
- **Deliverable:** Compare expected pointer revision and live writer fencing in one transaction.
- **Exit criterion:** Concurrent publication produces exactly one winner.
