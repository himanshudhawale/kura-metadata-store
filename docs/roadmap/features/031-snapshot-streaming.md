# Feature 031: Snapshot Streaming

- **Phase:** 3
- **Problem:** Large snapshots should not require one contiguous allocation.
- **Deliverable:** Produce and consume bounded chunks with incremental integrity.
- **Exit criterion:** Peak buffer usage remains below the configured chunk budget.
