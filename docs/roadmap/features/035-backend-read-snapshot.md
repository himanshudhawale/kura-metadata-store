# Feature 035: Backend Read Snapshot

- **Phase:** 3
- **Problem:** Concurrent reads need one stable backend revision.
- **Deliverable:** Expose bounded immutable read transactions.
- **Exit criterion:** Range results cannot mix two applied revisions.
