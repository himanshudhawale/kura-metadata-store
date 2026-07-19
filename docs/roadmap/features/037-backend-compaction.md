# Feature 037: Backend Compaction

- **Phase:** 3
- **Problem:** Obsolete MVCC versions consume logical storage.
- **Deliverable:** Remove history through the committed compaction revision.
- **Exit criterion:** Current state and retained watch history remain readable.
