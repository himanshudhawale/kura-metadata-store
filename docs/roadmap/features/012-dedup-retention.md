# Feature 012: Deduplication Retention

- **Phase:** 2
- **Problem:** Deduplication records need safe bounded cleanup.
- **Deliverable:** Define sequence windows and retention independent of wall-clock apply.
- **Exit criterion:** Cleanup cannot remove a record still valid for retry.
