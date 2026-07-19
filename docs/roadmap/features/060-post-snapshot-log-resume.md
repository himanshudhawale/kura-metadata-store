# Feature 060: Post-Snapshot Log Resume

- **Phase:** 4
- **Problem:** Followers must continue replication after snapshot installation.
- **Deliverable:** Reset progress to the included index and send the log suffix.
- **Exit criterion:** No entry is skipped or applied twice.
