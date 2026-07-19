# Feature 030: Snapshot Integrity

- **Phase:** 3
- **Problem:** Corrupt state cannot be installed safely.
- **Deliverable:** Validate metadata and content hashes before exposure.
- **Exit criterion:** One changed byte causes deterministic rejection.
