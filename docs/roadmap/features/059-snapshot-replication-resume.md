# Feature 059: Snapshot Replication Resume

- **Phase:** 4
- **Problem:** Repeated large snapshot restarts waste bandwidth.
- **Deliverable:** Negotiate verified chunk offsets for one snapshot identity.
- **Exit criterion:** Resumed bytes cannot mix two snapshot generations.
