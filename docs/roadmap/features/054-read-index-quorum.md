# Feature 054: ReadIndex Quorum

- **Phase:** 4
- **Problem:** A stale leader cannot safely serve linearizable reads.
- **Deliverable:** Confirm current leadership with a quorum heartbeat context.
- **Exit criterion:** Reads wait for quorum and local apply through the read index.
