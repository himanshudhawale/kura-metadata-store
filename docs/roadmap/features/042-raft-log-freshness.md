# Feature 042: Raft Log Freshness

- **Phase:** 4
- **Problem:** A stale candidate must not become leader.
- **Deliverable:** Compare last term and index lexicographically for votes.
- **Exit criterion:** Every stale-log election scenario is rejected.
