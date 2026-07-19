# Feature 090: Reader Guard

- **Phase:** 6
- **Problem:** Garbage collection must protect snapshots in active queries.
- **Deliverable:** Register snapshot identity under a keepalive lease.
- **Exit criterion:** Guard loss is observable before further protected reads.
