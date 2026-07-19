# Feature 025: WAL Group Commit

- **Phase:** 3
- **Problem:** One synchronization call per proposal limits throughput.
- **Deliverable:** Batch compatible durability waiters behind one disk sync.
- **Exit criterion:** Every acknowledged waiter is covered by the completed sync.
