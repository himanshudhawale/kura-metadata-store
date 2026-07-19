# Feature 010: Compaction Boundary

- **Phase:** 2
- **Problem:** Historical state cannot grow forever.
- **Deliverable:** Advance one explicit compaction revision transactionally.
- **Exit criterion:** Older reads fail with the exact compacted-through revision.
