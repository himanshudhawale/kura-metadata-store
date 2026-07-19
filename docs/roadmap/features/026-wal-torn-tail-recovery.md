# Feature 026: WAL Torn Tail Recovery

- **Phase:** 3
- **Problem:** A crash may leave one incomplete final record.
- **Deliverable:** Recover the valid prefix and truncate only a provable torn tail.
- **Exit criterion:** Interior corruption is never treated as a safe tail.
