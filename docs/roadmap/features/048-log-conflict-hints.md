# Feature 048: Log Conflict Hints

- **Phase:** 4
- **Problem:** Decrementing `nextIndex` one entry at a time is slow.
- **Deliverable:** Return conflicting term and first index hints.
- **Exit criterion:** Divergent followers converge in bounded message rounds.
