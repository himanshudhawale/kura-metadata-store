# Feature 023: WAL Segment Naming

- **Phase:** 3
- **Problem:** Recovery order must not depend on directory enumeration.
- **Deliverable:** Name segments by fixed-width first index and generation.
- **Exit criterion:** Lexicographic and logical ordering are identical.
