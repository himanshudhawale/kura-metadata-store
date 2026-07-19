# Feature 033: WAL Snapshot Truncation

- **Phase:** 3
- **Problem:** WAL data can be removed only after durable snapshot coverage.
- **Deliverable:** Gate segment deletion by included index and verified publication.
- **Exit criterion:** Recovery always has a snapshot plus complete log suffix.
