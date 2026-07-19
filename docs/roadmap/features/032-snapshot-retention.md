# Feature 032: Snapshot Retention

- **Phase:** 3
- **Problem:** Keeping every snapshot wastes disk while deleting all backups is unsafe.
- **Deliverable:** Retain a configurable number of verified generations.
- **Exit criterion:** Cleanup never removes the active or only recoverable snapshot.
