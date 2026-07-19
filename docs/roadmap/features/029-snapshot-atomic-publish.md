# Feature 029: Snapshot Atomic Publication

- **Phase:** 3
- **Problem:** A crash must not replace a valid snapshot with a partial one.
- **Deliverable:** Write, sync, rename, and synchronize the parent directory.
- **Exit criterion:** Recovery sees either the old or new complete snapshot.
