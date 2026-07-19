# Feature 040: Restore Revision Bump

- **Phase:** 3
- **Problem:** Restoring an old revision can confuse existing watchers.
- **Deliverable:** Advance restored revision and mark skipped history compacted.
- **Exit criterion:** New events never reuse a previously observable revision.
