# Feature 024: WAL Segment Rotation

- **Phase:** 3
- **Problem:** One indefinitely growing file complicates recovery and truncation.
- **Deliverable:** Seal and rotate segments at a configured safe boundary.
- **Exit criterion:** Rotation cannot split or overwrite a record.
