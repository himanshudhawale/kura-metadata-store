# Feature 091: Snapshot GC Scan

- **Phase:** 6
- **Problem:** Old object data can be deleted only without live readers.
- **Deliverable:** Read registrations and retention policy at one revision.
- **Exit criterion:** Collection never removes a protected snapshot.
