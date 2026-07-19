# Feature 036: Backend Space Quota

- **Phase:** 3
- **Problem:** Full disks can turn ordinary writes into unsafe partial failures.
- **Deliverable:** Reserve space and enter explicit no-space maintenance mode.
- **Exit criterion:** Quota exhaustion rejects writes before WAL acknowledgement.
