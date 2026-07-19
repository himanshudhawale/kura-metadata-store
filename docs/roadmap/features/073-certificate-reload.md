# Feature 073: Certificate Reload

- **Phase:** 5
- **Problem:** Certificate rotation should not require cluster downtime.
- **Deliverable:** Atomically replace validated key and trust material.
- **Exit criterion:** Existing and new connections follow documented rotation overlap.
