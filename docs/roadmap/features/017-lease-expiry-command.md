# Feature 017: Lease Expiry Command

- **Phase:** 2
- **Problem:** Followers cannot expire leases from independent clocks.
- **Deliverable:** Convert leader-observed expiry into replicated commands.
- **Exit criterion:** All replicas delete the same attached keys at one revision.
