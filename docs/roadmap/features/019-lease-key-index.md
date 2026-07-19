# Feature 019: Lease Key Index

- **Phase:** 2
- **Problem:** Revocation must find attached keys without scanning the store.
- **Deliverable:** Maintain a deterministic lease-to-key index.
- **Exit criterion:** Attach, detach, revoke, and restore preserve index consistency.
