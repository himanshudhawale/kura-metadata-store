# Feature 064: Strict Reconfiguration Check

- **Phase:** 4
- **Problem:** Operational requests can accidentally remove too many healthy voters.
- **Deliverable:** Reject changes that lose a currently reachable quorum.
- **Exit criterion:** Unsafe configuration requests have actionable errors.
