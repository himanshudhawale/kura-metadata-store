# Feature 075: Audit Events

- **Phase:** 5
- **Problem:** Security-sensitive changes require attributable records.
- **Deliverable:** Emit bounded structured events for auth, admin, and mutations.
- **Exit criterion:** Secrets and full values never enter audit output.
