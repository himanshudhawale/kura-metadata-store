# Feature 093: Kura Client Retry

- **Phase:** 6
- **Problem:** Metadata calls fail ambiguously across leader changes.
- **Deliverable:** Combine deadlines, idempotency IDs, leader hints, and backoff.
- **Exit criterion:** Retry never converts an uncertain mutation into a duplicate.
