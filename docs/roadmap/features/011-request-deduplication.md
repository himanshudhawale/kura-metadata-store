# Feature 011: Request Deduplication

- **Phase:** 2
- **Problem:** A lost response makes mutating retries ambiguous.
- **Deliverable:** Replicate request identifiers and original results.
- **Exit criterion:** Retrying one identifier cannot apply its command twice.
