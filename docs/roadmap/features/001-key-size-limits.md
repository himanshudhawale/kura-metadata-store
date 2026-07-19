# Feature 001: Key Size Limits

- **Phase:** 2
- **Problem:** Unbounded keys can exhaust memory before validation.
- **Deliverable:** Enforce one configured binary-key limit at every API boundary.
- **Exit criterion:** Oversized keys fail before allocation or state mutation.
