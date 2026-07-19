# Feature 080: OpenTelemetry Tracing

- **Phase:** 5
- **Problem:** Proposal latency spans client, consensus, disk, and apply.
- **Deliverable:** Propagate trace context through bounded sampled spans.
- **Exit criterion:** One request exposes wait time at each boundary.
