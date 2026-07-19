# Feature 055: ReadIndex Batching

- **Phase:** 4
- **Problem:** One quorum round per read limits throughput.
- **Deliverable:** Share a confirmed read index among compatible waiting reads.
- **Exit criterion:** Batching preserves invocation/completion real-time order.
