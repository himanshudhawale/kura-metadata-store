# Feature 020: Fencing Token

- **Phase:** 2
- **Problem:** A paused lock holder may resume after lease loss.
- **Deliverable:** Expose monotonic ownership revisions for protected writes.
- **Exit criterion:** Stale owners cannot mutate guarded metadata.
