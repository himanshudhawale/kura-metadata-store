# Feature 013: Watch Fanout Index

- **Phase:** 2
- **Problem:** Every mutation cannot scan every watcher at scale.
- **Deliverable:** Index exact-key and range subscriptions by binary boundaries.
- **Exit criterion:** Delivery work scales with matching watchers.
