# Feature 067: Peer Flow Control

- **Phase:** 4
- **Problem:** A slow follower can consume unbounded leader buffers.
- **Deliverable:** Bound peer queues and replication windows.
- **Exit criterion:** Slow peers reduce their own throughput, not cluster memory safety.
