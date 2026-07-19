# Feature 047: AppendEntries Heartbeat

- **Phase:** 4
- **Problem:** Followers need leadership and commit progress without new commands.
- **Deliverable:** Send empty AppendEntries on logical heartbeat ticks.
- **Exit criterion:** Healthy followers do not start elections under bounded delay.
