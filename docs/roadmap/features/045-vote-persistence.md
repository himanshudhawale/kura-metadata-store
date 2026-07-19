# Feature 045: Vote Persistence

- **Phase:** 4
- **Problem:** A crash must not permit two votes in one term.
- **Deliverable:** Persist term and vote before granting a response.
- **Exit criterion:** Crash schedules never produce double voting.
