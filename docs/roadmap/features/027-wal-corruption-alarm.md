# Feature 027: WAL Corruption Alarm

- **Phase:** 3
- **Problem:** Silent recovery past corruption can violate consensus safety.
- **Deliverable:** Stop startup with structured corruption location and reason.
- **Exit criterion:** Operators can identify the exact segment and offset.
