# Feature 034: Backend Atomic Apply

- **Phase:** 3
- **Problem:** State changes and applied index must survive together.
- **Deliverable:** Commit command effects and `lastApplied` in one backend transaction.
- **Exit criterion:** Restart cannot apply a committed command twice.
