# Feature 008: Key Generations

- **Phase:** 2
- **Problem:** Delete and recreate must not continue the old key version.
- **Deliverable:** Persist generation boundaries and creation revisions.
- **Exit criterion:** Recreated keys start at version one with a new creation revision.
