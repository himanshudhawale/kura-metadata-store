# Feature 058: InstallSnapshot Chunks

- **Phase:** 4
- **Problem:** Lagging followers may need snapshots larger than one message.
- **Deliverable:** Transfer ordered bounded chunks with identity and checksum.
- **Exit criterion:** Interrupted transfer resumes or restarts without partial install.
