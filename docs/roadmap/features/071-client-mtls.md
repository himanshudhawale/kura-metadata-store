# Feature 071: Client mTLS

- **Phase:** 5
- **Problem:** Network reachability must not grant metadata access.
- **Deliverable:** Authenticate client certificates against reloadable trust roots.
- **Exit criterion:** Untrusted clients cannot complete an API handshake.
