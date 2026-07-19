# Feature 079: Readiness Endpoint

- **Phase:** 5
- **Problem:** A live node may be unable to serve safe requests.
- **Deliverable:** Check corruption alarms, apply health, and linearizable reads.
- **Exit criterion:** Load balancers exclude nodes lacking required guarantees.
