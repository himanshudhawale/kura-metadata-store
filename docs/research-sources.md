# Research Sources

Primary references guiding the design:

1. Diego Ongaro and John Ousterhout, **In Search of an Understandable Consensus
   Algorithm (Raft)**: <https://raft.github.io/raft.pdf>
2. Diego Ongaro, **Consensus: Bridging Theory and Practice**:
   <https://github.com/ongardie/dissertation/blob/master/stanford.pdf>
3. Maurice Herlihy and Jeannette Wing, **Linearizability: A Correctness
   Condition for Concurrent Objects**:
   <https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf>
4. Cary Gray and David Cheriton, **Leases: An Efficient Fault-Tolerant Mechanism
   for Distributed File Cache Consistency**:
   <https://web.stanford.edu/class/cs240/readings/leases.pdf>
5. etcd, **API Reference**:
   <https://etcd.io/docs/v3.5/learning/api/>
6. etcd, **API Guarantees**:
   <https://etcd.io/docs/v3.5/learning/api_guarantees/>
7. etcd, **Data Model**:
   <https://etcd.io/docs/v3.5/learning/data_model/>
8. etcd, **Client Design**:
   <https://etcd.io/docs/v3.5/learning/design-client/>
9. etcd, **Learner Design**:
   <https://etcd.io/docs/v3.5/learning/design-learner/>
10. etcd, **Maintenance**:
    <https://etcd.io/docs/v3.5/op-guide/maintenance/>
11. etcd, **Disaster Recovery**:
    <https://etcd.io/docs/v3.5/op-guide/recovery/>
12. etcd, **Runtime Reconfiguration**:
    <https://etcd.io/docs/v3.5/op-guide/runtime-configuration/>
13. etcd, **Transport Security**:
    <https://etcd.io/docs/v3.5/op-guide/security/>
14. etcd, **Monitoring**:
    <https://etcd.io/docs/v3.5/op-guide/monitoring/>
15. Jepsen, **Linearizability**:
    <https://jepsen.io/consistency/models/linearizable>

etcd is a reference for proven API shape and operational lessons. Kura Metadata
Store is not an etcd fork and must specify and test its own guarantees.
