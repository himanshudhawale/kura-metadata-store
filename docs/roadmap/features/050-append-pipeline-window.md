# Feature 050: Append Pipeline Window

- **Phase:** 4
- **Problem:** One in-flight RPC per follower underuses network capacity.
- **Deliverable:** Bound pipelined AppendEntries by bytes and message count.
- **Exit criterion:** Reordering cannot advance match index past an unconfirmed gap.
