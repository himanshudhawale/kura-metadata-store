# Testing Strategy

## Layers

- Unit tests prove value, revision, comparison, and range semantics.
- Model tests compare optimized state machines with a simple reference model.
- Property tests generate command sequences and validate invariants.
- Recovery tests interrupt every WAL and snapshot write boundary.
- Concurrency tests exercise CAS, watch registration, lease expiry, and apply.
- Simulation tests control time, messages, partitions, and node crashes.
- Figure 2 catalog tests execute every rule and assert persistence-before-send
  effect ordering.
- Linearizability tests analyze complete concurrent client histories.
- Compatibility tests read every supported durable and protocol version.

## Required tool configurations

- GCC and Clang on Linux
- MSVC on Windows
- AddressSanitizer plus UndefinedBehaviorSanitizer
- ThreadSanitizer in a separate process
- Release and debug builds

No chaos result is accepted unless the random seed and operation history are
preserved for reproduction.
