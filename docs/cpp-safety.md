# C++ Safety Rules

Kura uses native code for control and predictable latency, not permission to
accept undefined behavior.

1. Resources use RAII.
2. Owning raw pointers are prohibited.
3. Byte input is copied or tied to an explicit lifetime.
4. Signed protocol counters use checked arithmetic.
5. Threads share state only behind documented synchronization.
6. Exceptions never cross process, C ABI, or plugin boundaries.
7. Parsers validate lengths before allocation or access.
8. Persistent input is checksummed and versioned.
9. AddressSanitizer, UBSan, and ThreadSanitizer run independently.
10. Fuzzers target protocol, WAL, snapshot, and decompression boundaries.

Safety findings block release regardless of benchmark improvement.
