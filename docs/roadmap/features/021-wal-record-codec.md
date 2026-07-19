# Feature 021: WAL Record Codec

- **Phase:** 3
- **Problem:** Durable commands need an unambiguous framed format.
- **Deliverable:** Encode version, type, term, index, length, payload, and checksum.
- **Exit criterion:** Golden vectors decode identically across supported platforms.
