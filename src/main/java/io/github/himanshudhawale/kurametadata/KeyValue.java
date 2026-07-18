package io.github.himanshudhawale.kurametadata;

import java.util.Objects;

public record KeyValue(
        ByteSequence key,
        ByteSequence value,
        long version,
        long createRevision,
        long modRevision,
        long leaseId) {

    public KeyValue {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (createRevision < 1 || modRevision < createRevision) {
            throw new IllegalArgumentException("invalid revision values");
        }
        if (leaseId < 0) {
            throw new IllegalArgumentException("leaseId must not be negative");
        }
    }
}
