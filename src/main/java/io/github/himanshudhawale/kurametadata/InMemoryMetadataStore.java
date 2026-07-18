package io.github.himanshudhawale.kurametadata;

import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

public final class InMemoryMetadataStore implements MetadataStore {
    private final NavigableMap<ByteSequence, KeyValue> values = new TreeMap<>();
    private long revision;

    public InMemoryMetadataStore() {
        this(0);
    }

    InMemoryMetadataStore(long initialRevision) {
        if (initialRevision < 0) {
            throw new IllegalArgumentException("initialRevision must not be negative");
        }
        revision = initialRevision;
    }

    @Override
    public synchronized StoreRead get(ByteSequence key) {
        validateKey(key);
        return new StoreRead(Optional.ofNullable(values.get(key)), revision);
    }

    @Override
    public synchronized RangeRead range(
            ByteSequence startInclusive,
            ByteSequence endExclusive) {
        validateKey(startInclusive);
        validateKey(endExclusive);
        if (startInclusive.compareTo(endExclusive) >= 0) {
            throw new IllegalArgumentException(
                    "range start must be less than range end");
        }

        return new RangeRead(
                new ArrayList<>(
                        values.subMap(startInclusive, true, endExclusive, false).values()),
                revision);
    }

    @Override
    public synchronized PutResult put(ByteSequence key, ByteSequence value) {
        validateKey(key);
        Objects.requireNonNull(value, "value");
        return putInternal(key, value);
    }

    @Override
    public synchronized DeleteResult delete(ByteSequence key) {
        validateKey(key);
        KeyValue previous = values.get(key);
        if (previous == null) {
            return new DeleteResult(false, Optional.empty(), revision);
        }

        long mutationRevision = nextRevisionValue();
        values.remove(key);
        revision = mutationRevision;
        return new DeleteResult(true, Optional.of(previous), mutationRevision);
    }

    @Override
    public synchronized CompareAndSetResult compareAndSet(
            ByteSequence key,
            long expectedModRevision,
            ByteSequence newValue) {
        validateKey(key);
        Objects.requireNonNull(newValue, "newValue");
        if (expectedModRevision < 0) {
            throw new IllegalArgumentException(
                    "expectedModRevision must not be negative");
        }

        KeyValue existing = values.get(key);
        boolean matches = existing == null
                ? expectedModRevision == 0
                : existing.modRevision() == expectedModRevision;
        if (!matches) {
            return new CompareAndSetResult(
                    false,
                    Optional.ofNullable(existing),
                    revision);
        }

        PutResult result = putInternal(key, newValue);
        return new CompareAndSetResult(
                true,
                Optional.of(result.current()),
                result.revision());
    }

    @Override
    public synchronized long revision() {
        return revision;
    }

    private PutResult putInternal(ByteSequence key, ByteSequence value) {
        KeyValue previous = values.get(key);
        long mutationRevision = nextRevisionValue();
        long nextVersion = previous == null
                ? 1
                : Math.incrementExact(previous.version());
        KeyValue current = new KeyValue(
                key,
                value,
                nextVersion,
                previous == null ? mutationRevision : previous.createRevision(),
                mutationRevision,
                0);
        values.put(key, current);
        revision = mutationRevision;
        return new PutResult(current, Optional.ofNullable(previous), mutationRevision);
    }

    private long nextRevisionValue() {
        if (revision == Long.MAX_VALUE) {
            throw new IllegalStateException("store revision exhausted");
        }
        return revision + 1;
    }

    private static void validateKey(ByteSequence key) {
        Objects.requireNonNull(key, "key");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
    }
}
