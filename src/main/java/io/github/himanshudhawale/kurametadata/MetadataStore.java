package io.github.himanshudhawale.kurametadata;

public interface MetadataStore {
    StoreRead get(ByteSequence key);

    RangeRead range(ByteSequence startInclusive, ByteSequence endExclusive);

    PutResult put(ByteSequence key, ByteSequence value);

    DeleteResult delete(ByteSequence key);

    CompareAndSetResult compareAndSet(
            ByteSequence key,
            long expectedModRevision,
            ByteSequence newValue);

    long revision();
}
