package io.github.himanshudhawale.kurametadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class InMemoryMetadataStoreTest {
    private static final ByteSequence KEY = ByteSequence.utf8("/kura/table/current");

    @Test
    void putCreatesAndUpdatesAKeyGeneration() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();

        PutResult created = store.put(KEY, ByteSequence.utf8("snapshot-1"));
        PutResult updated = store.put(KEY, ByteSequence.utf8("snapshot-2"));

        assertEquals(1, created.current().version());
        assertEquals(1, created.current().createRevision());
        assertEquals(1, created.current().modRevision());
        assertTrue(created.previous().isEmpty());

        assertEquals(2, updated.current().version());
        assertEquals(1, updated.current().createRevision());
        assertEquals(2, updated.current().modRevision());
        assertEquals(created.current(), updated.previous().orElseThrow());
        assertEquals(2, store.revision());
    }

    @Test
    void deleteNoOpDoesNotAdvanceRevisionAndRecreateStartsNewGeneration() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.put(KEY, ByteSequence.utf8("snapshot-1"));

        DeleteResult deleted = store.delete(KEY);
        DeleteResult absent = store.delete(KEY);
        PutResult recreated = store.put(KEY, ByteSequence.utf8("snapshot-2"));

        assertTrue(deleted.deleted());
        assertEquals(2, deleted.revision());
        assertFalse(absent.deleted());
        assertEquals(2, absent.revision());
        assertEquals(1, recreated.current().version());
        assertEquals(3, recreated.current().createRevision());
        assertEquals(3, recreated.current().modRevision());
    }

    @Test
    void compareAndSetUsesModificationRevision() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        PutResult initial = store.put(KEY, ByteSequence.utf8("snapshot-1"));

        CompareAndSetResult failed = store.compareAndSet(
                KEY,
                initial.revision() + 1,
                ByteSequence.utf8("wrong"));
        CompareAndSetResult succeeded = store.compareAndSet(
                KEY,
                initial.revision(),
                ByteSequence.utf8("snapshot-2"));

        assertFalse(failed.succeeded());
        assertEquals(1, failed.revision());
        assertEquals("snapshot-1", failed.current().orElseThrow().value().toUtf8());

        assertTrue(succeeded.succeeded());
        assertEquals(2, succeeded.revision());
        assertEquals("snapshot-2", succeeded.current().orElseThrow().value().toUtf8());
    }

    @Test
    void compareAndSetRevisionZeroCreatesOnlyWhenAbsent() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();

        assertTrue(store.compareAndSet(
                KEY,
                0,
                ByteSequence.utf8("snapshot-1")).succeeded());
        assertFalse(store.compareAndSet(
                KEY,
                0,
                ByteSequence.utf8("snapshot-2")).succeeded());
        assertEquals(1, store.revision());
    }

    @Test
    void rangeUsesUnsignedLexicographicOrderAndOneReadRevision() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.put(ByteSequence.utf8("b"), ByteSequence.utf8("2"));
        store.put(ByteSequence.utf8("a"), ByteSequence.utf8("1"));
        store.put(ByteSequence.utf8("c"), ByteSequence.utf8("3"));

        RangeRead result = store.range(ByteSequence.utf8("a"), ByteSequence.utf8("c"));

        assertEquals(3, result.revision());
        assertEquals(
                List.of("a", "b"),
                result.values().stream().map(value -> value.key().toUtf8()).toList());
    }

    @Test
    void rejectsEmptyKeysAndInvalidRanges() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();

        assertThrows(
                IllegalArgumentException.class,
                () -> store.get(ByteSequence.copyOf(new byte[0])));
        assertThrows(
                IllegalArgumentException.class,
                () -> store.range(ByteSequence.utf8("z"), ByteSequence.utf8("a")));
    }

    @Test
    void concurrentCompareAndSetHasOneWinner() throws Exception {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        long expectedRevision = store.put(KEY, ByteSequence.utf8("snapshot-1")).revision();
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<CompareAndSetResult> first = executor.submit(() -> {
                start.await();
                return store.compareAndSet(
                        KEY,
                        expectedRevision,
                        ByteSequence.utf8("snapshot-2a"));
            });
            Future<CompareAndSetResult> second = executor.submit(() -> {
                start.await();
                return store.compareAndSet(
                        KEY,
                        expectedRevision,
                        ByteSequence.utf8("snapshot-2b"));
            });

            start.countDown();
            long winners = List.of(first.get(), second.get()).stream()
                    .filter(CompareAndSetResult::succeeded)
                    .count();

            assertEquals(1, winners);
            assertEquals(2, store.revision());
            assertEquals(2, store.get(KEY).value().orElseThrow().version());
        }
    }

    @Test
    void revisionExhaustionDoesNotPartiallyMutateState() {
        InMemoryMetadataStore store = new InMemoryMetadataStore(Long.MAX_VALUE - 1);
        store.put(KEY, ByteSequence.utf8("snapshot-1"));

        assertThrows(IllegalStateException.class, () -> store.delete(KEY));
        assertEquals(
                "snapshot-1",
                store.get(KEY).value().orElseThrow().value().toUtf8());

        assertThrows(
                IllegalStateException.class,
                () -> store.put(KEY, ByteSequence.utf8("snapshot-2")));
        assertEquals(
                "snapshot-1",
                store.get(KEY).value().orElseThrow().value().toUtf8());
        assertEquals(Long.MAX_VALUE, store.revision());
    }
}
