package io.github.himanshudhawale.kurametadata;

import java.util.Objects;
import java.util.Optional;

public record DeleteResult(
        boolean deleted,
        Optional<KeyValue> previous,
        long revision) {

    public DeleteResult {
        Objects.requireNonNull(previous, "previous");
        if (deleted != previous.isPresent()) {
            throw new IllegalArgumentException("deleted and previous must agree");
        }
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
    }
}
