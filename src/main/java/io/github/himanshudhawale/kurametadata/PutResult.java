package io.github.himanshudhawale.kurametadata;

import java.util.Objects;
import java.util.Optional;

public record PutResult(
        KeyValue current,
        Optional<KeyValue> previous,
        long revision) {

    public PutResult {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(previous, "previous");
        if (revision != current.modRevision()) {
            throw new IllegalArgumentException("result revision must match current value");
        }
    }
}
