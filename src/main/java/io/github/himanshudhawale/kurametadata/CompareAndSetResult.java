package io.github.himanshudhawale.kurametadata;

import java.util.Objects;
import java.util.Optional;

public record CompareAndSetResult(
        boolean succeeded,
        Optional<KeyValue> current,
        long revision) {

    public CompareAndSetResult {
        Objects.requireNonNull(current, "current");
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
    }
}
