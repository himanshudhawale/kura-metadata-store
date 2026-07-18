package io.github.himanshudhawale.kurametadata;

import java.util.Objects;
import java.util.Optional;

public record StoreRead(Optional<KeyValue> value, long revision) {
    public StoreRead {
        Objects.requireNonNull(value, "value");
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
    }
}
