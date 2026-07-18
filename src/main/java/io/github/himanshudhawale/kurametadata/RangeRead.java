package io.github.himanshudhawale.kurametadata;

import java.util.List;
import java.util.Objects;

public record RangeRead(List<KeyValue> values, long revision) {
    public RangeRead {
        Objects.requireNonNull(values, "values");
        values = List.copyOf(values);
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
    }
}
