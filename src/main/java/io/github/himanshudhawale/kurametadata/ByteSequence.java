package io.github.himanshudhawale.kurametadata;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

public final class ByteSequence implements Comparable<ByteSequence> {
    private final byte[] bytes;

    private ByteSequence(byte[] bytes) {
        this.bytes = bytes;
    }

    public static ByteSequence copyOf(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return new ByteSequence(Arrays.copyOf(bytes, bytes.length));
    }

    public static ByteSequence utf8(String value) {
        Objects.requireNonNull(value, "value");
        return new ByteSequence(value.getBytes(StandardCharsets.UTF_8));
    }

    public int size() {
        return bytes.length;
    }

    public boolean isEmpty() {
        return bytes.length == 0;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public String toUtf8() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String toHex() {
        return HexFormat.of().formatHex(bytes);
    }

    @Override
    public int compareTo(ByteSequence other) {
        Objects.requireNonNull(other, "other");
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof ByteSequence that
                && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "0x" + toHex();
    }
}
