package io.github.himanshudhawale.kurametadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteSequenceTest {
    @Test
    void copiesInputAndOutputArrays() {
        byte[] input = {1, 2, 3};
        ByteSequence value = ByteSequence.copyOf(input);

        input[0] = 9;
        byte[] output = value.toByteArray();
        output[1] = 9;

        assertArrayEquals(new byte[]{1, 2, 3}, value.toByteArray());
    }

    @Test
    void comparesBytesAsUnsignedValues() {
        ByteSequence high = ByteSequence.copyOf(new byte[]{(byte) 0xff});
        ByteSequence low = ByteSequence.copyOf(new byte[]{0x01});

        assertTrue(high.compareTo(low) > 0);
        assertEquals(ByteSequence.utf8("abc"), ByteSequence.utf8("abc"));
    }
}
