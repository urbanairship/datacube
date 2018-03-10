package com.urbanairship.datacube;

import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class UtilUnitTest {

    @Test
    public void testHashing() {
        byte[] testBytes = {0x00, 0x23, 0x01, 0x05, 0x0e, 0x1e, 0x11, -0x2e};
        byte[] subTestBytes = Arrays.copyOfRange(testBytes, 1, testBytes.length);
        byte[] emptyBytes = Arrays.copyOfRange(testBytes, 0, 0);

        // test that for the full array, we're matching existing hashing results
        assertEquals((byte) Arrays.hashCode(testBytes), Util.hashByteArray(testBytes, 0, testBytes.length));
        // test that for sub arrays, we're still the same
        assertEquals((byte) Arrays.hashCode(subTestBytes), Util.hashByteArray(testBytes, 1, testBytes.length));

        try {
            // it should fail here with an invalid range
            Util.hashByteArray(testBytes, testBytes.length - 2, testBytes.length - 3);
        } catch (IllegalArgumentException e) {
            // should arrive here
        }
        // test when the start and end params are the same, we also get the same hash
        assertEquals((byte) Arrays.hashCode(emptyBytes), Util.hashByteArray(testBytes, 0, 0));
    }
}
