/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.urbanairship.datacube.serializables.*;
import org.junit.Test;
import org.junit.Assert;

import java.nio.ByteBuffer;

/**
* Test to make sure the serializers do what we expect them to do.
 */
public class SerializerTests {
    public static final byte[] FALSE_CASE = new byte[]{0};
    public static final byte[] TRUE_CASE = new byte[]{1};
    public static final long TEST_LONG = 5L;
    public static final String TEST_STRING = "test";

    @Test
    public void testBooleanSerializable() throws Exception {
        Assert.assertArrayEquals(new BooleanSerializable(true).serialize(), TRUE_CASE);
        Assert.assertArrayEquals(new BooleanSerializable(false).serialize(), FALSE_CASE);
    }

    @Test
    public void testBytesSerializable() throws Exception {
        Assert.assertArrayEquals(
                new BytesSerializable(new byte[]{1}).serialize(), TRUE_CASE);
        Assert.assertArrayEquals(
                new BytesSerializable(new byte[]{0}).serialize(), FALSE_CASE);
    }

    @Test
    public void testStringSerializable() throws Exception {
        Assert.assertArrayEquals(new StringSerializable(TEST_STRING).serialize(),
                TEST_STRING.getBytes());
    }

    @Test
    public void testLongSerializable() throws Exception {
        Assert.assertArrayEquals(new LongSerializable(TEST_LONG).serialize(),
                ByteBuffer.allocate(8).putLong(TEST_LONG).array());
    }

    private enum TestEnum {ZEROTH, FIRST, SECOND}
    @Test
    public void testEnumSerializable() throws Exception {
        Assert.assertArrayEquals(Util.intToBytes(0),
                EnumSerializable.staticSerialize(TestEnum.ZEROTH, 4));

        Assert.assertArrayEquals(new byte[] {0, 0, 0, 1},
                EnumSerializable.staticSerialize(TestEnum.FIRST, 4));
        Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 1},
                EnumSerializable.staticSerialize(TestEnum.FIRST, 5));
        Assert.assertArrayEquals(new byte[] {2},
                EnumSerializable.staticSerialize(TestEnum.SECOND, 1));
        Assert.assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 2},
                EnumSerializable.staticSerialize(TestEnum.SECOND, 9));

        Assert.assertArrayEquals(new byte[] {0, 0, 2},
                new EnumSerializable(TestEnum.SECOND, 3).serialize());

    }

    @Test
    public void testIntSerializable() throws Exception {
        Assert.assertArrayEquals(new byte[] {0, 0, 0, 0},
                IntSerializable.staticSerialize(0));
        Assert.assertArrayEquals(new byte[] {(byte)0xFF, -1, -1, -1},
                IntSerializable.staticSerialize(-1));
        Assert.assertArrayEquals(new byte[] {0x7F, (byte)0xFF, (byte)0xFF, (byte)0xFF},
                IntSerializable.staticSerialize(Integer.MAX_VALUE));
        Assert.assertArrayEquals(new byte[] {(byte)0x80, 0, 0, 0},
                IntSerializable.staticSerialize(Integer.MIN_VALUE));
    }
}
