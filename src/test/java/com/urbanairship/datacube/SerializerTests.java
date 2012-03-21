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


    /**
     * Test Boolean Serializable Class
     * @throws Exception
     */
    @Test
    public void testBooleanSerializable() throws Exception {
        Assert.assertArrayEquals(new BooleanSerializable(true).serialize(), TRUE_CASE);
        Assert.assertArrayEquals(new BooleanSerializable(false).serialize(), FALSE_CASE);
    }

    /**
     * Test BytesSerializable Class
     * @throws Exception
     */

    @Test
    public void testBytesSerializable() throws Exception {
        Assert.assertArrayEquals(
                new BytesSerializable(new byte[]{1}).serialize(), TRUE_CASE);
        Assert.assertArrayEquals(
                new BytesSerializable(new byte[]{0}).serialize(), FALSE_CASE);
    }

    /**
     * Test StringSerializable Class
     * @throws Exception
     */
    @Test
    public void testStringSerializable() throws Exception {
        Assert.assertArrayEquals(new StringSerializable(TEST_STRING).serialize(),
                TEST_STRING.getBytes());
    }

    /**
     * Test LongSerializable (mostly through re-implementation).
     * @throws Exception
     */
    @Test
    public void testLongSerializable() throws Exception {

        Assert.assertArrayEquals(new LongSerializable(TEST_LONG).serialize(),
                ByteBuffer.allocate(8).putLong(TEST_LONG).array());
    }
}
