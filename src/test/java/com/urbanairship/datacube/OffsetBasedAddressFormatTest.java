package com.urbanairship.datacube;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.bucketers.BigEndianIntBucketer;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.serializables.LongSerializable;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;


public class OffsetBasedAddressFormatTest {
    OffsetBasedAddressFormat target;


    private List<Dimension<?>> dimensions;

    @Mock
    private IdService idService;
    private Dimension<String> name;
    private Dimension<Integer> id;
    private Dimension<Double> dollars;
    private Dimension<DateTime> time;

    @Test
    public void toKey() throws Exception {
    }

    @Test
    public void fromKey() throws Exception {
    }


    @org.junit.Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        name = new Dimension<>("name", StringToBytesBucketer.getInstance(), true, 4, true);
        id = new Dimension<>("id", new BigEndianIntBucketer(), false, Ints.BYTES, true);
        dollars = new Dimension<>("dollars", new AbstractIdentityBucketer<Double>() {
            @Override
            public CSerializable makeSerializable(Double coord) {
                return new LongSerializable((long) Math.floor(coord));
            }

            @Override
            public Double deserialize(byte[] coord, BucketType bucketType) {
                return (double) LongSerializable.deserialize(coord);
            }
        }, false, Long.BYTES, true);
        time = new Dimension<>("time", new HourDayMonthBucketer(), false, Longs.BYTES, true);


        dimensions = Lists.newArrayList(
                name,
                id,
                dollars,
                time
        );

        target = new OffsetBasedAddressFormat(dimensions, true, new MapIdService(), Address::new);
    }


    @org.junit.Test
    public void test() throws Exception {
        byte[] name = "freckles" .getBytes();

        Optional<byte[]> bytes = target.toKey(
                new Address(true, dimensions).at(this.name, name),
                false);

        int capacity = this.name.getNumFieldBytes() + this.name.getBucketPrefixSize() +
                this.id.getNumFieldBytes() + this.id.getBucketPrefixSize() +
                this.dollars.getNumFieldBytes() + this.dollars.getBucketPrefixSize() +
                this.time.getNumFieldBytes() + this.time.getBucketPrefixSize()
                + 4;// one for each field for whether or not its wildcard.

        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        assertEquals(bytes.get().length,
                1 // to say wildcard or nah
                        + 1 // for the partition byte
                + this.name.getNumFieldBytes() // and then we skip all the rest because the follow name in the key.
        );

    }
}