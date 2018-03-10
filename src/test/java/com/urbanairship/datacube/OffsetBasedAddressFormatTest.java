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
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class OffsetBasedAddressFormatTest {
    OffsetBasedAddressFormat target;


    private List<Dimension<?>> dimensions;

    private Dimension<String> name;
    private Dimension<Integer> id;
    private Dimension<Double> dollars;
    private Dimension<DateTime> time;


    @org.junit.Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        name = new Dimension<>("name", StringToBytesBucketer.getInstance(), true, 4);
        id = new Dimension<>("id", new BigEndianIntBucketer(), false, Ints.BYTES);
        dollars = new Dimension<>("dollars", new AbstractIdentityBucketer<Double>() {
            @Override
            public CSerializable makeSerializable(Double coord) {
                return new LongSerializable((long) Math.floor(coord));
            }

            @Override
            public Double deserialize(byte[] coord, BucketType bucketType) {
                return (double) LongSerializable.deserialize(coord);
            }
        }, false, Long.BYTES);
        time = new Dimension<>("time", new HourDayMonthBucketer(), false, Longs.BYTES);

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
        String name = "freckles";

        Address nameOnly = new ReadBuilder(true, dimensions).at(this.name, name).build();
        Address idOnly = new ReadBuilder(true, dimensions).at(this.id, 1).build();
        Address allDimensionsNoHash = new ReadBuilder(true, dimensions)
                .at(this.name, "roger")
                .at(this.id, 10)
                .at(this.dollars, 11.11)
                .at(this.time, HourDayMonthBucketer.days, new DateTime())
                .build();


        Optional<byte[]> nameKey = target.toKey(
                nameOnly,
                false);

        Optional<byte[]> nameKeyForReading = target.toKey(
                nameOnly,
                true);

        Optional<byte[]> keyId = target.toKey(idOnly, true);
        Optional<byte[]> failedIdLookup = target.toKey(allDimensionsNoHash, true);
        assertFalse(failedIdLookup.isPresent());
        Optional<byte[]> allDimNoHashKey = target.toKey(allDimensionsNoHash, false);

        int capacity = this.name.getNumFieldBytes() + this.name.getBucketPrefixSize() +
                this.id.getNumFieldBytes() + this.id.getBucketPrefixSize() +
                this.dollars.getNumFieldBytes() + this.dollars.getBucketPrefixSize() +
                this.time.getNumFieldBytes() + this.time.getBucketPrefixSize()
                + 1 // partition byte;
                + 4;// one for each field for whether or not its wildcard.

        assertEquals(capacity, allDimNoHashKey.get().length);


        assertEquals(nameKey.get().length,
                1 // to say wildcard or nah
                        + 1 // for the partition byte
                + this.name.getNumFieldBytes() // and then we skip all the rest because the follow name in the key.
                        + this.name.getBucketPrefixSize() // should be zero cause we're wildcard.
        );

        assertEquals(keyId.get().length,
                1 // partition byte
                + 1 // to say wildcard or nah
                        + this.name.getNumFieldBytes()
                        + this.name.getBucketPrefixSize()
                        + 1 // to say wildcard or nah
                        + this.id.getNumFieldBytes() // and then we skip all the rest because the follow name in the key.
                        + this.id.getBucketPrefixSize() // and then we skip all the rest because the follow name in the key.
        );

        assertEquals(nameOnly, target.fromKey(nameKey.get()).get());
        assertEquals(nameOnly, target.fromKey(nameKeyForReading.get()).get());
        assertEquals(idOnly, target.fromKey(keyId.get()).get());
        assertEquals(allDimensionsNoHash, target.fromKey(allDimNoHashKey.get()).get());
    }
}