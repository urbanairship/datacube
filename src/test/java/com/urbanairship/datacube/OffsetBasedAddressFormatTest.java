package com.urbanairship.datacube;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
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

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class OffsetBasedAddressFormatTest {
    OffsetBasedAddressFormat target;


    private List<Dimension<?, ?>> dimensions;

    private Dimension<String, String> name;
    private Dimension<Integer, Integer> id;
    private Dimension<Double, Long> dollars;
    private Dimension<DateTime, DateTime> time;

    BucketType ones = new BucketType("ones precision", new byte[]{0});
    BucketType tens = new BucketType("tens precision", new byte[]{1});


    @org.junit.Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        name = new Dimension<>("name", StringToBytesBucketer.getInstance(), true, 4);
        id = new Dimension<>("id", new BigEndianIntBucketer(), false, Ints.BYTES);
        dollars = new Dimension<>("dollars", new Bucketer<Double, Long>() {
            @Override
            public SetMultimap<BucketType, CSerializable> bucketForWrite(Double coordinate) {
                // write 11.32
                HashMultimap<BucketType, CSerializable> m = HashMultimap.create();
                m.put(ones, new LongSerializable((long) Math.floor(coordinate)));
                m.put(tens, new LongSerializable((long) Math.floor(coordinate / 10)));
                return m;
            }

            @Override
            public CSerializable bucketForRead(Long coordinate, BucketType bucketType) {
                // read 1 at tens precision => [0.0, 10.0)
                // read 14 at tens precision => [10.0, 20.0)
                // read 1 at 1s precision => [1.0, 2.0)
                return new LongSerializable(coordinate);
            }

            @Override
            public Long deserialize(byte[] coord) {
                return LongSerializable.deserialize(coord);
            }

            @Override
            public List<BucketType> getBucketTypes() {
                return ImmutableList.of(ones, tens);
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
                .at(this.dollars, ones,11L)
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