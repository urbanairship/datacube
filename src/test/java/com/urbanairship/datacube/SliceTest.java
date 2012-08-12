/**
 * Copyright (C) 2012 Neofonie GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.urbanairship.datacube;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * This test is intended for specification of a possible
 * mechanism for preaggregating SLICEs of the mapCube.
 * See {@link Slice} for a longer explanation.
 */
public class SliceTest extends EmbeddedClusterTestAbstract {

    private static final byte[] cf = Bytes.toBytes("d");
    private static final byte[] table = Bytes.toBytes("slice_name");

    private final static String[] colors = {"red", "yellow", "green"};
    private final static String[] sizes = {"small", "medium", "big"};

    /**
     * those timestamps should lie within one month
     */
    final Long[] times = {526946400000L, 527032800000L, 527119200000L};


    private static final long NUM_APPLES = 10L;
    private DataCubeIo<LongOp> mapCubeIo;
    private Dimension<String> colorDimension;
    private Dimension<String> sizeDimension;
    private DataCube<LongOp> mapCube;
    private ConcurrentMap<BoxedByteArray,byte[]> backingMap;
    private Dimension<DateTime> timeDimension;
    private DataCube<LongOp> hbaseCube;
    private DataCubeIo<LongOp> hbaseCubeIo;

    @BeforeClass
    public static void init() throws Exception {
        getTestUtil().createTable(table, cf).close();
    }

    @Before
    public void setUp() throws Exception {
        colorDimension = new Dimension<String>("colorDimension", new StringToBytesBucketer(), true,6);
        sizeDimension = new Dimension<String>("sizeDimension", new StringToBytesBucketer(), true, 6);
        timeDimension = new Dimension<DateTime>("time", new HourDayMonthBucketer(), true, 4);

        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(colorDimension, sizeDimension, timeDimension);

        Rollup daysRollup = new Rollup(colorDimension, BucketType.IDENTITY, sizeDimension, BucketType.IDENTITY,
            timeDimension, HourDayMonthBucketer.days);
        Rollup monthRollup = new Rollup(colorDimension, BucketType.IDENTITY, sizeDimension, BucketType.IDENTITY,
            timeDimension, HourDayMonthBucketer.months);

        List<Rollup> rollups = ImmutableList.of(daysRollup, monthRollup);

        Set<DimensionAndBucketType> sizeTimeDaysDimBuckets = new HashSet<DimensionAndBucketType>();
        sizeTimeDaysDimBuckets.add(new DimensionAndBucketType(sizeDimension, BucketType.IDENTITY));
        sizeTimeDaysDimBuckets.add(new DimensionAndBucketType(timeDimension, HourDayMonthBucketer.days));

        Slice sliceColorDays = new Slice(colorDimension, sizeTimeDaysDimBuckets); // first dimension given is the "slice dimension"

        Set<DimensionAndBucketType> sizeTimeMonthsDimBuckets = new HashSet<DimensionAndBucketType>();
        sizeTimeMonthsDimBuckets.add(new DimensionAndBucketType(sizeDimension, BucketType.IDENTITY));
        sizeTimeMonthsDimBuckets.add(new DimensionAndBucketType(timeDimension, HourDayMonthBucketer.months));

        Slice sliceColorMonths = new Slice(colorDimension, sizeTimeMonthsDimBuckets); // first dimension given is the "slice dimension"
        ImmutableList<Slice> slices = ImmutableList.of(sliceColorDays, sliceColorMonths);

        backingMap = Maps.newConcurrentMap();
        // TODO: should the Slice-wildcard value be somehow registered with the id service?
        IdService idService = new MapIdService();
        DbHarness<LongOp> mapDbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER,
            DbHarness.CommitType.READ_COMBINE_CAS, idService);

        mapCube = new DataCube<LongOp>(dimensions, rollups, slices);
        mapCubeIo = new DataCubeIo<LongOp>(mapCube, mapDbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);


        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(pool, Bytes.toBytes("slice_cube"),
                table, cf, LongOp.DESERIALIZER, idService,
                DbHarness.CommitType.INCREMENT);

        hbaseCube = new DataCube<LongOp>(dimensions, rollups, slices);
        hbaseCubeIo = new DataCubeIo<LongOp>(mapCube, hbaseDbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        // Fill the mapCube with testdata:
        // Every day, 10 of each kind
        generateApples(NUM_APPLES, times, colors, sizes, mapCubeIo, mapCube);
        generateApples(NUM_APPLES, times, colors, sizes, hbaseCubeIo, hbaseCube);
    }

    private void generateApples(long num, Long[] times, String[] colors, String[] sizes, DataCubeIo<LongOp> cubeIo, DataCube<LongOp> cube)
        throws IOException, InterruptedException {

        for(Long timestamp : times) {
            DateTime date = new DateTime(timestamp);

            for(String color : colors) {
                for(String size : sizes) {
                    for(int i = 0; i < num; i++) {
                        cubeIo.writeSync(new LongOp(1), new WriteBuilder(cube)
                            .at(this.colorDimension, color)
                            .at(this.sizeDimension, size)
                            .at(this.timeDimension, date));
                    }
                }
            }
        }
    }

    @Test
    public void testSimpleInMemSlice() throws IOException, InterruptedException {

        Optional<Map<String,LongOp>> results = mapCubeIo.getSlice(new ReadBuilder(mapCube)
            .sliceFor(colorDimension)
            .at(sizeDimension, sizes[0])
            .at(timeDimension, HourDayMonthBucketer.days, new DateTime(times[0])), new StringToBytesBucketer(), BucketType.IDENTITY);

        verifyAppleSlice(results);
    }


    @Test
    public void testHbaseSimpleSlice() throws IOException, InterruptedException {

        Optional<Map<String,LongOp>> results = hbaseCubeIo.getSlice(new ReadBuilder(hbaseCube)
            .sliceFor(colorDimension)
            .at(sizeDimension, sizes[0])
            .at(timeDimension, HourDayMonthBucketer.days, new DateTime(times[0])), new StringToBytesBucketer(), BucketType.IDENTITY);

        verifyAppleSlice(results);
    }

    private void verifyAppleSlice(Optional<Map<String, LongOp>> results) {
        Assert.assertTrue(results.isPresent());
        Map<String,LongOp> sliceMap = results.get();

        Assert.assertEquals(sliceMap.size(), 3);

        Assert.assertEquals(NUM_APPLES, sliceMap.get("red").getLong());
        Assert.assertEquals(NUM_APPLES, sliceMap.get("green").getLong());
        Assert.assertEquals(NUM_APPLES, sliceMap.get("yellow").getLong());

        results = mapCubeIo.getSlice(new ReadBuilder(mapCube)
            .sliceFor(colorDimension)
            .at(sizeDimension, sizes[0])
            .at(timeDimension, HourDayMonthBucketer.months, new DateTime(times[0])),
            new StringToBytesBucketer(), BucketType.IDENTITY);

        Assert.assertTrue(results.isPresent());
        sliceMap = results.get();

        Long appleMonthCnt = NUM_APPLES*times.length;
        Assert.assertEquals(appleMonthCnt, (Long)sliceMap.get("red").getLong());
        Assert.assertEquals(appleMonthCnt, (Long)sliceMap.get("green").getLong());
        Assert.assertEquals(appleMonthCnt, (Long)sliceMap.get("yellow").getLong());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDirectSliceWrite() throws IOException, InterruptedException {
        mapCubeIo.writeSync(new LongOp(1), new WriteBuilder(mapCube)
            .at(colorDimension, Slice.getWildcardValue())
            .at(sizeDimension, sizes[0])
            .at(timeDimension, new DateTime(times[0])));
    }

    @Test
    public void testHbaseNonExistingSlice() {
        Optional<Map<String, LongOp>> results = hbaseCubeIo.getSlice(new ReadBuilder(mapCube)
            .sliceFor(sizeDimension)
            .at(colorDimension, colors[0])
            .at(timeDimension, HourDayMonthBucketer.months, new DateTime(times[0])),
            new StringToBytesBucketer(), BucketType.IDENTITY);

        Assert.assertFalse(results.isPresent());
    }
}
