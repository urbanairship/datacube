package com.urbanairship.datacube;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.MapDbHarness.BoxedByteArray;

public class ExampleTest {
    
    @Test
    public void exampleTest() throws Exception {
        final BucketType hourBucketType = new BucketType("hour"); 
        final BucketType dayBucketType = new BucketType("day");
        final BucketType monthBucketType = new BucketType("month");
        
        List<BucketType> timeBuckets = ImmutableList.of(hourBucketType, dayBucketType, monthBucketType);

        // Describes how time is bucketed into hours, days, and months 
        Bucketer timeBucketer = new Bucketer() {
            @Override
            public Map<BucketType,byte[]> getBuckets(Object c) {
                DateTime dt = new DateTime((Long)c);
                DateTime hourBucket = dt.withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0);
                DateTime dayBucket  = hourBucket.withHourOfDay(0);
                DateTime monthBucket = dayBucket.withDayOfMonth(1);
                return ImmutableMap.of(hourBucketType, Util.longToBytes(hourBucket.getMillis()),
                        dayBucketType, Util.longToBytes(dayBucket.getMillis()),
                        monthBucketType, Util.longToBytes(monthBucket.getMillis()));
            }
        };
        
        // Define the cube dimensions and instantiate it
        Dimension time = new Dimension("time", timeBuckets, timeBucketer, false, 8); 
        Dimension zipcode = new Dimension("zipcode", true, 5); 
        DataCube<LongOp> cube = new DataCube<LongOp>(ImmutableList.of(time, zipcode));
        
        // Get an exploded batch of cube mutations to do an increment of 5
        long now = System.currentTimeMillis();
        Coords c = cube.newCoords().set(time, Util.longToBytes(now))
                .set(zipcode, "97201".getBytes());
        Batch<LongOp> batch = cube.getBatch(c, new LongOp(5));
        
        // Apply the mutation batch to the backing storage
        Map<BoxedByteArray,byte[]> backingMap = new HashMap<BoxedByteArray,byte[]>();
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap,
                new LongOp.LongOpDeserializer());
        dbHarness.runBatch(batch);
        
        // Read back the value that we wrote
        Coords queryAt = cube.newCoords().set(time, Util.longToBytes(now))
                .set(zipcode, "97201".getBytes());
        Optional<LongOp> ctrVal = dbHarness.get(queryAt);
        Assert.assertTrue(ctrVal.isPresent());
        Assert.assertEquals(5L, ctrVal.get().getValue());
    }
}
