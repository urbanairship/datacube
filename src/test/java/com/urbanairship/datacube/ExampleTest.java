package com.urbanairship.datacube;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.MapDbHarness.BoxedByteArray;

public class ExampleTest {
    
    static final BucketType hours = new BucketType("hour", new byte[]{1}); 
    static final BucketType days = new BucketType("day", new byte[]{2});
    static final BucketType months = new BucketType("month", new byte[]{3});
    
    // Describes how time is bucketed into hours, days, and months 
    static final Bucketer hourDayMonthBucketer = new Bucketer() {
        @Override
        public byte[] getBucket(byte[] timestampBytes, BucketType bucketType) {
            DateTime dt = new DateTime(Util.bytesToLong(timestampBytes));

            DateTime hourBucket = dt.withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0);
            DateTime dayBucket  = hourBucket.withHourOfDay(0);
            DateTime monthBucket = dayBucket.withDayOfMonth(1);

            if(bucketType == hours) {
                return Util.longToBytes(hourBucket.getMillis()); 
            } else if(bucketType == days) {
                return Util.longToBytes(dayBucket.getMillis());
            } else if(bucketType == months) {
                return Util.longToBytes(monthBucket.getMillis());
            } else {
                throw new RuntimeException();
            }
        }

        @Override
        public List<BucketType> getBucketTypes() {
            return ImmutableList.of(hours, days, months);
        }
    };

    static final Dimension time = new Dimension("time", hourDayMonthBucketer, false, 8);; 
    static final Dimension zipcode = new Dimension("zipcode", true, 5);; 

    static Map<BoxedByteArray,byte[]> backingMap = Maps.newHashMap();
    static private DbHarness<LongOp> dbHarness = null;
    static DataCubeIo<LongOp> cube;

    /**
     * Set up the cube.
     */
    @BeforeClass
    public static void beforeClass() {
        List<Dimension> dimensions =  ImmutableList.of(time, zipcode);
        
        DataCube<LongOp> cubeInternal = new DataCube<LongOp>(dimensions);
        dbHarness = new MapDbHarness<LongOp>(dimensions, backingMap, new LongOp.LongOpDeserializer());
        cube = new DataCubeIo<LongOp>(cubeInternal, dbHarness, 1);
    }
    
    /**
     * Clear the cube betweeen tests
     */
    @After
    public void afterTest() {
//        cubeIo.
    }
    
    /**
     * Write and read back a single value, with bucketing.
     */
    @Test
    public void writeAndRead() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        
        // Do an increment of 5 for a certain time and zipcode
        cube.write(new LongOp(5), new WriteBuilder()
                .at(time, Util.longToBytes(now.getMillis()))
                .at(zipcode, "97201".getBytes()).build());
        
        // Do an increment of 10 for the same zipcode in a different hour of the same day
        DateTime differentHour = now.withHourOfDay((now.getHourOfDay()+1)%24);
        cube.write(new LongOp(10), new WriteBuilder()
                .at(time, Util.longToBytes(differentHour.getMillis()))
                .at(zipcode, "97201".getBytes()).build());

        // Read back the value that we wrote for the current hour, should be 5 
        Optional<LongOp> thisHourCount = cube.get(new ReadBuilder()
                .at(time, hours, Util.longToBytes(now.getMillis()))
                .at(zipcode, "97201".getBytes()).build());
        Assert.assertTrue(thisHourCount.isPresent());
        Assert.assertEquals(5L, thisHourCount.get().getValue());
        
        // Read back the value we wrote for the other hour, should be 10
        Optional<LongOp> differentHourCount = cube.get(new ReadBuilder()
                .at(time, hours, Util.longToBytes(differentHour.getMillis()))
                .at(zipcode, "97201".getBytes()).build());
        Assert.assertTrue(differentHourCount.isPresent());
        Assert.assertEquals(10L, differentHourCount.get().getValue());

        // The total for today should be the sum of the two increments
        Optional<LongOp> todayCount = cube.get(new ReadBuilder()
                .at(time, days, Util.longToBytes(now.getMillis()))
                    .at(zipcode, "97201".getBytes()).build());
        Assert.assertTrue(todayCount.isPresent());
        Assert.assertEquals(15L, todayCount.get().getValue());
    }
}
