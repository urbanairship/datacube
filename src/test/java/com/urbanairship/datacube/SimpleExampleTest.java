package com.urbanairship.datacube;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.MapDbHarness.BoxedByteArray;

public class SimpleExampleTest {
    // Describes how time is bucketed into hours, days, and months 
    private static final HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

    private static final Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
    private static final Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), 
            true, 5);
//    static final Dimension<CSerializable> zipcode = Dimension.newUnBucketedDimension("zipcode", true, 5);

    private static ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
            new ConcurrentHashMap<BoxedByteArray,byte[]>();
    private static DbHarness<LongOp> dbHarness = null;
    private static DataCubeIo<LongOp> cubeIo = null;
    private static DataCube<LongOp> cube;
    
    private static Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
    private static Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
    private static Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
    private static Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);
    
    /**
     * Set up the cubeIo.
     */
    @BeforeClass
    public static void beforeClass() {
        List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(time, zipcode);
        List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
                dayRollup);
        
        cube = new DataCube<LongOp>(dimensions, rollups);
        dbHarness = new MapDbHarness<LongOp>(dimensions, backingMap, LongOp.DESERIALIZER, 
                CommitType.READ_COMBINE_CAS, 3);
        cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1);
    }
    
    /**
     * Write and read back a single value, with bucketing.
     */
    @Test
    public void writeAndRead() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        
        // Do an increment of 5 for a certain time and zipcode
        cubeIo.write(new LongOp(5), new WriteAddressBuilder(cube)
                .at(time, now)
                .at(zipcode, "97201").build());
        
        // Do an increment of 10 for the same zipcode in a different hour of the same day
        DateTime differentHour = now.withHourOfDay((now.getHourOfDay()+1)%24);
        cubeIo.write(new LongOp(10), new WriteAddressBuilder(cube)
                .at(time, differentHour)
                .at(zipcode, "97201").build());

        System.err.println("Done inserting");
        
        // Read back the value that we wrote for the current hour, should be 5 
        Optional<LongOp> thisHourCount = cubeIo.get(new ReadAddressBuilder()
                .at(time, HourDayMonthBucketer.hours, now)
                .at(zipcode, "97201").build());
        Assert.assertTrue(thisHourCount.isPresent());
        Assert.assertEquals(5L, thisHourCount.get().getValue());
        
        // Read back the value we wrote for the other hour, should be 10
        Optional<LongOp> differentHourCount = cubeIo.get(new ReadAddressBuilder()
                .at(time, HourDayMonthBucketer.hours, differentHour)
                .at(zipcode, "97201").build());
        Assert.assertTrue(differentHourCount.isPresent());
        Assert.assertEquals(10L, differentHourCount.get().getValue());

        // The total for today should be the sum of the two increments
        Optional<LongOp> todayCount = cubeIo.get(new ReadAddressBuilder()
                .at(time, HourDayMonthBucketer.days, now)
                .at(zipcode, "97201").build());
        Assert.assertTrue(todayCount.isPresent());
        Assert.assertEquals(15L, todayCount.get().getValue());
    }
}
