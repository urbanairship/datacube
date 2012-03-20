package com.urbanairship.datacube;

import java.util.List;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.ops.LongOp;

public class DbHarnessTests {
    /**
     * To do a basic test of DbHarness implementation, you can instantiate a cube and pass it
     * to this function. It will do some gets and sets and throw junit assertions if anything
     * isn't behaving correctly. 
     */
    public static void basicTest(DbHarness<LongOp> dbHarness) throws Exception {
        HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

        Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
        Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), 
                true, 5);
        
        DataCubeIo<LongOp> cubeIo = null;
        DataCube<LongOp> cube;
        
        Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
        Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);
        
        List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(time, zipcode);
        List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
                dayRollup);
        
        cube = new DataCube<LongOp>(dimensions, rollups);

        cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1);
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        
        // Do an increment of 5 for a certain time and zipcode
        cubeIo.write(new LongOp(5), new WriteBuilder(cube)
                .at(time, now)
                .at(zipcode, "97201"));
        
        // Do an increment of 10 for the same zipcode in a different hour of the same day
        DateTime differentHour = now.withHourOfDay((now.getHourOfDay()+1)%24);
        cubeIo.write(new LongOp(10), new WriteBuilder(cube)
                .at(time, differentHour)
                .at(zipcode, "97201"));

        System.err.println("Done inserting");
        
        // Read back the value that we wrote for the current hour, should be 5 
        Optional<LongOp> thisHourCount = cubeIo.get(new ReadAddressBuilder(cube)
                .at(time, HourDayMonthBucketer.hours, now)
                .at(zipcode, "97201"));
        Assert.assertTrue(thisHourCount.isPresent());
        Assert.assertEquals(5L, thisHourCount.get().getValue());
        
        // Read back the value we wrote for the other hour, should be 10
        Optional<LongOp> differentHourCount = cubeIo.get(new ReadAddressBuilder(cube)
                .at(time, HourDayMonthBucketer.hours, differentHour)
                .at(zipcode, "97201"));
        Assert.assertTrue(differentHourCount.isPresent());
        Assert.assertEquals(10L, differentHourCount.get().getValue());

        // The total for today should be the sum of the two increments
        Optional<LongOp> todayCount = cubeIo.get(new ReadAddressBuilder(cube)
                .at(time, HourDayMonthBucketer.days, now)
                .at(zipcode, "97201"));
        Assert.assertTrue(todayCount.isPresent());
        Assert.assertEquals(15L, todayCount.get().getValue());
    }
}
