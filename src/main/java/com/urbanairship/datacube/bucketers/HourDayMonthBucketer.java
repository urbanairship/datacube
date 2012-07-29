/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.List;

import org.joda.time.DateTime;


import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.LongSerializable;

public class HourDayMonthBucketer implements Bucketer<DateTime> {
    public static final BucketType hours = new BucketType("hour", new byte[]{1}); 
    public static final BucketType days = new BucketType("day", new byte[]{2});
    public static final BucketType months = new BucketType("month", new byte[]{3});
    public static final BucketType years = new BucketType("year", new byte[]{4});
    public static final BucketType weeks = new BucketType("weeks", new byte[]{5});

    @Override
    public CSerializable bucketForWrite(DateTime coordinateField, BucketType bucketType) {
        return bucket(coordinateField, bucketType);
    }

    @Override
    public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
        return bucket((DateTime)coordinateField, bucketType);
    }
    
    private CSerializable bucket(DateTime dt, BucketType bucketType) {
        DateTime returnVal;
        if(bucketType == hours) {
            returnVal = hourFloor(dt); 
        } else if(bucketType == days) {
            returnVal = dayFloor(dt);
        } else if(bucketType == months) {
            returnVal = monthFloor(dt);
        } else if(bucketType == years) {
            returnVal = yearFloor(dt);
        } else if(bucketType == weeks) {
            returnVal = weekFloor(dt);
        } else {
            throw new RuntimeException("Unexpected bucket type " + bucketType);
        }
        return new LongSerializable(returnVal.getMillis());
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(hours, days, months, years, weeks);
    }
    
    public static DateTime hourFloor(DateTime dt) {
        return dt.withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0);
    }
    
    public static DateTime dayFloor(DateTime dt) {
        return hourFloor(dt).withHourOfDay(0);
    }
    
    public static DateTime monthFloor(DateTime dt) {
        return dayFloor(dt).withDayOfMonth(1);
    }

    public static DateTime yearFloor(DateTime dt) {
        return monthFloor(dt).withMonthOfYear(1);
    }

    public static DateTime weekFloor(DateTime dt) {
        return dayFloor(dt.minusDays(dt.getDayOfWeek() -1));
    }
}
