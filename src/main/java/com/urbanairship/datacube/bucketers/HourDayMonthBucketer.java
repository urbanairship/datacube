/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
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

    static final Set<BucketType> ALL_BUCKET_TYPES = ImmutableSet.<BucketType>builder().
            add(hours).add(days).add(months).add(years).add(weeks).build();
    
    private final List<BucketType> bucketTypes;
    
    // TODO move this class to TimeBucker and have HourDayMonthBucketer be a subclass that
    // just calls super(ImmutableList.of(hours, days, months))
    public HourDayMonthBucketer() {
        this(ImmutableList.of(hours, days, months));
    }
    
    public HourDayMonthBucketer(List<BucketType> bucketTypes) {
        this.bucketTypes = ImmutableList.copyOf(bucketTypes); // defensive copy
        for(BucketType bucketType: bucketTypes) {
            if(!ALL_BUCKET_TYPES.contains(bucketType)) {
                throw new IllegalArgumentException("Invalid bucket type " + bucketType + 
                        ", expected one of " + ALL_BUCKET_TYPES);
            }
        }
    }
    
    @Override
    public SetMultimap<BucketType, CSerializable> bucketForWrite(
            DateTime coordinate) {
        ImmutableSetMultimap.Builder<BucketType,CSerializable> builder = 
                ImmutableSetMultimap.builder();
        for(BucketType bucketType: bucketTypes) {
            builder.put(bucketType, bucket(coordinate, bucketType));
        }
        return builder.build();
    }

    @Override
    public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
        DateTime inputTime = (DateTime)coordinateField;
        return bucket(inputTime, bucketType);
    }
    
    private CSerializable bucket(DateTime inputTime, BucketType bucketType) {
        DateTime returnVal;
        if(bucketType == hours) {
            returnVal = hourFloor(inputTime); 
        } else if(bucketType == days) {
            returnVal = dayFloor(inputTime);
        } else if(bucketType == months) {
            returnVal = monthFloor(inputTime);
        } else if(bucketType == years) {
            returnVal = yearFloor(inputTime);
        } else if(bucketType == weeks) {
            returnVal = weekFloor(inputTime);
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
