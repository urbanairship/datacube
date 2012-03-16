package com.urbanairship.datacube;

import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;

public class HourDayMonthBucketer implements Bucketer<DateTime> {
    static final BucketType hours = new BucketType("hour", new byte[]{1}); 
    static final BucketType days = new BucketType("day", new byte[]{2});
    static final BucketType months = new BucketType("month", new byte[]{3});
    
    @Override
    public CSerializable bucketForWrite(DateTime dt, BucketType bucketType) {
        return bucket(dt, bucketType);
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
        } else {
            throw new RuntimeException("Unexpected bucket type " + bucketType);
        }
        return new LongSerializable(returnVal.getMillis());
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(hours, days, months);
    }

//    @Override
//    public Class<DateTime> getInputFieldClass() {
//        return DateTime.class;
//    }
    
    public static DateTime hourFloor(DateTime dt) {
        return dt.withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0);
    }
    
    public static DateTime dayFloor(DateTime dt) {
        return hourFloor(dt).withHourOfDay(0);
    }
    
    public static DateTime monthFloor(DateTime dt) {
        return dayFloor(dt).withDayOfMonth(1);
    }
}
