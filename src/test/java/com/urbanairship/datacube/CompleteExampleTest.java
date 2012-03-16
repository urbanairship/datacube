package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.MapDbHarness.BoxedByteArray;

/**
 * An example of using all the DataCube features at the same time.
 */
public class CompleteExampleTest {
    /**
     * We're going to count mobile devices by type, location, and timestamp.
     */

    enum DeviceType {IPHONE, IPAD, HTC_MYTOUCH, SAMSUNG_GALAXY}
    enum City {PORTLAND, SALEM, SANFRANCISCO, SACRAMENTO, OLYMPIA, SEATTLE}

    enum OsManufacturer {ANDROID, APPLE}
    enum UsState {OREGON, CALIFORNIA, WASHINGTON}
    
    static class MobileCountCube {
        Bucketer<DateTime> timeBucketer = new HourDayMonthBucketer();
    
        static class LocationBucketer implements Bucketer<City> {
            static BucketType usState = new BucketType("us_state", new byte[]{1});
            static BucketType usCity = new BucketType("city", new byte[]{2});
            
            @Override
            public CSerializable bucketForWrite(City city, BucketType bucketType) {
                if(bucketType == usState) {
                    switch(city) {
                    case PORTLAND:
                    case SALEM:
                        // These cities are in Oregon
                        return new EnumSerializable(UsState.OREGON, 4);
                    case SANFRANCISCO:
                    case SACRAMENTO:
                        // These cities are in California
                        return new EnumSerializable(UsState.CALIFORNIA, 4);
                    case OLYMPIA:
                    case SEATTLE:
                        // These cities are in Washington
                        return new EnumSerializable(UsState.WASHINGTON, 4);
                    default:
                        throw new RuntimeException("Unknown city " + city);
                    }
                } else if(bucketType == usCity) {
                    return new EnumSerializable(city, 4);
                } else {
                    throw new RuntimeException("Unknown bucket type " + bucketType);
                }
            }

            @Override
            public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
                if(coordinateField instanceof City) {
                    return new EnumSerializable((City)coordinateField, 4);
                } else if(coordinateField instanceof UsState) {
                    return new EnumSerializable((UsState)coordinateField, 4);
                } else {
                    throw new RuntimeException("Unrecognized object of type " + 
                            coordinateField.getClass().getName());
                }
            }

            @Override
            public List<BucketType> getBucketTypes() {
                return ImmutableList.of(usState, usCity);
            }
        };
        
        public static class DeviceBucketer implements Bucketer<DeviceType> {
            static BucketType deviceName = new BucketType("device_name", new byte[] {1});
            static BucketType osType = new BucketType("os", new byte[] {2});
            
            @Override
            public CSerializable bucketForWrite(DeviceType deviceType, BucketType bucketType) {
                if(bucketType == osType) {
                    switch(deviceType) {
                    case HTC_MYTOUCH:
                    case SAMSUNG_GALAXY:
                        return new EnumSerializable(OsManufacturer.ANDROID, 4);
                    case IPAD:
                    case IPHONE: 
                        return new EnumSerializable(OsManufacturer.APPLE, 4);
                    default:
                        throw new RuntimeException("Unknown device " + deviceType);
                    }
                } else if(bucketType == deviceName) {
                    return new EnumSerializable(deviceType, 4);
                } else {
                    throw new RuntimeException("Unknown bucket type " + bucketType);
                }
            }

            @Override
            public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
                if(coordinateField instanceof DeviceType) {
                    return new EnumSerializable((DeviceType)coordinateField, 4);
                } else if(coordinateField instanceof OsManufacturer) {
                    return new EnumSerializable((OsManufacturer)coordinateField, 4);
                } else {
                    throw new RuntimeException("Unexpected coordinate class " + 
                            coordinateField.getClass());
                }
            }

            @Override
            public List<BucketType> getBucketTypes() {
                return ImmutableList.of(deviceName, osType);
            }
        }
        
        Dimension<DateTime> time = new Dimension<DateTime>("time", new HourDayMonthBucketer(), false, 8);
        Dimension<DeviceType> device = new Dimension<DeviceType>("device", new DeviceBucketer(), true, 4);
        Dimension<City> location = new Dimension<City>("location", new LocationBucketer(), true, 4);
        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, device, location);
        
        Rollup cityAndOsRollup = new Rollup(location, LocationBucketer.usCity,
                device, DeviceBucketer.osType);
        Rollup stateRollup = new Rollup(location, LocationBucketer.usState);
        Rollup stateAndDeviceNameRollup = new Rollup(location, LocationBucketer.usState,
                device, DeviceBucketer.deviceName);
        Rollup monthAndStateRollup = new Rollup(time, HourDayMonthBucketer.months,
                location, LocationBucketer.usState);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup deviceTypeDayRollup = new Rollup(device, DeviceBucketer.deviceName,
                time, HourDayMonthBucketer.days);
        Rollup allRollup = new Rollup(ImmutableSet.<DimensionAndBucketType>of());
        Rollup stateMonthOsRollup = new Rollup(time, HourDayMonthBucketer.months,
                location, LocationBucketer.usState, 
                device, DeviceBucketer.osType);
        
        List<Rollup> rollups = ImmutableList.of(cityAndOsRollup, stateRollup, 
                stateAndDeviceNameRollup, monthAndStateRollup, hourRollup,
                deviceTypeDayRollup, allRollup, stateMonthOsRollup);
        
        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = Maps.newConcurrentMap();
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(dimensions, backingMap, 
                LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, 3);
        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 1);
        
        public void addEvent(DeviceType deviceType, City city, DateTime when) throws IOException {
            dataCubeIo.write(new LongOp(1), new WriteAddressBuilder(dataCube)
                    .at(time, when)
                    .at(device, deviceType)
                    .at(location, city)
                    .build());
        }
        
        public Optional<LongOp> read(ReadAddress address) throws IOException {
            return dataCubeIo.get(address);
        }
        
        public long getStateCount(UsState usState) throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddressBuilder()
                    .at(location, LocationBucketer.usState, usState)
                    .build());
            if(!opt.isPresent()) {
                return 0;
            } else {
                return opt.get().getValue();
            }
        }
        
        public long getCityManufacturerCount(City city, OsManufacturer manufacturer) throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddressBuilder()
                    .at(device, DeviceBucketer.osType, OsManufacturer.APPLE)
                    .at(location, LocationBucketer.usCity, City.PORTLAND)
                    .build());
            
            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getValue();
            }
        }
        
        public long getDeviceDayCount(DeviceType deviceType, DateTime day) throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddressBuilder()
                    .at(device, DeviceBucketer.deviceName, deviceType)
                    .at(time, HourDayMonthBucketer.days, day)
                    .build());
            
            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getValue();
            }
        }
        
        public long getHourCount(DateTime hour) throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddressBuilder()
                    .at(time, HourDayMonthBucketer.hours, hour)
                    .build());

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getValue();
            }
        }
        
        public long getAllEventsCount() throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddress());
            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getValue();
            }
        }
        
        public long getStateMonthOsCount(UsState state, DateTime month, OsManufacturer os) throws IOException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadAddressBuilder()
                    .at(time, HourDayMonthBucketer.months, month)
                    .at(location, LocationBucketer.usState, state)
                    .at(device, DeviceBucketer.osType, os)
                    .build());

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getValue();
            }
        }
    }
    
    @Test
    public void test() throws Exception {
        MobileCountCube mobileCube = new MobileCountCube();
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        DateTime oneHourAgo = now.minusHours(1);
        DateTime oneDayAgo = now.minusDays(1);
        DateTime oneMonthAgo = now.minusMonths(1);
        
        mobileCube.addEvent(DeviceType.IPHONE, City.PORTLAND, now);
        mobileCube.addEvent(DeviceType.IPAD, City.PORTLAND, now);
        mobileCube.addEvent(DeviceType.SAMSUNG_GALAXY, City.PORTLAND, now);
        mobileCube.addEvent(DeviceType.HTC_MYTOUCH, City.PORTLAND, now);
        mobileCube.addEvent(DeviceType.HTC_MYTOUCH, City.PORTLAND, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPAD, City.SEATTLE, now);
        mobileCube.addEvent(DeviceType.IPAD, City.SEATTLE, oneHourAgo);
        mobileCube.addEvent(DeviceType.IPHONE, City.SEATTLE, now);
        mobileCube.addEvent(DeviceType.IPHONE, City.OLYMPIA, oneDayAgo);
        mobileCube.addEvent(DeviceType.IPHONE, City.OLYMPIA, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPHONE, City.OLYMPIA, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPHONE, City.SEATTLE, oneMonthAgo);
        
        // There were two events from apple devices in Portland
        long appleCountPortland = mobileCube.getCityManufacturerCount(City.PORTLAND, OsManufacturer.APPLE);
        Assert.assertEquals(2L, appleCountPortland);
        
        // There were three events from android devices in Portland
        long androidCountPortland = mobileCube.getCityManufacturerCount(City.PORTLAND, OsManufacturer.ANDROID);
        Assert.assertEquals(2L, androidCountPortland);
        
        // There were 4 events in Oregon
        long oregonCount = mobileCube.getStateCount(UsState.OREGON);
        Assert.assertEquals(5L, oregonCount);
        
        // There were 2 iphone events today
        long todayIPhoneCount = mobileCube.getDeviceDayCount(DeviceType.IPHONE, now);
        Assert.assertEquals(2L, todayIPhoneCount);
        
        // There were 6 events in the current hour
        long thisHourCount = mobileCube.getHourCount(now);
        Assert.assertEquals(6L, thisHourCount);
        
        // There were 12 events total
        long allEventsCount = mobileCube.getAllEventsCount();
        Assert.assertEquals(12L, allEventsCount);
        
        // There were 3 events from apple devices last month in Washington
        long lastMonthWashingtonAppleCount = mobileCube.getStateMonthOsCount(
                UsState.WASHINGTON, oneMonthAgo, OsManufacturer.APPLE);
        Assert.assertEquals(3L, lastMonthWashingtonAppleCount);
    }
}
