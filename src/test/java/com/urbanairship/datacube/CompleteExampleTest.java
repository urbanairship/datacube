/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.*;
import com.urbanairship.datacube.serializables.IntSerializable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.serializables.EnumSerializable;


/**
* An example of using all the DataCube features at the same time.
*/
public class CompleteExampleTest {
    enum DeviceType {IPHONE, IPAD, HTC_MYTOUCH, SAMSUNG_GALAXY}
    enum City {PORTLAND, SALEM, SANFRANCISCO, SACRAMENTO, OLYMPIA, SEATTLE}

    enum OsManufacturer {ANDROID, APPLE}
    enum UsState {OREGON, CALIFORNIA, WASHINGTON}

    /**
     * This is a wrapper around a DataCube that we're going to use to count mobile devices by
     * type, location, and timestamp. We'll be able to get slices/rollup counts by arbitrary
     * dimensions and buckets (how many events match arbitrary criteria X).
     */
    static class MobileCountCube {
        Bucketer<DateTime> timeBucketer = new HourDayMonthBucketer();

        /**
         * Bucketize cities into two buckets: (1) the state they belong to and (2) their
         * literal city name
         */
        static class LocationBucketer implements Bucketer<City> {
            static BucketType usState = new BucketType("us_state", new byte[]{1});
            static BucketType usCity = new BucketType("city", new byte[]{2});


            @Override
            public SetMultimap<BucketType, CSerializable> bucketForWrite(City city) {
                ImmutableSetMultimap.Builder<BucketType,CSerializable> mapBuilder =
                        ImmutableSetMultimap.builder();

                // One bucket type is the state where the user is currently located.
                switch(city) {
                    case PORTLAND:
                    case SALEM:
                        // These cities are in Oregon
                        mapBuilder.put(usState, new EnumSerializable(UsState.OREGON, 1));
                        break;
                    case SANFRANCISCO:
                    case SACRAMENTO:
                        // These cities are in California
                        mapBuilder.put(usState, new EnumSerializable(UsState.CALIFORNIA, 1));
                        break;
                    case OLYMPIA:
                    case SEATTLE:
                        // These cities are in Washington
                        mapBuilder.put(usState, new EnumSerializable(UsState.WASHINGTON, 1));
                        break;
                    default:
                        throw new RuntimeException("Unknown city " + city);
                }

                // The other bucket type is the city where the user is currently located.
                mapBuilder.put(usCity, new EnumSerializable(city, 1));

                return mapBuilder.build();
            }

            @Override
            public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
                if(coordinateField instanceof City) {
                    return new EnumSerializable((City)coordinateField, 1);
                } else if(coordinateField instanceof UsState) {
                    return new EnumSerializable((UsState)coordinateField, 1);
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

        /**
         * Bucketize mobile devices into two bucket types: (1) the literal device type (e.g.
         * ipad, mytouch) and (2) the OS manufacturer (apple, android)
         */
        public static class DeviceBucketer implements Bucketer<DeviceType> {
            static BucketType deviceName = new BucketType("device_name", new byte[] {1});
            static BucketType osType = new BucketType("os", new byte[] {2});

            @Override
            public SetMultimap<BucketType, CSerializable> bucketForWrite(DeviceType deviceType) {
                ImmutableSetMultimap.Builder<BucketType,CSerializable> mapBuilder =
                        ImmutableSetMultimap.builder();

                switch(deviceType) {
                    case HTC_MYTOUCH:
                    case SAMSUNG_GALAXY:
                        mapBuilder.put(osType, new EnumSerializable(OsManufacturer.ANDROID, 1));
                        break;
                    case IPAD:
                    case IPHONE:
                        mapBuilder.put(osType, new EnumSerializable(OsManufacturer.APPLE, 1));
                        break;
                }

                mapBuilder.put(deviceName, new EnumSerializable(deviceType, 1));
                return mapBuilder.build();
            }

            @Override
            public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
                if(coordinateField instanceof DeviceType) {
                    return new EnumSerializable((DeviceType)coordinateField, 1);
                } else if(coordinateField instanceof OsManufacturer) {
                    return new EnumSerializable((OsManufacturer)coordinateField, 1);
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

        /*
         * Define the DataCube. This requires defining all the dimensions and defining the ways
         * that we want to roll up the counts (e.g. keep an aggregate count for each hour).
         */
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
        Rollup allRollup = new Rollup();
        Rollup stateMonthOsRollup = new Rollup(time, HourDayMonthBucketer.months,
                location, LocationBucketer.usState,
                device, DeviceBucketer.osType);

        List<Rollup> rollups = ImmutableList.of(cityAndOsRollup, stateRollup,
                stateAndDeviceNameRollup, monthAndStateRollup, hourRollup,
                deviceTypeDayRollup, allRollup, stateMonthOsRollup);

        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = Maps.newConcurrentMap();
        IdService idService = new CachingIdService(4, new MapIdService());
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap,
                LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, idService);
        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 1,
                Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        public void addEvent(DeviceType deviceType, City city, DateTime when) throws IOException, InterruptedException {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder(dataCube)
                    .at(time, when)
                    .at(device, deviceType)
                    .at(location, city));
        }

        public long getStateCount(UsState usState) throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(location, LocationBucketer.usState, usState));
            if(!opt.isPresent()) {
                return 0;
            } else {
                return opt.get().getLong();
            }
        }

        public long getCityManufacturerCount(City city, OsManufacturer manufacturer) throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(device, DeviceBucketer.osType, OsManufacturer.APPLE)
                    .at(location, LocationBucketer.usCity, City.PORTLAND));

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getLong();
            }
        }

        public long getDeviceDayCount(DeviceType deviceType, DateTime day) throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(device, DeviceBucketer.deviceName, deviceType)
                    .at(time, HourDayMonthBucketer.days, day));

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getLong();
            }
        }

        public long getHourCount(DateTime hour) throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(time, HourDayMonthBucketer.hours, hour));

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getLong();
            }
        }

        public long getAllEventsCount() throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube));
            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getLong();
            }
        }

        public long getStateMonthOsCount(UsState state, DateTime month, OsManufacturer os)
                throws IOException, InterruptedException  {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(time, HourDayMonthBucketer.months, month)
                    .at(location, LocationBucketer.usState, state)
                    .at(device, DeviceBucketer.osType, os));

            if(!opt.isPresent()) {
                return 0L;
            } else {
                return opt.get().getLong();
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

        // There were 2 events from android devices in Portland
        long androidCountPortland = mobileCube.getCityManufacturerCount(City.PORTLAND, OsManufacturer.ANDROID);
        Assert.assertEquals(2L, androidCountPortland);

        // There were 5 events in Oregon
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
