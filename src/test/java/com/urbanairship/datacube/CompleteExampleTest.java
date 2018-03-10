/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertTrue;


/**
 * An example of using all the DataCube features at the same time.
 */
public class CompleteExampleTest {
    enum DeviceType {IPHONE, IPAD, HTC_MYTOUCH, SAMSUNG_GALAXY}

    enum OsManufacturer {APPLE, ANDROID}

    static OsManufacturer getManufacturer(DeviceType deviceType) {
        switch (deviceType) {
            case HTC_MYTOUCH:
            case SAMSUNG_GALAXY:
                return OsManufacturer.ANDROID;
            case IPAD:
            case IPHONE:
                return OsManufacturer.APPLE;
            default:
                throw new RuntimeException("we don't know who made " + deviceType);
        }
    }

    static BucketType usStateBucketType = new BucketType("us_state", new byte[]{1});
    static BucketType usCityBucketType = new BucketType("city", new byte[]{2});

    public enum Location implements CSerializable {
        WASHINGTON(0, usStateBucketType),
        OREGON(1, usStateBucketType),
        CALIFORNIA(2, usStateBucketType),

        PORTLAND(3, usCityBucketType, OREGON),
        SALEM(4, usCityBucketType, OREGON),
        SANFRANCISCO(5, usCityBucketType, CALIFORNIA),
        SACRAMENTO(6, usCityBucketType, CALIFORNIA),
        OLYMPIA(7, usCityBucketType, WASHINGTON),
        SEATTLE(8, usCityBucketType, WASHINGTON);

        public static final int SERIALIZED_SIZE = Ints.BYTES;
        private int id;
        private BucketType bucketType;
        private Set<Location> parent;

        Location(int id, BucketType bucketType, Location... parent) {
            this.id = id;
            this.bucketType = bucketType;
            this.parent = ImmutableSet.<Location>builder().add(parent).build();
        }

        public static Location getById(int id) {
            for (Location location : Location.values()) {
                if (location.id == id) {
                    return location;
                }
            }
            throw new RuntimeException("unknown id " + id);
        }

        @Override
        public byte[] serialize() {
            return Ints.toByteArray(id);
        }

        public static Location deserialize(byte[] serialized) {
            ByteBuffer wrap = ByteBuffer.wrap(serialized);
            int id = wrap.getInt();
            return getById(id);
        }
    }


    /**
     * This is a wrapper around a DataCube that we're going to use to count mobile devices by
     * type, location, and timestamp. We'll be able to get slices/rollup counts by arbitrary
     * dimensions and buckets (how many events match arbitrary criteria X).
     */
    static class MobileCountCube {
        /**
         * Bucketize cities into two buckets: (1) the state they belong to and (2) their
         * literal city name
         */
        static class LocationBucketer implements UniBucketer<Location> {

            public SetMultimap<BucketType, CSerializable> bucketForWrite(Location location) {
                ImmutableSetMultimap.Builder<BucketType, CSerializable> mapBuilder =
                        ImmutableSetMultimap.builder();

                mapBuilder.put(location.bucketType, location);
                for (Location parent : location.parent) {
                    mapBuilder.put(parent.bucketType, parent);
                }

                return mapBuilder.build();
            }

            @Override
            public Location deserialize(byte[] coord) {
                return Location.deserialize(coord);
            }

            public CSerializable bucketForRead(Location coordinateField, BucketType bucketType) {
                return coordinateField;
            }

            public List<BucketType> getBucketTypes() {
                return ImmutableList.of(usStateBucketType, usCityBucketType);
            }
        }

        BucketType deviceName = BucketType.IDENTITY;
        BucketType osType = BucketType.IDENTITY;

        Dimension<DateTime,DateTime> time = new Dimension<DateTime, DateTime>("time", new HourDayMonthBucketer(), false, 8);

        /**
         * Bucketize mobile devices into two bucket types: (1) the literal device type (e.g.
         * ipad, mytouch) and (2) the OS manufacturer (apple, android)
         */
        UniBucketer<DeviceType> deviceTypeBucketer = new EnumToOrdinalBucketer<>(1, DeviceType.class);
        UniBucketer<OsManufacturer> manufacturerBucketer = new EnumToOrdinalBucketer<>(1, OsManufacturer.class);

        Dimension<DeviceType, DeviceType> device = new Dimension<DeviceType, DeviceType>("device", deviceTypeBucketer, true, 1);
        Dimension<OsManufacturer, OsManufacturer> manufacturer = new Dimension<OsManufacturer, OsManufacturer>("manufacturer", manufacturerBucketer, true, 1);
        Dimension<Location, Location> location = new Dimension<Location, Location>("location", new LocationBucketer(), false, 4);
        List<Dimension<?, ?>> dimensions = ImmutableList.of(time, device, manufacturer, location);

        Rollup cityAndOsRollup = new Rollup(location, usCityBucketType, manufacturer, osType);
        Rollup stateRollup = new Rollup(location, usStateBucketType);
        Rollup stateAndDeviceNameRollup = new Rollup(location, usStateBucketType, device, deviceName);
        Rollup monthAndStateRollup = new Rollup(time, HourDayMonthBucketer.months, location, usStateBucketType);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup deviceTypeDayRollup = new Rollup(device, deviceName, time, HourDayMonthBucketer.days);
        Rollup manufacturerDayRollup = new Rollup(manufacturer, osType, time, HourDayMonthBucketer.days);
        Rollup allRollup = new Rollup();
        Rollup stateMonthOsRollup = new Rollup(time, HourDayMonthBucketer.months, location, usStateBucketType, device, osType);
        Rollup cityRollup = new Rollup(location, usCityBucketType);

        List<Rollup> rollups = ImmutableList.of(cityAndOsRollup, stateRollup, cityRollup,
                stateAndDeviceNameRollup, monthAndStateRollup, hourRollup,
                deviceTypeDayRollup, allRollup, stateMonthOsRollup, manufacturerDayRollup);

        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        ConcurrentMap<BoxedByteArray, byte[]> backingMap = Maps.newConcurrentMap();
        IdService idService = new CachingIdService(4, new MapIdService(), "test");
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap,
                LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, idService);
        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 1,
                Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        public void addEvent(DeviceType deviceType, Location city, DateTime when) throws IOException, InterruptedException {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder()
                    .at(time, when)
                    .at(device, deviceType)
                    .at(manufacturer, getManufacturer(deviceType))
                    .at(location, city));
        }

        public long getStateCount(Location inputLoc) throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(location, inputLoc.bucketType, inputLoc));
            return opt.map(LongOp::getLong).orElse(0L);
        }

        public long getCityManufacturerCount(Location city, OsManufacturer manufacturer) throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(this.manufacturer, this.osType, manufacturer)
                    .at(this.location, city.bucketType, city));

            return opt.map(LongOp::getLong).orElse(0L);
        }

        public long getDeviceDayCount(DeviceType deviceType, DateTime day) throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(device, deviceName, deviceType)
                    .at(time, HourDayMonthBucketer.days, day));

            return opt.map(LongOp::getLong).orElse(0L);
        }

        public long getHourCount(DateTime hour) throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(time, HourDayMonthBucketer.hours, hour));

            return opt.map(LongOp::getLong).orElse(0L);
        }

        public long getAllEventsCount() throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube));
            return opt.map(LongOp::getLong).orElse(0L);
        }

        public long getStateMonthOsCount(Location state, DateTime month, OsManufacturer os)
                throws IOException, InterruptedException {
            java.util.Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(time, HourDayMonthBucketer.months, month)
                    .at(location, state.bucketType, state)
                    .at(manufacturer, osType, os));

            return opt.map(LongOp::getLong).orElse(0L);
        }


    }

    @Test
    public void test() throws Exception {
        MobileCountCube mobileCube = new MobileCountCube();

        DateTime now = new DateTime(2015, 8, 15, 0, 0, 0, DateTimeZone.UTC);
        DateTime oneHourAgo = now.minusHours(1);
        DateTime oneDayAgo = now.minusDays(1);
        DateTime oneMonthAgo = now.minusMonths(1);

        mobileCube.addEvent(DeviceType.IPHONE, Location.PORTLAND, now);
        mobileCube.addEvent(DeviceType.IPAD, Location.PORTLAND, now);
        mobileCube.addEvent(DeviceType.SAMSUNG_GALAXY, Location.PORTLAND, now);
        mobileCube.addEvent(DeviceType.HTC_MYTOUCH, Location.PORTLAND, now);
        mobileCube.addEvent(DeviceType.HTC_MYTOUCH, Location.PORTLAND, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPAD, Location.SEATTLE, now);
        mobileCube.addEvent(DeviceType.IPAD, Location.SEATTLE, oneHourAgo);
        mobileCube.addEvent(DeviceType.IPHONE, Location.SEATTLE, now);
        mobileCube.addEvent(DeviceType.IPHONE, Location.OLYMPIA, oneDayAgo);
        mobileCube.addEvent(DeviceType.IPHONE, Location.OLYMPIA, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPHONE, Location.OLYMPIA, oneMonthAgo);
        mobileCube.addEvent(DeviceType.IPHONE, Location.SEATTLE, oneMonthAgo);

        assertTrue(mobileCube.dataCubeIo.get(new ReadBuilder(mobileCube.dataCube)
                .at(mobileCube.location, Location.PORTLAND.bucketType, Location.PORTLAND)).isPresent());
        assertTrue(mobileCube.dataCubeIo.get(new ReadBuilder(mobileCube.dataCube)
                .at(mobileCube.location, Location.OREGON.bucketType, Location.OREGON)).isPresent());

        // There were two events from apple devices in Portland
        long appleCountPortland = mobileCube.getCityManufacturerCount(Location.PORTLAND, OsManufacturer.APPLE);
        Assert.assertEquals(2L, appleCountPortland);

        // There were 3 events from android devices in Portland
        long androidCountPortland = mobileCube.getCityManufacturerCount(Location.PORTLAND, OsManufacturer.ANDROID);
        Assert.assertEquals(3L, androidCountPortland);

        // There were 5 events in Oregon
        long oregonCount = mobileCube.getStateCount(Location.OREGON);
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
                Location.WASHINGTON, oneMonthAgo, OsManufacturer.APPLE);
        Assert.assertEquals(3L, lastMonthWashingtonAppleCount);
    }
}
