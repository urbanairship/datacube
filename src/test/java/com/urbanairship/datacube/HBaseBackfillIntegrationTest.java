/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.backfill.HBaseBackfillMerger;
import com.urbanairship.datacube.backfill.HBaseSnapshotter;
import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.ops.LongOp.LongOpDeserializer;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HBaseBackfillIntegrationTest extends EmbeddedClusterTestAbstract {
    private enum Color {RED, BLUE}

    private static final DateTime midnight = new DateTime(DateTimeZone.UTC).minusDays(1).
            withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);

    private static final byte[] LIVE_CUBE_TABLE = "live_cube_table" .getBytes();
    private static final byte[] SNAPSHOT_TABLE = "snapshot_table" .getBytes();
    private static final byte[] BACKFILL_TABLE = "backfill_table" .getBytes();

    private static final byte[] IDSERVICE_LOOKUP_TABLE = "lookup_table" .getBytes();
    private static final byte[] IDSERVICE_COUNTER_TABLE = "counter_table" .getBytes();
    private static final byte[] CF = "c" .getBytes();

    private static final Dimension<DateTime> timeDimension = new Dimension<DateTime>("time",
            new HourDayMonthBucketer(), false, 8);
    private static final Dimension<Color> colorDimension = new Dimension<Color>("color",
            new EnumToOrdinalBucketer<Color>(1), false, 1);

    private static IdService idService;
    private static DataCube<LongOp> oldCube, newCube;

    private static class Event {
        public final DateTime time;
        public final Color color;

        public Event(DateTime time, Color color) {
            this.time = time;
            this.color = color;
        }
    }

    /**
     * A wrapper around a datacube that doesn't know about the color dimension that we'll add later.
     */
    private static class OldCubeWrapper {
        private final DataCubeIo<LongOp> dataCubeIo;

        public OldCubeWrapper() throws Exception {
            this.dataCubeIo = makeDataCubeIo(oldCube, LIVE_CUBE_TABLE);
        }

        public void put(Event event) throws IOException, InterruptedException {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder()
                    .at(timeDimension, event.time));
        }

        public long getDayCount(DateTime day) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(oldCube)
                    .at(timeDimension, HourDayMonthBucketer.days, day));
            if (countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }

        public long getHourCount(DateTime hour) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(oldCube)
                    .at(timeDimension, HourDayMonthBucketer.hours, hour));
            if (countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }
    }

    /**
     * A wrapper around a datacube that includes the new dimension that we add halfway through the test.
     */
    private static class NewCubeWrapper {
        private final DataCubeIo<LongOp> dataCubeIo;

        public NewCubeWrapper(DataCubeIo<LongOp> dataCubeIo) throws IOException {
            this.dataCubeIo = dataCubeIo;
        }

        public void put(Event event) throws IOException, InterruptedException {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder()
                    .at(timeDimension, event.time)
                    .at(colorDimension, event.color));
        }

        public long getHourColorCount(DateTime hour, Color color) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(newCube)
                    .at(timeDimension, HourDayMonthBucketer.hours, hour)
                    .at(colorDimension, color));
            if (countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }

        public long getDayCount(DateTime day) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(newCube)
                    .at(timeDimension, HourDayMonthBucketer.days, day));
            if (countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }
    }

    @BeforeClass
    public static void init() throws Exception {
        Configuration conf = getTestUtil().getConfiguration();
        idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF,
                ArrayUtils.EMPTY_BYTE_ARRAY);

        Rollup hourRollup = new Rollup(timeDimension, HourDayMonthBucketer.hours);
        Rollup hourColorRollup = new Rollup(colorDimension, timeDimension, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(timeDimension, HourDayMonthBucketer.days);

        List<Dimension<?>> oldDims = ImmutableList.of(timeDimension);
        List<Rollup> oldRollups = ImmutableList.of(hourRollup, dayRollup);

        List<Dimension<?>> newDims = ImmutableList.of(timeDimension, colorDimension);
        List<Rollup> newRollups = ImmutableList.of(hourRollup, dayRollup, hourColorRollup);

        oldCube = new DataCube<LongOp>(oldDims, oldRollups);
        newCube = new DataCube<LongOp>(newDims, newRollups);
    }

    /**
     * A complete simulation of backfilling/recreating a cube with a new dimension that didn't
     * previously exist in the cube.
     */
    @Test
    public void newDimensionTest() throws Exception {
        getTestUtil().createTable(LIVE_CUBE_TABLE, CF);
        OldCubeWrapper oldCubeWrapper = new OldCubeWrapper();

        List<Event> events = ImmutableList.of(
                new Event(midnight.plusHours(2), Color.BLUE),
                new Event(midnight.plusHours(2).plusMinutes(1), Color.RED),
                new Event(midnight.plusHours(6).plusMinutes(30), Color.RED),
                new Event(midnight.plusHours(9).plusMinutes(1), Color.RED));
        for (Event event : events) {
            oldCubeWrapper.put(event);
        }
        Assert.assertEquals(2L, oldCubeWrapper.getHourCount(midnight.plusHours(2)));
        Assert.assertEquals(1L, oldCubeWrapper.getHourCount(midnight.plusHours(6)));
        Assert.assertEquals(1L, oldCubeWrapper.getHourCount(midnight.plusHours(9)));
        Assert.assertEquals(4L, oldCubeWrapper.getDayCount(midnight));

        Configuration hadoopConf = getTestUtil().getConfiguration();

        // Take a snapshot of the "live" table and store it in the "snapshot" table
        boolean success;
        success = new HBaseSnapshotter(hadoopConf, LIVE_CUBE_TABLE, CF, SNAPSHOT_TABLE,
                new Path("hdfs://localhost:" + getTestUtil().getDFSCluster().getNameNodePort() + "/snapshot_hfiles"),
                false, null, null).runWithCheckedExceptions();
        Assert.assertTrue(success);

        // Backfill (re-count all existing events into the backfill table using the new cube)
        getTestUtil().createTable(BACKFILL_TABLE, CF);
        NewCubeWrapper backfillWrapper = new NewCubeWrapper(makeDataCubeIo(newCube, BACKFILL_TABLE));
        for (Event event : events) {
            backfillWrapper.put(event);
        }

        // Add another event into the live table using the new cube definition.
        NewCubeWrapper newCubeWrapper = new NewCubeWrapper(makeDataCubeIo(newCube, LIVE_CUBE_TABLE));
        newCubeWrapper.put(new Event(midnight.plusHours(2).plusMinutes(10), Color.BLUE));

        // The just-inserted event should have incremented the counts seen by the old cube, even
        // though the old cube doesn't know about the color dimension.
        Assert.assertEquals(3L, oldCubeWrapper.getHourCount(midnight.plusHours(2)));
        Assert.assertEquals(5L, oldCubeWrapper.getDayCount(midnight));

        // The live cube has only counted one event with the new cube definition. So the count for
        // that event's color should be 1, and the other colors should be 0, since only one event
        // was counted since we added the color dimension.
        Assert.assertEquals(1L, newCubeWrapper.getHourColorCount(midnight.plusHours(2), Color.BLUE));
        Assert.assertEquals(0L, newCubeWrapper.getHourColorCount(midnight.plusHours(1), Color.RED));
        Assert.assertEquals(0L, newCubeWrapper.getHourColorCount(midnight.plusHours(2), Color.RED));

        // Merge the backfill table and the snapshot table into the live cube table
        success = new HBaseBackfillMerger(hadoopConf, ArrayUtils.EMPTY_BYTE_ARRAY, LIVE_CUBE_TABLE,
                SNAPSHOT_TABLE, BACKFILL_TABLE, CF, LongOpDeserializer.class).runWithCheckedExceptions();
        Assert.assertTrue(success);

        // Now, after the backfill, all the events' colors should have been counted.
        Assert.assertEquals(2L, newCubeWrapper.getHourColorCount(midnight.plusHours(2), Color.BLUE));
        Assert.assertEquals(1L, newCubeWrapper.getHourColorCount(midnight.plusHours(2), Color.RED));
        Assert.assertEquals(1L, newCubeWrapper.getHourColorCount(midnight.plusHours(6), Color.RED));
        Assert.assertEquals(1L, newCubeWrapper.getHourColorCount(midnight.plusHours(9), Color.RED));
        Assert.assertEquals(5L, newCubeWrapper.getDayCount(midnight));

        // The counters should still work when accessed using the old cube definition.
        Assert.assertEquals(3L, oldCubeWrapper.getHourCount(midnight.plusHours(2)));
        Assert.assertEquals(1L, oldCubeWrapper.getHourCount(midnight.plusHours(6)));
        Assert.assertEquals(1L, oldCubeWrapper.getHourCount(midnight.plusHours(9)));
        Assert.assertEquals(5L, oldCubeWrapper.getDayCount(midnight));

        // Remove the snapshot table
        getTestUtil().getHBaseAdmin().disableTable(SNAPSHOT_TABLE);
        getTestUtil().getHBaseAdmin().deleteTable(SNAPSHOT_TABLE);
    }

    /**
     * Simple helper function that creates a DataCubeIo for a given DataCube that will write to the
     * given HBase table.
     */
    private static DataCubeIo<LongOp> makeDataCubeIo(DataCube<LongOp> cube, byte[] table)
            throws Exception {
        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<LongOp> dbHarness = new HBaseDbHarness<LongOp>(pool, ArrayUtils.EMPTY_BYTE_ARRAY,
                table, CF, LongOp.DESERIALIZER, idService, CommitType.INCREMENT, "scope");
        return new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC, "scope", true);
    }
}
