/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.backfill.HBaseBackfill;
import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class BackfillExampleTest extends EmbeddedClusterTestAbstract {
    private static final DateTime midnight = new DateTime(DateTimeZone.UTC).minusDays(1).
            withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);

    private static final byte[] LIVE_CUBE_TABLE = "live_cube_table" .getBytes();
    private static final byte[] SNAPSHOT_TABLE = "snapshot_table" .getBytes();
    private static final byte[] BACKFILL_TABLE = "backfill_table" .getBytes();

    private static final byte[] IDSERVICE_LOOKUP_TABLE = "lookup_table" .getBytes();
    private static final byte[] IDSERVICE_COUNTER_TABLE = "counter_table" .getBytes();
    private static final byte[] CF = "c" .getBytes();

    private static final Dimension<DateTime, DateTime> timeDimension = new Dimension<DateTime, DateTime>("time", new HourDayMonthBucketer(), false, 8);

    private static IdService idService;
    private static DataCube<LongOp> dataCube;

    private static class Event {
        public final DateTime time;

        public Event(DateTime time) {
            this.time = time;
        }
    }

    @BeforeClass
    public static void init() throws Exception {
        Configuration conf = getTestUtil().getConfiguration();
        idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF,
                ArrayUtils.EMPTY_BYTE_ARRAY);

        Rollup hourRollup = new Rollup(timeDimension, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(timeDimension, HourDayMonthBucketer.days);

        dataCube = new DataCube<LongOp>(ImmutableList.of(timeDimension),
                ImmutableList.of(hourRollup, dayRollup));

        getTestUtil().createTable(LIVE_CUBE_TABLE, CF);
    }

    private static class CubeWrapper {
        private final DataCubeIo<LongOp> dataCubeIo;

        public CubeWrapper(byte[] table, byte[] cf) throws Exception {
            HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
            DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(pool,
                    ArrayUtils.EMPTY_BYTE_ARRAY, table, cf, LongOp.DESERIALIZER, idService,
                    CommitType.INCREMENT);
            dataCubeIo = new DataCubeIo<LongOp>(dataCube, hbaseDbHarness, 1, Long.MAX_VALUE,
                    SyncLevel.FULL_SYNC);
        }

        public void put(Event event) throws Exception {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder()
                    .at(timeDimension, event.time));
        }

        public long getHourCount(DateTime hour) throws IOException, InterruptedException {
            java.util.Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(dataCube)
                    .at(timeDimension, HourDayMonthBucketer.hours, hour));
            return countOpt.map(LongOp::getLong).orElse(0L);
        }
    }

    @Test
    public void test() throws Exception {
        CubeWrapper cubeWrapper = new CubeWrapper(LIVE_CUBE_TABLE, CF);

        // This event will disappear from the counts after the backfill
        cubeWrapper.put(new Event(midnight.plusMinutes(30)));
        Assert.assertEquals(1L, cubeWrapper.getHourCount(midnight));

        HBaseBackfillCallback backfillCallback = new HBaseBackfillCallback() {
            @Override
            public void backfillInto(Configuration conf, byte[] table, byte[] cf, long snapshotFinishMs)
                    throws IOException {
                try {
                    CubeWrapper cubeWrapper = new CubeWrapper(table, cf);

                    final List<Event> events = ImmutableList.of(new Event(midnight.plusHours(1)),
                            new Event(midnight.plusHours(2)),
                            new Event(midnight.plusHours(2).plusMinutes(30)));

                    for (Event event : events) {
                        cubeWrapper.put(event);
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };

        HBaseBackfill backfill = new HBaseBackfill(getTestUtil().getConfiguration(),
                backfillCallback, LIVE_CUBE_TABLE, SNAPSHOT_TABLE, BACKFILL_TABLE, CF,
                LongOp.LongOpDeserializer.class);
        boolean success = backfill.runWithCheckedExceptions();
        Assert.assertTrue(success);

        Assert.assertEquals(0L, cubeWrapper.getHourCount(midnight.plusHours(0)));
        Assert.assertEquals(1L, cubeWrapper.getHourCount(midnight.plusHours(1)));
        Assert.assertEquals(2L, cubeWrapper.getHourCount(midnight.plusHours(2)));
        Assert.assertEquals(0L, cubeWrapper.getHourCount(midnight.plusHours(3)));
    }
}
