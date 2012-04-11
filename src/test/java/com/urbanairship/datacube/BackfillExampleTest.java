package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.backfill.HBaseBackfill;
import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;

public class BackfillExampleTest {
    private static final DateTime midnight = new DateTime(DateTimeZone.UTC).minusDays(1).
            withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
    
    private static final byte[] LIVE_CUBE_TABLE = "live_cube_table".getBytes();
    private static final byte[] SNAPSHOT_TABLE = "snapshot_table".getBytes();
    private static final byte[] BACKFILL_TABLE = "backfill_table".getBytes();
    
    private static final byte[] IDSERVICE_LOOKUP_TABLE = "lookup_table".getBytes();
    private static final byte[] IDSERVICE_COUNTER_TABLE = "counter_table".getBytes();
    private static final byte[] CF = "c".getBytes();
    
    private static HBaseTestingUtility hbaseTestUtil;

    private static final Dimension<DateTime> timeDimension = new Dimension<DateTime>("time", 
            new HourDayMonthBucketer(), false, 8);

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
        hbaseTestUtil = new HBaseTestingUtility();
        hbaseTestUtil.startMiniCluster();
        
        TestUtil.preventMiniClusterNPE(hbaseTestUtil); 
        hbaseTestUtil.startMiniMapReduceCluster();

        Configuration conf = hbaseTestUtil.getConfiguration(); 
        idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF, 
                ArrayUtils.EMPTY_BYTE_ARRAY);
        
        Rollup hourRollup = new Rollup(timeDimension, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(timeDimension, HourDayMonthBucketer.days);
        
        dataCube = new DataCube<LongOp>(ImmutableList.<Dimension<?>>of(timeDimension), 
                ImmutableList.of(hourRollup, dayRollup));
        
        hbaseTestUtil.createTable(LIVE_CUBE_TABLE, CF);
    }
    
    @AfterClass
    public static void shutdown() throws IOException {
        hbaseTestUtil.shutdownMiniMapReduceCluster();
        hbaseTestUtil.shutdownMiniCluster();
        TestUtil.cleanupHadoopLogs();
    }
    
    /**
     * A wrapper around a datacube that doesn't know about the color dimension that we'll add later.
     */
    private static class CubeWrapper {
        private final DataCubeIo<LongOp> dataCubeIo;
        
        public CubeWrapper(byte[] table, byte[] cf) throws IOException {
            DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(
                    hbaseTestUtil.getConfiguration(), ArrayUtils.EMPTY_BYTE_ARRAY, table, 
                    cf, LongOp.DESERIALIZER, idService);
            dataCubeIo = new DataCubeIo<LongOp>(dataCube, hbaseDbHarness, 1);
        }

        public void put(Event event) throws IOException {
            dataCubeIo.write(new LongOp(1), new WriteBuilder(dataCube)
                    .at(timeDimension, event.time));
        }
        
        public long getDayCount(DateTime day) throws IOException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadAddressBuilder(dataCube)
                .at(timeDimension, HourDayMonthBucketer.days, day));
            if(countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }
        
        public long getHourCount(DateTime hour) throws IOException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadAddressBuilder(dataCube)
                .at(timeDimension, HourDayMonthBucketer.hours, hour));
            if(countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
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
                CubeWrapper cubeWrapper = new CubeWrapper(table, cf);

                final List<Event> events = ImmutableList.of(new Event(midnight.plusHours(1)),
                        new Event(midnight.plusHours(2)),
                        new Event(midnight.plusHours(2).plusMinutes(30)));
                
                for(Event event: events) {
                    cubeWrapper.put(event);
                }
            }
        };
        
        HBaseBackfill backfill = new HBaseBackfill(hbaseTestUtil.getConfiguration(), 
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
