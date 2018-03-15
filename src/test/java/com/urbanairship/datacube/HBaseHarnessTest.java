package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class HBaseHarnessTest extends EmbeddedClusterTestAbstract {
    public static final byte[] DATA_CUBE_TABLE = "data_cube" .getBytes();

    public static final byte[] CF = "c" .getBytes();


    @BeforeClass
    public static void setupCluster() throws Exception {
        getTestUtil().createTable(DATA_CUBE_TABLE, CF);
    }

    @Test
    public void basicCallbackTest() throws Exception {
        HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();
        Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
        Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), true, 5);

        Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
        Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);

        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, zipcode);
        List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup, dayRollup);

        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        IdService idService = new MapIdService();
        Semaphore s = new Semaphore(1);
        s.acquire();

        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(pool,
                "dh" .getBytes(), DATA_CUBE_TABLE, CF, LongOp.DESERIALIZER, idService,
                DbHarness.CommitType.INCREMENT, new TestCallback(s), 1, 1, 1, "none", 1);
        // Do an increment of 5 for a certain time and zipcode
        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, hbaseDbHarness, 1, 100000, SyncLevel.BATCH_SYNC);

        dataCubeIo.writeSync(new LongOp(5), new WriteBuilder()
                .at(time, DateTime.now(DateTimeZone.UTC))
                .at(zipcode, "97201"));

        boolean callbackCalled = s.tryAcquire(1, TimeUnit.SECONDS);
        Assert.assertTrue(callbackCalled);

        dataCubeIo.writeAsync(new LongOp(5), new WriteBuilder()
                .at(time, DateTime.now(DateTimeZone.UTC))
                .at(zipcode, "97202"));

        callbackCalled = s.tryAcquire(1, TimeUnit.SECONDS);
        Assert.assertTrue(callbackCalled);
    }

    @Test
    public void noCallbackTest() throws Exception {
        HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();
        Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
        Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), true, 5);

        Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
        Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);

        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, zipcode);
        List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup, dayRollup);

        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        IdService idService = new MapIdService();

        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(pool,
                "dh" .getBytes(), DATA_CUBE_TABLE, CF, LongOp.DESERIALIZER, idService,
                DbHarness.CommitType.INCREMENT, 1, 1, 1, "none");

        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, hbaseDbHarness, 1, 100000, SyncLevel.BATCH_SYNC);

        dataCubeIo.writeSync(new LongOp(5), new WriteBuilder()
                .at(time, DateTime.now(DateTimeZone.UTC))
                .at(zipcode, "97201"));
    }

    @Ignore
    public static class TestCallback implements Function<Map<byte[], byte[]>, Void> {
        private final Semaphore flag;

        public TestCallback(Semaphore flag) {
            this.flag = flag;
        }

        @Nullable
        @Override
        public Void apply(@Nullable Map<byte[], byte[]> map) {
            flag.release();
            return null;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            return (o instanceof TestCallback) && this == o;
        }
    }
}