package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.AfterExecute;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.dbharnesses.HbaseDbHarnessConfiguration;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MockHbaseHbaseDbHarnessTest {
    private static final Logger log = LogManager.getLogger(MockHbaseHbaseDbHarnessTest.class);

    private byte[] CUBE_DATA_TABLE = "t".getBytes();
    private byte[] CF = "c".getBytes();

    private HbaseDbHarnessConfiguration config = HbaseDbHarnessConfiguration.newBuilder()
            .setBatchSize(5)
            .setNumIoeTries(15)
            .setUniqueCubeName(new byte[]{})
            .setTableName(CUBE_DATA_TABLE)
            .setCf(CF)
            .setCommitType(DbHarness.CommitType.INCREMENT)
            .build();


    @Test
    public void test() throws Exception {
        IdService idService = new MapIdService();

        HTablePool pool = mock(HTablePool.class);
        HTableInterface htableInterface = mock(HTableInterface.class);

        final List<Increment> incrementOperations = new LinkedList<>();

        when(pool.getTable(any(byte[].class))).thenReturn(htableInterface);
        long value = 10L;
        doAnswer(a -> {
            List<Increment> increments = (List<Increment>) a.getArguments()[0];
            Object[] objects = (Object[]) a.getArguments()[1];

            int size = increments.size();

            List<Row> failedOperations = new ArrayList<>();
            List<Throwable> throwables = new ArrayList<>();
            List<String> hosts = new ArrayList<>();

            for (int i = 0; i < size; ++i) {
                if (i % 2 == 1) {
                    failedOperations.add(increments.get(i));
                    throwables.add(new IOException("ughhh " + i));
                    hosts.add("host: " + i);
                    objects[i] = null;
                } else {
                    incrementOperations.add(increments.get(i));

                    objects[i] = new Result(new KeyValue[]{new KeyValue(
                            increments.get(i).getRow(),
                            CF,
                            HBaseDbHarness.QUALIFIER,
                            Longs.toByteArray(value)
                    )});
                }
            }

            if (!failedOperations.isEmpty()) {
                throw new RetriesExhaustedWithDetailsException(throwables, failedOperations, hosts);
            }
            return objects;
        }).when(htableInterface).batch(anyList(), any(Object[].class));

        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(config, pool, LongOp.DESERIALIZER, idService, (avoid) -> null);

        when(htableInterface.get(any(Get.class))).thenReturn(getResult(value), getResult(value), getResult(100L));

        HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

        Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
        Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(),
                true, 5);


        Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
        Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
        Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
        Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);

        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, zipcode);
        List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
                dayRollup);

        DataCube<LongOp> cube = new DataCube<LongOp>(dimensions, rollups);

        WriteBuilder writeBuilder = new WriteBuilder();

        // now, sup: hour and zip rollup
        // now, sup: day and zip rollup
        // now, sup: hour rollup
        // now, sup: day rollup
        // => 4 writes
        Batch<LongOp> writes = cube.getWrites(writeBuilder
                        .at(time, DateTime.now())
                        .at(zipcode, "sup"),
                new LongOp(10)
        );

        // yesterday, sup: hour and zip rollup
        // yesterday, sup: day and zip rollup
        // yesterday, sup: hour rollup
        // yesterday, sup: day rollup
        // => 4 writes
        writes.putAll(cube.getWrites(writeBuilder
                        .at(time, DateTime.now().minusDays(1))
                        .at(zipcode, "sup"),
                new LongOp(10)
        ));

        // yesterday, ok: hour and zip rollup
        // yesterday, ok: day and zip rollup
        // REPEAT: yesterday, ok: hour rollup
        // REPEAT: yesterday, ok: day rollup
        // => 2 writes
        writes.putAll(cube.getWrites(writeBuilder
                        .at(time, DateTime.now().minusDays(1))
                        .at(zipcode, "ok"),
                new LongOp(10)
        ));

        log.info(writes.getMap().size());

        List<BoxedByteArray> expected = writes.getMap().keySet()
                .stream()
                .map(a -> {
                    try {
                        return a.toWriteKey(idService);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sorted((a, b) -> Bytes.BYTES_COMPARATOR.compare(a, b))
                .map(BoxedByteArray::new)
                .collect(Collectors.toList());


        hbaseDbHarness.runBatchAsync(writes, new AfterExecute<LongOp>() {
            @Override
            public void afterExecute(Throwable t) {
                throw new RuntimeException(t);
            }
        });

        hbaseDbHarness.shutdown();

        List<BoxedByteArray> result = incrementOperations.stream().map(Increment::getRow)
                .sorted((a, b) -> Bytes.BYTES_COMPARATOR.compare(a, b))
                .map(BoxedByteArray::new).collect(Collectors.toList());

        assertEquals(expected, result);
        assertEquals(incrementOperations.size(), expected.size());

        verify(htableInterface, times(computeTimes(expected.size(), 2, config.batchSize))).batch(anyList(), any(Object[].class));
    }

    public int computeTimes(final int size, int divisor, int batcheSize) {
        int tempSize = size;
        int count = 0;
        while (tempSize > 0) {
            count++;
            for (int i = 0; i < size; ++i) {
                if (i % divisor == 1) {
                    tempSize--;
                }
            }
        }

        int batches = size / batcheSize;


        log.info("Ran " + count);

        return count * (batches) + 1;
    }

    private Result getResult(long value) {
        return new Result(ImmutableList.of(new KeyValue(new byte[]{}, new byte[]{}, new byte[]{}, Longs.toByteArray(value))));
    }
}
