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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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

    private HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

    private Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
    private Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(),
            true, 5);


    private Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
    private Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
    private Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
    private Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);

    private List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, zipcode);
    private List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
            dayRollup);

    private DataCube<LongOp> cube = new DataCube<LongOp>(dimensions, rollups);

    private Map<byte[], byte[]> entries = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    @Test
    public void test() throws Exception {
        IdService idService = new MapIdService();

        HTablePool pool = mock(HTablePool.class);
        HTableInterface htableInterface = mock(HTableInterface.class);

        final List<Increment> incrementOperations = new LinkedList<>();

        when(pool.getTable(any(byte[].class))).thenReturn(htableInterface);

        long value = 10L;
        doAnswer(new BatchAnswer(incrementOperations)).when(htableInterface).batch(anyList(), any(Object[].class));

        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(config, pool, LongOp.DESERIALIZER, idService, (map) -> {
            entries.putAll(map);
            return null;
        });

        when(htableInterface.get(any(Get.class))).thenAnswer(new GetAnswer());

        WriteBuilder writeBuilder = new WriteBuilder();

        DateTime now = DateTime.now();
        DateTime yesterday = now.minusDays(1);
        String sup = "sup";
        String ok = "ok";

        // now, sup: hour and zip rollup
        // now, sup: day and zip rollup
        // now, sup: hour rollup
        // now, sup: day rollup
        // => 4 unique rows
        Batch<LongOp> writes = cube.getWrites(writeBuilder
                        .at(time, now)
                        .at(zipcode, sup),
                new LongOp(10)
        );

        // yesterday, sup: hour and zip rollup
        // yesterday, sup: day and zip rollup
        // yesterday, sup: hour rollup
        // yesterday, sup: day rollup
        // => 4 unique rows
        writes.putAll(cube.getWrites(writeBuilder
                        .at(time, yesterday)
                        .at(zipcode, sup),
                new LongOp(10)
        ));

        // yesterday, ok: hour and zip rollup
        // yesterday, ok: day and zip rollup
        // REPEAT: yesterday, ok: hour rollup
        // REPEAT: yesterday, ok: day rollup
        // => 2 unique rows
        writes.putAll(cube.getWrites(writeBuilder
                        .at(time, yesterday)
                        .at(zipcode, ok),
                new LongOp(10)
        ));

        log.info(writes.getMap());

        List<BoxedByteArray> expected = writes.getMap().keySet()
                .stream()
                .map(a -> {
                    try {
                        return a.toWriteKey(idService);
                    } catch (IOException | InterruptedException e) {
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

        hbaseDbHarness.flush();

        assertEquals(10L, hbaseDbHarness.get(new ReadBuilder(cube)
                .at(time, HourDayMonthBucketer.hours, now)
                .build()).get().getLong());
        assertEquals(20L, hbaseDbHarness.get(new ReadBuilder(cube)
                .at(time, HourDayMonthBucketer.hours, yesterday)
                .build()).get().getLong());

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

        return count * (batches) + 1;
    }

    private class BatchAnswer implements Answer {
        private final List<Increment> incrementOperations;

        public BatchAnswer(List<Increment> incrementOperations) {
            this.incrementOperations = incrementOperations;
        }

        @Override
        public Object answer(InvocationOnMock a) throws Throwable {
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
                            Longs.toByteArray(increments.get(i).getFamilyMap().get(CF).get(HBaseDbHarness.QUALIFIER))
                    )});
                }
            }

            if (!failedOperations.isEmpty()) {
                throw new RetriesExhaustedWithDetailsException(throwables, failedOperations, hosts);
            }
            return objects;
        }
    }

    private class GetAnswer implements Answer<Result> {
        @Override
        public Result answer(InvocationOnMock invocation) throws Throwable {
            Get get = (Get) invocation.getArguments()[0];
            return new Result(new KeyValue[]{new KeyValue(
                    get.getRow(),
                    config.cf,
                    HBaseDbHarness.QUALIFIER,
                    entries.get(get.getRow())
            )});
        }
    }
}
