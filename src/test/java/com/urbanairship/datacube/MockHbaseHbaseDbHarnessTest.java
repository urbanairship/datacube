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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MockHbaseHbaseDbHarnessTest {
    private static final Logger log = LogManager.getLogger(MockHbaseHbaseDbHarnessTest.class);

    private byte[] CUBE_DATA_TABLE = "table".getBytes();
    private byte[] CF = "c".getBytes();

    @Test
    public void test() throws Exception {
        IdService idService = new MapIdService();

        HTablePool pool = mock(HTablePool.class);
        HTableInterface htableInterface = mock(HTableInterface.class);

        final List<Increment> incrementOperations = new LinkedList<>();

        when(pool.getTable(any(byte[].class))).thenReturn(htableInterface);
        long value = 10L;
        when(htableInterface.batch(anyList()))
                .thenAnswer(a -> {
                    List<Increment> arguments = (List<Increment>) a.getArguments()[0];
                    int size = arguments.size();
                    Object[] objects = new Object[size];

                    for (int i = 0; i < size; ++i) {
                        incrementOperations.add(arguments.get(i));
                        if (i % 2 == 1) {
                            objects[i] = null;
                        }
                        objects[i] = new Result(new KeyValue[]{new KeyValue(
                                arguments.get(i).getRow(),
                                CF,
                                HBaseDbHarness.QUALIFIER,
                                Longs.toByteArray(value)
                        )});
                    }
                    return objects;
                });

        HbaseDbHarnessConfiguration config = HbaseDbHarnessConfiguration.newBuilder()
                .setBatchSize(2)
                .setUniqueCubeName(new byte[]{})
                .setTableName(CUBE_DATA_TABLE)
                .setCf(CF)
                .setCommitType(DbHarness.CommitType.INCREMENT)
                .build();

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
        Batch<LongOp> writes = cube.getWrites(writeBuilder
                        .at(time, DateTime.now())
                        .at(zipcode, "sup"),
                new LongOp(10)
        );

        hbaseDbHarness.runBatchAsync(writes, new AfterExecute<LongOp>() {
            @Override
            public void afterExecute(Throwable t) {
                throw new RuntimeException(t);
            }
        });

        hbaseDbHarness.shutdown();

        int size = writes.getMap().size();

        List<BoxedByteArray> result = incrementOperations.stream().map(Increment::getRow)
                .sorted((a, b) -> Bytes.BYTES_COMPARATOR.compare(a, b))
                .map(BoxedByteArray::new).collect(Collectors.toList());

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
                .map(BoxedByteArray::new)
                .sorted((a, b) -> Bytes.BYTES_COMPARATOR.compare(a.bytes, b.bytes))
                .collect(Collectors.toList());

        assertEquals(expected, result);

        assertEquals(incrementOperations.size(), size);

        verify(htableInterface, times(size / config.batchSize)).batch(anyList());
    }

    private Result getResult(long value) {
        return new Result(ImmutableList.of(new KeyValue(new byte[]{}, new byte[]{}, new byte[]{}, Longs.toByteArray(value))));
    }
}
