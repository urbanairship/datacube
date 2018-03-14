package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.dbharnesses.HbaseDbHarnessConfiguration;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Test;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MockHbaseHbaseDbHarnessTest {

    private byte[] CUBE_DATA_TABLE = "table".getBytes();
    private byte[] CF = "c".getBytes();

    @Test
    public void test() throws Exception {
        IdService idService = new MapIdService();

        HTablePool pool = mock(HTablePool.class);
        HTableInterface htableInterface = mock(HTableInterface.class);

        when(pool.getTable(any(byte[].class))).thenReturn(htableInterface);
        when(htableInterface.batch(anyList()))
                .thenAnswer(a -> {
                    List<Row> arguments = (List<Row>) a.getArguments()[0];
                    int size = arguments.size();
                    if (size < 3) {
                        return arguments;
                    }
                    Object[] objects = new Object[size];

                    for (int i = 0; i < size; ++i) {
                        if (i % 2 == 0) {
                            objects[i] = null;
                        }
                        objects[i] = new Object();
                    }
                    return objects;
                });

        HbaseDbHarnessConfiguration config = HbaseDbHarnessConfiguration.newBuilder()
                .setBatchSize(100)
                .setUniqueCubeName("hbaseForCubeDataTest".getBytes())
                .setTableName(CUBE_DATA_TABLE)
                .setCf(CF)
                .setCommitType(DbHarness.CommitType.INCREMENT)
                .build();

        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(config, pool, LongOp.DESERIALIZER, idService, (avoid) -> null);

        when(htableInterface.get(any(Get.class))).thenReturn(getResult(10L), getResult(10L), getResult(100L));
        DbHarnessTests.asyncBatchWritesTest(hbaseDbHarness, 10);
        verify(htableInterface, atLeast(2)).batch(anyList());
    }

    private Result getResult(long value) {
        return new Result(ImmutableList.of(new KeyValue(new byte[]{}, new byte[]{}, new byte[]{}, Longs.toByteArray(value))));
    }
}
