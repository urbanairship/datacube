package com.urbanairship.datacube.dbharnesses;

import com.codahale.metrics.Histogram;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.urbanairship.datacube.dbharnesses.HBaseDbHarness.QUALIFIER;

public class HbaseBatchIncrementer implements BatchDbHarness.BlockingIO<Map<byte[], Long>, Map<byte[], Long>> {
    private final byte[] cf;
    private final Histogram incrementSize;
    private final HTablePool pool;
    private final byte[] tableName;

    public HbaseBatchIncrementer(byte[] cf, String metricsScope, HTablePool pool, byte[] tableName) {
        this.cf = cf;
        this.incrementSize = Metrics.histogram(BatchDbHarness.class, "incrementSize", metricsScope);
        this.pool = pool;
        this.tableName = tableName;

    }

    @Override
    public Map<byte[], Long> apply(Map<byte[], Long> writes) throws IOException, InterruptedException {
        ImmutableMap.Builder<byte[], Long> failures = ImmutableMap.builder();

        List<Row> increments = new ArrayList<>();

        List<Map.Entry<byte[], Long>> entries = ImmutableList.copyOf(writes.entrySet());

        for (Map.Entry<byte[], Long> entry : entries) {
            // .... this is what the `increment` method below does. It doesn't seem super safe to me.
            long amount = entry.getValue();
            Increment increment = new Increment(entry.getKey());
            increment.addColumn(cf, QUALIFIER, amount);
            incrementSize.update(amount);
            increments.add(increment);
        }

        Object[] objects = WithHTable.batch(pool, tableName, increments);

        for (int i = 0; i < objects.length; ++i) {
            if (objects[i] == null) {
                failures.put(entries.get(i));
            }
        }

        return failures.build();
    }
}
