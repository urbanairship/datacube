package com.urbanairship.datacube.dbharnesses;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Package private class implementing all the batch increment operations accomplished by the hbase dbharness
 *
 * @param <T>
 */
class HbaseBatchIncrementer<T extends Op> {

    private final HTablePool pool;
    private final IncrementerMetrics incrementerMetrics;
    private final BlockingIO<Address, BoxedByteArray> serializer;
    private HbaseDbHarnessConfiguration configuration;

    interface BlockingIO<I, O> {
        O apply(I i) throws InterruptedException, IOException;
    }

    HbaseBatchIncrementer(HbaseDbHarnessConfiguration configuration, HTablePool pool, BlockingIO<Address, BoxedByteArray> serializer) {
        this.configuration = configuration;
        this.pool = pool;
        this.serializer = serializer;
        this.incrementerMetrics = new IncrementerMetrics(HBaseDbHarness.class, configuration.metricsScope);
    }

    /**
     * Slices up the batch map into batches of a reasonable size to send to the database, interprets the results
     * (include partial failures where only a part of the batch succeeded, and an exception was thrown) and updates
     * the success map and list with the results. It then rethrows whatever exception resulted from the HBase
     * interaction.
     *
     * @param batchMap            The write operations to accomplish
     * @param successfulAddresses The datacube address whose writes succeeded
     * @param successfulRows      A map from HBase row key to the value in the row after completion of the increment
     *                            operation
     *
     * @throws IOException
     * @throws InterruptedException
     */

    public void batchIncrement(Map<Address, T> batchMap, List<Address> successfulAddresses, Map<byte[], byte[]> successfulRows) throws IOException, InterruptedException {
        Iterable<List<Map.Entry<Address, T>>> partitions = Iterables.partition(batchMap.entrySet(), configuration.batchSize);

        Map<BoxedByteArray, byte[]> successes = new HashMap<>();

        int batchesPerFlush = 0;
        Map<BoxedByteArray, Address> backwards = new HashMap<>();
        boolean caughtException = false;
        try {
            for (List<Map.Entry<Address, T>> entries : partitions) {
                Map<BoxedByteArray, T> increments = new HashMap<>();

                batchesPerFlush++;

                final long nanoTimeBeforeWrite = System.nanoTime();

                for (Map.Entry<Address, T> entry : entries) {
                    final Address address = entry.getKey();
                    final BoxedByteArray rowKey = serializer.apply(address);

                    increments.put(rowKey, entry.getValue());
                    backwards.put(rowKey, address);
                }

                List<Map.Entry<BoxedByteArray, T>> entriesList = ImmutableList.copyOf(increments.entrySet());
                Object[] objects = new Object[entriesList.size()];

                try {
                    hbaseIncrement(entriesList, objects);
                } catch (InterruptedException | IOException e) {
                    caughtException = true;
                    throw e;
                } finally {
                    successes.putAll(processBatchCallresults(entriesList, objects));
                    long writeDurationNanos = System.nanoTime() - nanoTimeBeforeWrite;
                    incrementerMetrics.batchWritesTimer.update(writeDurationNanos, TimeUnit.NANOSECONDS);
                }
            }
        } finally {
            incrementerMetrics.batchesPerFlush.update(batchesPerFlush);

            for (Map.Entry<BoxedByteArray, byte[]> entry : successes.entrySet()) {
                successfulAddresses.add(backwards.get(entry.getKey()));
                successfulRows.put(entry.getKey().bytes, entry.getValue());
            }

            int failures = batchMap.size() - successfulAddresses.size();

            if (failures > 0 || caughtException) {
                incrementerMetrics.incrementFailuresPerFlush.update(failures);

                if (!caughtException) {
                    // the implementation prior to the addition of the batch increment code assumes any failed increment
                    // operation results in an io exception. This matches that expectation.
                    throw new IOException(String.format("Some writes failed (%s of %s attempted); queueing retry", failures, batchMap.size()));
                }
            }
        }
    }

    private void hbaseIncrement(List<Map.Entry<BoxedByteArray, T>> entriesList, Object[] objects) throws IOException, InterruptedException {
        List<Row> rows = new ArrayList<>();

        for (Map.Entry<BoxedByteArray, T> entry : entriesList) {
            long amount = Bytes.toLong(entry.getValue().serialize());
            Increment increment = new Increment(entry.getKey().bytes);
            increment.addColumn(configuration.cf, HBaseDbHarness.QUALIFIER, amount);
            incrementerMetrics.incrementSize.update(amount);
            rows.add(increment);
        }

        HTableInterface table = pool.getTable(configuration.tableName);

        table.batch(rows, objects);
    }

    /**
     * Converts the datacube request objects and an array of objects returned from an hbase batch call into the map we
     * use to track success.
     *
     * @param entries The map entries we used to construct the increment request against hbase.
     * @param objects The response to the batch operation
     *
     * @return A map from the serialized {@link Address} to the bytes in the column after completion of the operation.
     */
    private Map<BoxedByteArray, byte[]> processBatchCallresults(List<Map.Entry<BoxedByteArray, T>> entries, Object[] objects) {
        Map<BoxedByteArray, byte[]> successes = new HashMap<>();
        for (int i = 0; i < objects.length; ++i) {
            if (objects[i] != null && objects[i] instanceof Result) {
                Result result = (Result) objects[i];
                byte[] value = result.getValue(configuration.cf, HBaseDbHarness.QUALIFIER);
                successes.put(entries.get(i).getKey(), value);
            }
        }
        return successes;
    }

    private class IncrementerMetrics {
        private final Histogram incrementSize;
        private final Histogram incrementFailuresPerFlush;
        private final Histogram batchesPerFlush;
        private final Timer batchWritesTimer;

        public IncrementerMetrics(Class clazz, String metricsScope) {
            incrementSize = Metrics.histogram(clazz, "incrementSize", metricsScope);
            incrementFailuresPerFlush = Metrics.histogram(clazz, "failuresPerFlush", metricsScope);
            batchesPerFlush = Metrics.histogram(clazz, "batchesPerFlush", metricsScope);
            batchWritesTimer = Metrics.timer(clazz, "batchWrites", metricsScope);
        }
    }
}
