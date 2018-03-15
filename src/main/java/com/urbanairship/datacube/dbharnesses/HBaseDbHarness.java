/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.NamedThreadFactory;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.ThreadedIdServiceLookup;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HBaseDbHarness<T extends Op> implements DbHarness<T> {

    private static final Logger log = LoggerFactory.getLogger(HBaseDbHarness.class);

    public final static byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;

    private final static Function<Map<byte[], byte[]>, Void> NOP = new Function<Map<byte[], byte[]>, Void>() {
        @Nullable
        @Override
        public Void apply(@Nullable Map<byte[], byte[]> map) {
            return null;
        }
    };
    private final static int ID_SERVICE_LOOKUP_THREADS = 100;

    private final HTablePool pool;
    private final Deserializer<T> deserializer;
    private final byte[] uniqueCubeName;
    private final byte[] tableName;
    private final byte[] cf;
    private final IdService idService;
    private final ThreadPoolExecutor flushExecutor;
    private final CommitType commitType;
    private final int numIoeTries;
    private final int numCasTries;
    private final Timer flushSuccessTimer;
    private final Timer flushFailTimer;
    private final Timer singleWriteTimer;
    private final Histogram incrementSize;
    private final Histogram casTries;
    private final Timer multiGetTotalLatency;
    private final Timer multiGetCubeLatency;
    private final HbaseDbHarnessConfiguration configuration;

    private final Counter casRetriesExhausted;
    private final Timer iOExceptionsRetrySleepDuration;
    private final Function<Map<byte[], byte[]>, Void> onFlush;
    private final Set<Batch<T>> batchesInFlight = Sets.newHashSet();
    private final String metricsScope;
    private ThreadedIdServiceLookup idServiceLookup;

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType)
            throws IOException {
        this(pool, uniqueCubeName, tableName, cf, deserializer, idService, commitType, NOP, 5, 5, 10, null, 1);
    }

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType,
                          int numFlushThreads, int numIoeTries, int numCasTries, String metricsScope)
            throws IOException {
        this(pool, uniqueCubeName, tableName, cf, deserializer, idService, commitType, NOP,
                numFlushThreads, numIoeTries, numCasTries, metricsScope, 1);
    }

    public HBaseDbHarness(HbaseDbHarnessConfiguration configuration, HTablePool hTablePool, Deserializer<T> deserializer, IdService idService, Function<Map<byte[], byte[]>, Void> onFlush) throws IOException {
        this(hTablePool, configuration.uniqueCubeName, configuration.tableName, configuration.cf, deserializer, idService, configuration.commitType, onFlush, configuration.numFlushThreads, configuration.numIoeTries, configuration.numCasTries, configuration.metricsScope, configuration.batchSize);
    }

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType,
                          Function<Map<byte[], byte[]>, Void> onFlush, int numFlushThreads, int numIoeTries, int numCasTries,
                          String metricsScope, int batchSize) throws IOException {

        HbaseDbHarnessConfiguration hbaseDbHarnessConfiguration = HbaseDbHarnessConfiguration.newBuilder()
                .setUniqueCubeName(uniqueCubeName)
                .setTableName(tableName)
                .setCf(cf)
                .setCommitType(commitType)
                .setNumFlushThreads(numFlushThreads)
                .setNumIoeTries(numIoeTries)
                .setNumCasTries(numCasTries)
                .setMetricsScope(metricsScope)
                .setBatchSize(batchSize)
                .build();


        this.pool = pool;
        this.deserializer = deserializer;
        this.uniqueCubeName = uniqueCubeName;
        this.tableName = tableName;
        this.cf = cf;
        this.idService = idService;
        this.commitType = commitType;
        this.numIoeTries = numIoeTries;
        this.numCasTries = numCasTries;
        this.onFlush = onFlush;
        this.metricsScope = metricsScope;
        this.configuration = hbaseDbHarnessConfiguration;

        flushSuccessTimer = Metrics.timer(HBaseDbHarness.class, "successfulBatchFlush", metricsScope);
        flushFailTimer = Metrics.timer(HBaseDbHarness.class, "failedBatchFlush", metricsScope);
        singleWriteTimer = Metrics.timer(HBaseDbHarness.class, "singleWrites", metricsScope);
        incrementSize = Metrics.histogram(HBaseDbHarness.class, "incrementSize", metricsScope);
        casTries = Metrics.histogram(HBaseDbHarness.class, "casTries", metricsScope);
        casRetriesExhausted = Metrics.counter(HBaseDbHarness.class, "casRetriesExhausted", metricsScope);
        iOExceptionsRetrySleepDuration = Metrics.timer(HBaseDbHarness.class, "retrySleepDuration", metricsScope);
        multiGetTotalLatency = Metrics.timer(HBaseDbHarness.class, "multiGetTotalLatency", metricsScope);
        multiGetCubeLatency = Metrics.timer(HBaseDbHarness.class, "multiGetCubeLatency", metricsScope);

        String cubeName = new String(uniqueCubeName);
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(numFlushThreads);
        this.flushExecutor = new ThreadPoolExecutor(numFlushThreads, numFlushThreads, 1,
                TimeUnit.MINUTES, workQueue, new NamedThreadFactory("HBase DB flusher " + cubeName));
        this.idServiceLookup = new ThreadedIdServiceLookup(idService, ID_SERVICE_LOOKUP_THREADS, metricsScope);

        Metrics.gauge(HBaseDbHarness.class, "asyncFlushQueueDepth", metricsScope, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return flushExecutor.getQueue().size();
            }
        });

        Metrics.gauge(HBaseDbHarness.class, "asyncFlushersActive", metricsScope, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return flushExecutor.getActiveCount();
            }
        });
    }

    @Override
    public Optional<T> get(Address c) throws IOException, InterruptedException {
        final Optional<byte[]> maybeKey = c.toReadKey(idService);
        if (!maybeKey.isPresent()) {
            // If we fail to make the key, some mapped dimension value had never been seen.
            // In that case, there can't be a value in our backing store.
            return Optional.empty();
        }

        final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, maybeKey.get());
        Get get = new Get(rowKey);
        get.addFamily(cf);

        Result result = WithHTable.get(pool, tableName, get);
        if (result == null || result.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Returning absent for cube:" + Arrays.toString(uniqueCubeName) +
                        " for address:" + c + " key " + Base64.encodeBase64String(rowKey));
            }
            return Optional.empty();
        } else {
            T deserialized = deserializer.fromBytes(result.value());
            if (log.isDebugEnabled()) {
                log.debug("Returning value for cube:" + Arrays.toString(uniqueCubeName) + " address:" +
                        c + ": " + " key " + Base64.encodeBase64String(rowKey) + ": " + deserialized);
            }
            return Optional.of(deserialized);
        }
    }

    /**
     * Hands off the given batch to the flush executor to be sent to the database soon. Doesn't
     * throw IOException, since batches are just asynchronously submitted for execution, but
     * will throw AsyncException if some previous batch had a RuntimeException.
     *
     * @return a Future that the caller can use to detect when the database IO is done. The returned
     * future will never have an ExecutionException.
     */
    @Override
    public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute) throws FullQueueException {
        /* 
         * Since ThreadPoolExecutor throws RejectedExecutionException when its queue is full,
         * we have to backoff and retry if execute() throws RejectedExecutionHandler.
         *
         * This will cause all other threads writing to this cube to block. This is the desired
         * behavior since we want to stop accepting new writes past a certain point if we can't
         * send them to the database.
         */
        try {
            // Submit this batch
            synchronized (batchesInFlight) {
                batchesInFlight.add(batch);
            }
            return flushExecutor.submit(new FlushWorkerRunnable(batch, afterExecute));
        } catch (RejectedExecutionException ree) {
            throw new FullQueueException();
        }
    }

    private class FlushWorkerRunnable implements Callable<Object> {

        private static final long SLEEP_INTERVAL_MILLIS = 500;
        private static final long MAX_SLEEP_MILLIS = 1000 * 60 * 5;

        private final Batch<T> batch;
        private final AfterExecute<T> afterExecute;

        public FlushWorkerRunnable(Batch<T> batch, AfterExecute<T> afterExecute) {
            this.batch = batch;
            this.afterExecute = afterExecute;
        }

        @Override
        public Object call() throws Exception {
            IOException lastIOException = null;
            try {
                for (int attempt = 0; attempt < numIoeTries; attempt++) {
                    try {
                        flushBatch(batch);
                        afterExecute.afterExecute(null); // null => no exception
                        return null; // The return value of this callable is ignored
                    } catch (IOException e) {
                        lastIOException = e;
                        log.error("IOException in worker thread flushing to HBase on attempt " +
                                attempt + "/" + numIoeTries + ", will retry", e);

                        long retrySleepMillis = jitteredRetryMillis(attempt);
                        iOExceptionsRetrySleepDuration.update(retrySleepMillis, TimeUnit.MILLISECONDS);
                        Thread.sleep(retrySleepMillis);
                    }
                }
            } catch (Exception e) {
                afterExecute.afterExecute(e);
                throw e;
            } finally {
                synchronized (batchesInFlight) {
                    batchesInFlight.remove(batch);
                }
            }

            afterExecute.afterExecute(lastIOException);
            throw lastIOException;
        }

        private long jitteredRetryMillis(int attempt) {
            double jitter = 0.9 + (1.1 - 0.9) * RandomUtils.nextDouble();
            long retryMillis = SLEEP_INTERVAL_MILLIS * (attempt + 1);
            return Math.min(Math.round(retryMillis * jitter), MAX_SLEEP_MILLIS);
        }
    }

    private long increment(byte[] rowKey, T op) throws IOException {
        long amount = Bytes.toLong(op.serialize());
        incrementSize.update(amount);
        return WithHTable.increment(pool, tableName, rowKey, cf, QUALIFIER, amount);
    }

    /**
     * @param writes map from rowkey to the operation (which had better be bytes compatible with long)
     *
     * @return A map from bytes to the new values written to the database.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public Map<BoxedByteArray, byte[]> increment(Map<BoxedByteArray, T> writes) throws IOException, InterruptedException {
        ImmutableMap.Builder<BoxedByteArray, byte[]> successes = ImmutableMap.builder();

        List<Row> increments = new ArrayList<>();

        List<Map.Entry<BoxedByteArray, byte[]>> entries = writes.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<BoxedByteArray, byte[]>(e.getKey(), e.getValue().serialize()))
                .collect(Collectors.toList());


        for (Map.Entry<BoxedByteArray, byte[]> entry : entries) {
            long amount = Bytes.toLong(entry.getValue());
            Increment increment = new Increment(entry.getKey().bytes);
            increment.addColumn(cf, QUALIFIER, amount);
            incrementSize.update(amount);
            increments.add(increment);
        }

        Object[] objects = WithHTable.batch(pool, tableName, increments);

        for (int i = 0; i < objects.length; ++i) {
            if (objects[i] != null) {
                Result result = (Result) objects[i];
                byte[] value = result.getValue(cf, QUALIFIER);
                successes.put(entries.get(i).getKey(), value);
            }
        }

        return successes.build();
    }


    @SuppressWarnings("unchecked")
    private byte[] readCombineCas(byte[] rowKey, T newOp) throws IOException {
        Get get = new Get(rowKey);
        get.addColumn(cf, QUALIFIER);

        for (int i = 0; i < numCasTries; i++) {
            Result result = WithHTable.get(pool, tableName, get);

            byte[] prevSerializedOp = result.getValue(cf, QUALIFIER);
            T combinedOp;
            if (prevSerializedOp == null) {
                combinedOp = newOp;
            } else {
                T previousOp = deserializer.fromBytes(prevSerializedOp);
                combinedOp = (T) previousOp.add(newOp);
            }


            Put put = new Put(rowKey);
            byte[] serializedOp = combinedOp.serialize();
            put.add(cf, QUALIFIER, serializedOp);

            if (WithHTable.checkAndPut(pool, tableName, rowKey, cf, QUALIFIER, prevSerializedOp, put)) {
                casTries.update(i + 1);
                return serializedOp; // successful write
            } else {
                log.warn("checkAndPut failed on try " + (i + 1) + " out of " + numCasTries);
            }
        }

        casRetriesExhausted.inc();
        throw new IOException("Exhausted retries doing checkAndPut after " + numCasTries +
                " tries");
    }

    @Override
    public void set(Address c, T op) throws IOException, InterruptedException {
        final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, c.toWriteKey(idService));
        overwrite(rowKey, op);
    }

    private void overwrite(byte[] rowKey, T op) throws IOException {
        Put put = new Put(rowKey);
        put.add(cf, QUALIFIER, op.serialize());
        WithHTable.put(pool, tableName, put);
        if (log.isDebugEnabled()) {
            log.debug("Set of key " + Base64.encodeBase64String(rowKey));
        }
    }

    private void flushBatch(Batch<T> batch) throws IOException, InterruptedException {
        Map<Address, T> batchMap = batch.getMap();

        List<Address> successfulAddresses = Lists.newArrayListWithExpectedSize(batchMap.size());
        Map<byte[], byte[]> successfulRows = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        long nanoTimeBeforeBatch = System.nanoTime();

        try {
            if (commitType.equals(CommitType.INCREMENT) && configuration.batchSize > 1) {
                Iterable<List<Map.Entry<Address, T>>> partition = Iterables.partition(batchMap.entrySet(), configuration.batchSize);
                Map<BoxedByteArray, T> increments = new HashMap<>();
                Map<BoxedByteArray, Address> backwards = new HashMap<>();
                for (List<Map.Entry<Address, T>> entries : partition) {
                    final long nanoTimeBeforeWrite = System.nanoTime();
                    for (Map.Entry<Address, T> entry : entries) {
                        final Address address = entry.getKey();
                        final T op = entry.getValue();
                        final BoxedByteArray rowKey = new BoxedByteArray(ArrayUtils.addAll(uniqueCubeName, address.toWriteKey(idService)));
                        increments.put(rowKey, op);
                        backwards.put(rowKey, address);
                    }

                    Map<BoxedByteArray, byte[]> successes = increment(increments);

                    for (Map.Entry<BoxedByteArray, byte[]> entry : successes.entrySet()) {
                        successfulAddresses.add(backwards.get(entry.getKey()));
                        successfulRows.put(entry.getKey().bytes, entry.getValue());
                    }

                    long writeDurationNanos = System.nanoTime() - nanoTimeBeforeWrite;
                    singleWriteTimer.update(writeDurationNanos, TimeUnit.NANOSECONDS);
                }
            } else {
                for (Map.Entry<Address, T> entry : batchMap.entrySet()) {
                    final Address address = entry.getKey();
                    final T op = entry.getValue();
                    final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, address.toWriteKey(idService));
                    final long nanoTimeBeforeWrite = System.nanoTime();

                    byte[] dbBytes;
                    switch (commitType) {
                        case INCREMENT:
                            long postIncr = increment(rowKey, op);
                            dbBytes = Bytes.toBytes(postIncr);
                            break;
                        case READ_COMBINE_CAS:
                            dbBytes = readCombineCas(rowKey, op);
                            break;
                        case OVERWRITE:
                            overwrite(rowKey, op);
                            dbBytes = op.serialize();
                            break;
                        default:
                            throw new RuntimeException("Unsupported commit type " + commitType);
                    }

                    long writeDurationNanos = System.nanoTime() - nanoTimeBeforeWrite;
                    singleWriteTimer.update(writeDurationNanos, TimeUnit.NANOSECONDS);

                    if (log.isDebugEnabled()) {
                        log.debug("Succesfully wrote cube:" + Arrays.toString(uniqueCubeName) +
                                " address:" + address);
                    }
                    successfulAddresses.add(address);
                    successfulRows.put(rowKey, dbBytes);
                }
            }

            long batchDurationNanos = System.nanoTime() - nanoTimeBeforeBatch;
            flushSuccessTimer.update(batchDurationNanos, TimeUnit.NANOSECONDS);
        } catch (IOException ioe) {
            // There was an IOException while attempting to use the DB. If we were successful with some
            // of the pending writes, they should be removed from the batch so they are not retried later.
            // The operations that didn't get into the DB should be left in the batch to be retried later.
            log.warn("IOException when flushing batch to HBase", ioe);
            for (Address address : successfulAddresses) {
                batch.getMap().remove(address);
            }

            long batchDurationNanos = System.nanoTime() - nanoTimeBeforeBatch;
            flushFailTimer.update(batchDurationNanos, TimeUnit.NANOSECONDS);

            throw ioe;
        } finally {
            try {
                onFlush.apply(successfulRows);
            } catch (Exception e) {
                log.error("Unhandled exception in onFlush callback.", e);
            }
        }
    }

    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        Timer.Context totalLatency = multiGetTotalLatency.time();

        final int size = addresses.size();
        final List<Get> gets = Lists.newArrayListWithCapacity(size);
        final List<Optional<T>> resultsOptionals = Lists.newArrayListWithCapacity(size);
        final List<byte[]> rowKeys = Lists.newArrayListWithCapacity(size);
        final Set<Integer> unknownKeyPositions = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

        try {
            List<Optional<byte[]>> addressKeys = idServiceLookup.execute(addresses, unknownKeyPositions);
            for (Optional<byte[]> maybeKey : addressKeys) {
                if (maybeKey.isPresent()) {
                    final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, maybeKey.get());
                    rowKeys.add(rowKey);
                    Get get = new Get(rowKey);
                    get.addFamily(cf);
                    gets.add(get);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        Timer.Context multiGetLatency = multiGetCubeLatency.time();
        final Result[] results = WithHTable.get(pool, tableName, gets);
        multiGetLatency.stop();

        int resultPosition = 0;
        int outputPosition = 0;

        while (resultsOptionals.size() < size) {
            if (unknownKeyPositions.contains(outputPosition)) {
                resultsOptionals.add(Optional.<T>empty());
            } else {
                final Result result = results[resultPosition];

                if (result == null || result.isEmpty()) {
                    resultsOptionals.add(Optional.<T>empty());
                } else {
                    T deserialized = deserializer.fromBytes(result.value());
                    resultsOptionals.add(Optional.of(deserialized));
                }
                resultPosition++;

            }
            outputPosition++;
        }

        totalLatency.stop();
        return resultsOptionals;
    }

    @Override
    public void flush() throws InterruptedException {
        // Algorithm: get the set of batches that are in flight at the moment we start flushing.
        // As soon as all those batches are done, the "flush" is complete. Disregard all batches
        // that arrive after the flush starts.

        Set<Batch<T>> batchesToWaitFor;
        synchronized (batchesInFlight) {
            batchesToWaitFor = Sets.newHashSet(batchesInFlight);
        }
        while (true) {
            Set<Batch<T>> justFinished = Sets.newHashSet();
            synchronized (batchesInFlight) {
                for (Batch<T> batch : batchesToWaitFor) {
                    if (!batchesInFlight.contains(batch)) {
                        justFinished.add(batch);
                    }
                }
            }

            for (Batch<T> finishedBatch : justFinished) {
                batchesToWaitFor.remove(finishedBatch);
            }

            if (batchesToWaitFor.isEmpty()) {
                return;
            }

            Thread.sleep(100);
        }
    }


    public void shutdown() throws InterruptedException {
        /**
         * Flush, await for all inflight queues to finish, and then shutdown.
         */
        flush();
        flushExecutor.shutdown();
    }
}
