/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.NamedThreadFactory;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private final Timer multiGetIdServicesLatency;
    private final Timer multiGetCubeLatency;
    private final Histogram multiGetIdServicesLatencyRatio;

    private final Counter casRetriesExhausted;
    private final Timer iOExceptionsRetrySleepDuration;
    private final Function<Map<byte[], byte[]>, Void> onFlush;
    private final Set<Batch<T>> batchesInFlight = Sets.newHashSet();

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType)
            throws IOException {
        this(pool, uniqueCubeName, tableName, cf, deserializer, idService, commitType, NOP, 5, 5, 10, null);
    }

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType,
                          int numFlushThreads, int numIoeTries, int numCasTries, String metricsScope)
            throws IOException {
        this(pool, uniqueCubeName, tableName, cf, deserializer, idService, commitType, NOP,
                numFlushThreads, numIoeTries, numCasTries, metricsScope);
    }

    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName,
                          byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType,
                          Function<Map<byte[], byte[]>, Void> onFlush, int numFlushThreads, int numIoeTries, int numCasTries,
                          String metricsScope) throws IOException {

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

        flushSuccessTimer = Metrics.timer(HBaseDbHarness.class, "successfulBatchFlush", metricsScope);
        flushFailTimer = Metrics.timer(HBaseDbHarness.class, "failedBatchFlush", metricsScope);
        singleWriteTimer = Metrics.timer(HBaseDbHarness.class, "singleWrites", metricsScope);
        incrementSize = Metrics.histogram(HBaseDbHarness.class, "incrementSize", metricsScope);
        casTries = Metrics.histogram(HBaseDbHarness.class, "casTries", metricsScope);
        casRetriesExhausted = Metrics.counter(HBaseDbHarness.class, "casRetriesExhausted", metricsScope);
        iOExceptionsRetrySleepDuration = Metrics.timer(HBaseDbHarness.class, "retrySleepDuration", metricsScope);
        multiGetIdServicesLatency = Metrics.timer(HBaseDbHarness.class, "multiGetIdServicesLatency", metricsScope);
        multiGetCubeLatency = Metrics.timer(HBaseDbHarness.class, "multiGetCubeLatency", metricsScope);
        multiGetIdServicesLatencyRatio = Metrics.histogram(HBaseDbHarness.class, "multiGetIdServicesLatencyRatio", metricsScope);

        String cubeName = new String(uniqueCubeName);
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(numFlushThreads);
        this.flushExecutor = new ThreadPoolExecutor(numFlushThreads, numFlushThreads, 1,
                TimeUnit.MINUTES, workQueue, new NamedThreadFactory("HBase DB flusher " + cubeName));

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
            return Optional.absent();
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
            return Optional.absent();
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
        Map<byte[], byte[]> successfulRows = Maps.newHashMap();

        long nanoTimeBeforeBatch = System.nanoTime();

        try {
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
        final int size = addresses.size();
        final List<Get> gets = Lists.newArrayListWithCapacity(size);
        final List<Optional<T>> resultsOptionals = Lists.newArrayListWithCapacity(size);
        final List<byte[]> rowKeys = Lists.newArrayListWithCapacity(size);
        final Set<Integer> unknownKeyPositions = Sets.newHashSet();
        long idServicesLatency = 0L;

        for (int i = 0; i < addresses.size(); i++) {
            Address address = addresses.get(i);

            try {
                long start = System.currentTimeMillis();
                final Optional<byte[]> maybeKey = address.toReadKey(idService);
                if (maybeKey.isPresent()) {
                    idServicesLatency += System.currentTimeMillis() - start;
                    final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, maybeKey.get());
                    rowKeys.add(rowKey);
                    Get get = new Get(rowKey);
                    get.addFamily(cf);
                    gets.add(get);
                } else {
                    unknownKeyPositions.add(i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        long startTime = System.currentTimeMillis();
        final Result[] results = WithHTable.get(pool, tableName, gets);
        long cubeReadLatency = System.currentTimeMillis() - startTime;
        long idServicesLatencyRatio = (long) (idServicesLatency / ((double) cubeReadLatency + idServicesLatency) * 100);

        multiGetIdServicesLatency.update(idServicesLatency, TimeUnit.MILLISECONDS);
        multiGetCubeLatency.update(cubeReadLatency, TimeUnit.MILLISECONDS);
        multiGetIdServicesLatencyRatio.update(idServicesLatencyRatio);

        int resultPosition = 0;
        int outputPosition = 0;

        while (resultsOptionals.size() < size) {
            if (unknownKeyPositions.contains(outputPosition)) {
                resultsOptionals.add(Optional.<T>absent());
            } else {
                final Result result = results[resultPosition];

                if (result == null || result.isEmpty()) {
                    resultsOptionals.add(Optional.<T>absent());
                } else {
                    T deserialized = deserializer.fromBytes(result.value());
                    resultsOptionals.add(Optional.of(deserialized));
                }
                resultPosition++;

            }
            outputPosition++;
        }

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
}
