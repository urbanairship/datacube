/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.dbharnesses.AfterExecute;
import com.urbanairship.datacube.dbharnesses.FullQueueException;
import com.urbanairship.datacube.metrics.Metrics;
import com.urbanairship.datacube.ops.IntOp;
import com.urbanairship.datacube.ops.LongOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A DataCube does no IO, it merely returns batches that can be executed. This class wraps
 * around a DataCube and does IO against a storage backend.
 * <p>
 * Thread safe. Writes can block for a long time if another thread is flushing to the database.
 * <p>
 * <h3>HOW INTERNAL ASYNC BATCH FLUSHING WORKS (for datacube developers, not clients):</h3>
 * <p>
 * Batches accumulate here, in DataSyncIo objects. When they pass their size or age threshold, they
 * are sent to the underlying DbHarness to be saved to the database. This is where it gets weird,
 * since the DbHarness layer is completely asynchronous and non-blocking.
 * <p>
 * When a Batch is sent to the DbHarness to be saved, either we get back a Future, or we get a
 * FullQueueException which means that the DbHarness cannot currently accept more batches for saving.
 * <p>
 * If we get back a Future, then one of two things will happen:
 * <p>
 * <ul>
 * <li>If the client requested a blocking write using writeSync(), we block and wait for the Future
 * to complete.</li>
 * <li>If the client requested a non-blocking write using writeAsync(), we will return new Future
 * that will wait for the DbHarness flush Future to complete.
 * </ul>
 * <p>
 * <h3>Error handling</h3>
 * <p>
 * If batch flushing encounters an exception during a writeSync(), the exception will be thrown to
 * the caller. If batch flushing encounters an exception during a writeAsync(), we can't throw the
 * exception back to the caller since the writeAsync() call has already returned. We make sure the
 * caller handles the error by doing two things.
 * <p>
 * First, we rethrow the exception from the Future returned by writeAsync(), which the caller will see
 * as an ExecutionException thrown by Future.get(). This alone is not sufficient though, because the
 * caller may never call Future.get().
 * <p>
 * Second, when an asynchronous batch flush has an exception, the exception is saved in
 * {@link #asyncException}. When {@link #asyncException} is non-null, all future calls to writeAsync()
 * will throw AsyncException and refuse to write. This prevents clients blithely throwing away data
 * if the underlying database is stuck in a bad state.
 */
public class DataCubeIo<T extends Op> {
    private static final Logger log = LoggerFactory.getLogger(DataCubeIo.class);

    private final DbHarness<T> db;
    private final DataCube<T> cube;
    private final int batchSize;
    private final long maxBatchAgeMs;
    private final SyncLevel syncLevel;
    private AsyncException asyncException = null;

    private final Object lock = new Object();

    // This executor will wait for DB writes to complete then check if they had an error.
    private final ThreadPoolExecutor asyncErrorMonitorExecutor;

    private final String metricsScope;
    private final Meter writesMeter;
    private final Meter asyncQueueBackoffMeter;
    private final Meter runBatchMeter;
    private final Meter ageFlushes;
    private final Meter sizeFlushes;

    private final boolean perRollupMetrics;
    private final Counter writeAddrWithoutRollup;
    private final ConcurrentMap<Rollup, Counter> rollupReadNotFound = Maps.newConcurrentMap();
    private final ConcurrentMap<Rollup, Counter> rollupReadCount = Maps.newConcurrentMap();
    private final ConcurrentMap<Rollup, Counter> rollupWriteIndividual = Maps.newConcurrentMap();
    private final ConcurrentMap<Rollup, Histogram> rollupReadSize = Maps.newConcurrentMap();
    private final ConcurrentMap<Rollup, Histogram> rollupWriteSize = Maps.newConcurrentMap();

    private Batch<T> batchInProgress = new Batch<T>();
    private long batchFlushDeadlineMs;

    /**
     * @param batchSize     if after doing a write the number of rows to be written to the database
     *                      exceeds this number, a flush will be done.
     * @param maxBatchAgeMs if after doing a write the batch's oldest write was more than this long ago,
     *                      a flush will be done. This is not a hard ceiling on the age of writes in the batch, because
     *                      the batch will not be flushed until the *next* write arrives after the timeout.
     */
    public DataCubeIo(DataCube<T> cube, DbHarness<T> db, int batchSize, long maxBatchAgeMs,
                      SyncLevel syncLevel, String metricsScope, boolean perRollupMetrics) {
        this.cube = cube;
        this.db = db;
        this.batchSize = batchSize;
        this.maxBatchAgeMs = maxBatchAgeMs;
        this.syncLevel = syncLevel;

        this.perRollupMetrics = perRollupMetrics;
        this.metricsScope = metricsScope;
        writesMeter = Metrics.meter(DataCubeIo.class, "writes", metricsScope);
        asyncQueueBackoffMeter = Metrics.meter(DataCubeIo.class, "backoffMeter", metricsScope);
        runBatchMeter = Metrics.meter(DataCubeIo.class, "runBatchMeter", metricsScope);
        ageFlushes = Metrics.meter(DataCubeIo.class, "flushesDueToAge", metricsScope);
        sizeFlushes = Metrics.meter(DataCubeIo.class, "flushesDueToSize", metricsScope);
        writeAddrWithoutRollup = Metrics.counter(DataCubeIo.class, "writeAddrWithoutRollup", metricsScope);

        this.asyncErrorMonitorExecutor = new ThreadPoolExecutor(Integer.MAX_VALUE,
                Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("DataCubeIo async DB watcher"));

        if (perRollupMetrics) {
            cube.getRollups().forEach(rollup -> rollupReadCount.put(rollup, Metrics.counter(
                    DataCubeIo.class, metricsScope + "_rollupReadCount", rollup.getMetricName()
            )));
        }

        Metrics.gauge(DataCubeIo.class, "errorMonitorActiveCount", metricsScope, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return asyncErrorMonitorExecutor.getActiveCount();
            }
        });

    }

    /**
     * Do some writes into the in-memory batch, possibly flushing to the backing database, depending
     * on the {@link SyncLevel}.
     *
     * @return If the operation did not cause a database flush, Optional.absent will be returned.
     * Otherwise, if a batch flush is triggered, a Future will be returned. This Future's get()
     * method will return null when the database flush is finished, and rethrow any exceptions as
     * ExecutionExceptions (see {@link java.util.concurrent.Future}).
     */
    public Optional<Future<?>> writeAsync(T op, WriteBuilder at) throws AsyncException, InterruptedException {
        if (asyncException != null) {
            throw asyncException;
        }

        Batch<T> newBatch = cube.getWrites(at, op);

        return writeAsync(newBatch);
    }

    public Optional<Future<?>> writeAsync(Batch<T> newBatch) throws AsyncException, InterruptedException {
        writesMeter.mark();

        Batch<T> batchToFlush = null;

        if (perRollupMetrics) {
            newBatch.getMap().keySet().forEach(addr -> {
                if (addr.getSourceRollup().isPresent()) {
                    updateRollupCounter(rollupWriteIndividual, addr.getSourceRollup().get(), "rollupIndividualWrite");
                } else {
                    writeAddrWithoutRollup.inc();
                }
            });
        }

        switch (syncLevel) {
            case FULL_SYNC:
                // At this sync level we flush all batches immediately. No sharing between threads.
                batchToFlush = newBatch;
                break;
            case BATCH_ASYNC:
            case BATCH_SYNC:
                synchronized (lock) {
                    if (batchInProgress.getMap().isEmpty()) {
                        // Start the timer for this batch, it should be flushed when it becomes old
                        long nowTimeMs = System.currentTimeMillis();
                        batchFlushDeadlineMs = nowTimeMs + maxBatchAgeMs; // Flush when we reach this timestamp

                        // If the flush deadline timestamp overflowed its long, set it back to the largest
                        // possible value. This will occur if the client passes a very large max batch age.
                        if (batchFlushDeadlineMs < nowTimeMs) {
                            batchFlushDeadlineMs = Long.MAX_VALUE;
                        }
                    }
                    batchInProgress.putAll(newBatch);

                    boolean shouldFlush = false;

                    if (batchInProgress.getMap().size() >= batchSize) {
                        sizeFlushes.mark();
                        shouldFlush = true;
                    } else if (System.currentTimeMillis() >= batchFlushDeadlineMs) {
                        ageFlushes.mark();
                        shouldFlush = true;
                    }

                    if (shouldFlush) {
                        batchToFlush = batchInProgress;
                        batchInProgress = new Batch<T>();
                    }
                }
                break;
            default:
                throw new RuntimeException("Unknown sync level " + syncLevel);

        }

        if (batchToFlush != null) {
            return Optional.<Future<?>>of(runBatch(batchToFlush));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Hand off a batch to the DbHarness layer, retrying on FullQueueException.
     */
    private Future<?> runBatch(Batch<T> batch) throws InterruptedException {
        while (true) {
            try {
                runBatchMeter.mark();
                if (perRollupMetrics) {
                    batch.getMap().forEach((addr, op) -> {
                        if (addr.getSourceRollup().isPresent()) {
                            updateRollupHistogram(rollupWriteSize, addr.getSourceRollup().get(), "rollupWriteSize", op);
                        }
                    });
                }

                return db.runBatchAsync(batch, flushErrorHandler);
            } catch (FullQueueException e) {
                asyncQueueBackoffMeter.mark();

                // Sleeping and retrying like this means batches may be flushed out of order
                log.debug("Async queue is full, retrying soon");
                Thread.sleep(100);
            }
        }
    }

    /**
     * If the {@link SyncLevel} is {@link SyncLevel#FULL_SYNC} or {@link SyncLevel#BATCH_SYNC}, then
     * no asynchronous IO is happening. You can use this function to write instead of
     * {@link #writeAsync(Op, WriteBuilder)} without catching AsyncException or InterruptedException.
     * <p>
     * IMPORTANT: the name of this function does not imply that the write is immediately flushed to
     * the database. This is only true if {@link SyncLevel#FULL_SYNC} is set. Otherwise your write
     * is probably just staged in a batch for writing later.
     * <p>
     * You can only use this function if this DataCubeIo was constructed with
     * {@link SyncLevel#FULL_SYNC} or {@link SyncLevel#BATCH_SYNC}.
     */
    public void writeSync(T op, WriteBuilder at) throws IOException, InterruptedException {
        if (syncLevel == SyncLevel.BATCH_ASYNC) {
            throw new IllegalArgumentException("You can't use WriteSync for this cube with " +
                    "SyncLevel " + syncLevel);
        }

        Optional<Future<?>> optFuture;
        try {
            optFuture = writeAsync(op, at);
        } catch (AsyncException pe) {
            throw new RuntimeException("Internal error, when at a synchronized syncLevel there should" +
                    " be no asynchronous exceptions");
        }

        if (optFuture.isPresent()) {
            // Our write triggered a batch flush. Wait for it to finish, rethrowing exceptions.
            try {
                optFuture.get().get();
            } catch (ExecutionException ee) {
                Throwable flushException = ee.getCause();
                if (flushException instanceof IOException) {
                    throw (IOException) flushException;
                } else if (flushException instanceof InterruptedException) {
                    throw (InterruptedException) flushException;
                } else if (flushException instanceof RuntimeException) {
                    throw new RuntimeException(flushException);
                } else {
                    throw new RuntimeException("Unreachable");
                }
            }
        } else {
            // Our write updated a batch in memory without causing a flush. Return success.
        }
    }

    /**
     * @return absent if the bucket doesn't exist, or the bucket if it does.
     */
    public Optional<T> get(Address addr) throws IOException, InterruptedException {
        cube.checkValidReadOrThrow(addr);

        final Optional<T> result = db.get(addr);
        if (perRollupMetrics && addr.getSourceRollup().isPresent()) {
            updateRollupCounter(rollupReadCount, addr.getSourceRollup().get(), "rollupReadCount");
            if (result.isPresent()) {
                updateRollupHistogram(rollupReadSize, addr.getSourceRollup().get(), "rollupReadSize", result.get());
            } else {
                updateRollupCounter(rollupReadNotFound, addr.getSourceRollup().get(), "rollupReadNotFound");
            }
        }

        return result;
    }

    public Optional<T> get(ReadBuilder readBuilder) throws IOException, InterruptedException {
        return this.get(readBuilder.build());
    }

    public List<Optional<T>> multiGet(List<ReadBuilder> readBuilders) throws IOException, InterruptedException {
        List<Address> addresses = Lists.newArrayListWithCapacity(readBuilders.size());
        for (ReadBuilder readBuilder : readBuilders) {
            Address address = readBuilder.build();
            cube.checkValidReadOrThrow(address);
            addresses.add(address);
        }

        final List<Optional<T>> results = db.multiGet(addresses);
        if (perRollupMetrics) {
            for (int i = 0; i < results.size(); i++) {
                final Optional<T> result = results.get(i);
                final Address addr = addresses.get(i);
                if (!addr.getSourceRollup().isPresent()) {
                    //Should never happen, but be paranoid
                    continue;
                }

                updateRollupCounter(rollupReadCount, addr.getSourceRollup().get(), "rollupReadCount");
                if (result.isPresent()) {
                    updateRollupHistogram(rollupReadSize, addr.getSourceRollup().get(), "rollupReadSize", result.get());
                } else {
                    updateRollupCounter(rollupReadNotFound, addr.getSourceRollup().get(), "rollupReadNotFound");
                }
            }
        }

        return results;
    }

    public void flush() throws InterruptedException {
        Batch<T> batchToFlush;
        synchronized (lock) {
            batchToFlush = batchInProgress;
            batchInProgress = new Batch<T>();
        }
        runBatch(batchToFlush);
        db.flush();
    }

    private AfterExecute<T> flushErrorHandler = new AfterExecute<T>() {
        @Override
        public void afterExecute(Throwable t) {
            if (t != null) {
                asyncException = new AsyncException(t);
                log.error("Putting DataCubeIo into an error state due to flush exception", t);
            }
        }
    };

    private void updateRollupCounter(Map<Rollup, Counter> counters, Rollup rollup, String metric) {
        if (!perRollupMetrics) {
            return;
        }

        final Counter counter = counters.computeIfAbsent(rollup, ru ->
                Metrics.counter(DataCubeIo.class, metricsScope + "_" + metric, rollup.getMetricName()));
        counter.inc();
    }

    private void updateRollupHistogram(Map<Rollup, Histogram> histograms, Rollup rollup, String metric, T op) {
        if (!perRollupMetrics) {
            return;
        }

        final long opNumericValue;
        if (op instanceof LongOp) {
            opNumericValue = ((LongOp) op).getLong();
        } else if (op instanceof IntOp) {
            opNumericValue = ((IntOp) op).getInt();
        } else {
            return;
        }

        final Histogram histogram = histograms.computeIfAbsent(rollup, ru ->
                Metrics.histogram(DataCubeIo.class, metricsScope + "_" + metric, rollup.getMetricName()));
        histogram.update(opNumericValue);
    }
}
