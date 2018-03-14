package com.urbanairship.datacube;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.urbanairship.datacube.dbharnesses.BatchDbHarness;
import com.urbanairship.datacube.metrics.Metrics;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * converts bytes to maps of long ops, buffers until we have achieved the configured number of updates and then attempts
 * to flush to the underlying database.
 */
public class AsyncIncrementer extends AbstractScheduledService implements Function<byte[], CompletableFuture<Void>> {
    private final long maxAge;
    private final DataCube<LongOp> dataCube;
    private final Function<byte[], WriteBuilder> serializer;
    private BatchDbHarness batchDbHarness;
    private Function<ThreadsafeBatch.Update<?, ?>, BatchDbHarness.RetryPolicy> retryPolicyFactory;

    private static final Logger log = LogManager.getLogger(AsyncIncrementer.class);

    private final ThreadsafeBatch<LongOp> batch;

    public AsyncIncrementer(long maxAge,
                            int maxSize,
                            DataCube<LongOp> dataCube,
                            Function<byte[], WriteBuilder> serializer,
                            BatchDbHarness batchDbHarness,
                            Function<ThreadsafeBatch.Update<?, ?>, BatchDbHarness.RetryPolicy> retryPolicyFactory
    ) {
        this.maxAge = maxAge;
        this.dataCube = dataCube;
        this.serializer = serializer;
        this.batchDbHarness = batchDbHarness;
        this.retryPolicyFactory = retryPolicyFactory;
        this.batch = new ThreadsafeBatch<LongOp>(maxAge, maxSize, (a, b) -> (LongOp) a.add(b));

        Metrics.gauge(AsyncIncrementer.class, "events pending", batch::getPending);
        Metrics.gauge(AsyncIncrementer.class, "events written", batch::getTotalUpdatesSeen);
        Metrics.gauge(AsyncIncrementer.class, "rows written", batch::getRowWrites);
        Metrics.gauge(AsyncIncrementer.class, "rows seen", batch::getRowsSeen);
    }

    public CompletableFuture<Void> apply(byte[] event) {
        try {
            WriteBuilder writes = serializer.apply(event);
            Batch<LongOp> batch = dataCube.getWrites(writes, new LongOp(1));
            return addBatch(batch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    private CompletableFuture<Void> addBatch(Batch<LongOp> batch) throws InterruptedException, IOException {
        // it could have filled up in the meantime.
        Preconditions.checkState(isRunning(), "service must be running to add a batch to it");
        while (isRunning()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            Optional<CompletableFuture<Void>> future = this.batch.offer(batch.getMap());
            if (future.isPresent()) {
                return future.get();
            }
            flush();
            // then try again.
        }
        // it will never complete.
        return new CompletableFuture<>();
    }

    private void flush() throws InterruptedException, IOException {
        Optional<ThreadsafeBatch.Update<Address, LongOp>> maybeUpdate = this.batch.drain();
        if (maybeUpdate.isPresent()) {
            ThreadsafeBatch.Update<Address, LongOp> update = maybeUpdate.get();
            batchDbHarness.increment(update.getMap(), retryPolicyFactory.apply(update));
        }
    }

    @Override
    protected void runOneIteration() throws InterruptedException, IOException {
        if (this.batch.isFull()) flush();
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(maxAge, maxAge, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws InterruptedException, IOException {
        while (!batch.isEmpty()) flush();
    }
}
