package com.urbanairship.datacube.dbharnesses;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.NamedThreadFactory;
import com.urbanairship.datacube.ops.LongOp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ParallelBatchDbHarness extends AbstractIdleService implements BatchDbHarness {
    final BatchDbHarness delegate;
    private final int threads;
    private final int batchSize;
    private String name;
    private long terminationSeconds;

    private Semaphore semaphore;
    private Consumer<Map<byte[], Long>> failureConsumer;
    private ExecutorService workers;

    public ParallelBatchDbHarness(BatchDbHarness delegate, int threads, int batchSize, String name, long terminationSeconds, Consumer<Map<byte[], Long>> failureConsumer) {
        this.delegate = delegate;
        this.threads = threads;
        this.batchSize = batchSize;
        this.name = name;
        this.terminationSeconds = terminationSeconds;
        this.semaphore = new Semaphore(threads);
        this.failureConsumer = failureConsumer;
    }

    @Override
    public void increment(Map<Address, LongOp> batch, RetryPolicy retryPolicy) throws InterruptedException, IOException {
        Set<Map.Entry<Address, LongOp>> entries = batch.entrySet();
        Iterable<List<Map.Entry<Address, LongOp>>> partition = Iterables.partition(entries, batchSize);

        for (List<Map.Entry<Address, LongOp>> entryList : partition) {
            Map<Address, LongOp> map = entryList.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            semaphore.acquire();
            Incrementer incrementerRunnable = new Incrementer(map, retryPolicy);
            CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(incrementerRunnable, workers);
            completableFuture.whenComplete((avoid, throwable) -> {
                semaphore.release();
            });
        }

    }

    private final class Incrementer implements Runnable {
        private final Map<Address, LongOp> batch;
        private final RetryPolicy retryPolicy;

        public Incrementer(Map<Address, LongOp> batch, RetryPolicy retryPolicy) {
            this.batch = batch;
            this.retryPolicy = retryPolicy;
        }

        @Override
        public void run() {
            try {
                delegate.increment(batch, retryPolicy);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void startUp() throws Exception {
        workers = Executors.newFixedThreadPool(threads, new NamedThreadFactory(name + " parallel db harness %d"));
    }

    @Override
    protected void shutDown() throws Exception {
        workers.shutdown();
        workers.awaitTermination(terminationSeconds, TimeUnit.MILLISECONDS);
    }
}
