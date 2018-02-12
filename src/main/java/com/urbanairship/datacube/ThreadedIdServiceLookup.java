package com.urbanairship.datacube;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class ThreadedIdServiceLookup implements Closeable {
    private static final Logger log = LogManager.getLogger(ThreadedIdServiceLookup.class);

    private Timer LATENCY_TIMER;
    private Histogram BATCH_SIZE_HISTO;

    private final ExecutorService executorService;
    private final IdService idService;

    /**
     * This utility class provides threaded Address => Row Key lookups for a list of addresses
     * against an IdService implementation
     *
     * @param idService id service that lookups will be executed against
     * @param threads concurrency level
     * @param metricsScope name used for metrics and threads
     */
    public ThreadedIdServiceLookup(IdService idService, int threads, String metricsScope) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(metricsScope + " threaded id service lookup %d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.error("Uncaught error from id service lookup thread", e);
                    }
                })
                .build();

        this.executorService = Executors.newFixedThreadPool(threads, threadFactory);
        this.idService = idService;

        LATENCY_TIMER = Metrics.timer(ThreadedIdServiceLookup.class, "latency", metricsScope);
        BATCH_SIZE_HISTO = Metrics.histogram(ThreadedIdServiceLookup.class, "batch_size", metricsScope);
    }

    public List<Optional<byte[]>> execute(List<Address> addresses, Set<Integer> unknownKeyPositions) throws IOException, InterruptedException {
        Timer.Context timer = LATENCY_TIMER.time();
        BATCH_SIZE_HISTO.update(addresses.size());

        List<Callable<Optional<byte[]>>> callableList = new ArrayList<Callable<Optional<byte[]>>>(addresses.size());
        List<Optional<byte[]>> keys = new ArrayList<Optional<byte[]>>(addresses.size());

        for (int i = 0; i < addresses.size(); i++) {
            Address address = addresses.get(i);
            callableList.add(new ReadKeyCallable(idService, address, i, unknownKeyPositions));
        }

        try {
            List<Future<Optional<byte[]>>> futures = executorService.invokeAll(callableList);
            for (Future<Optional<byte[]>> future : futures) {
                Optional<byte[]> key = future.get();
                keys.add(key);
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            if (e.getCause() instanceof InterruptedException) {
                throw (InterruptedException) e.getCause();
            }

            throw new RuntimeException(e.getCause());
        }

        timer.stop();
        return keys;
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }

    private class ReadKeyCallable implements Callable<Optional<byte[]>> {

        private final IdService idService;
        private final Address address;
        private final int index;
        private final Set<Integer> unknownKeyPositions;

        private ReadKeyCallable(IdService idService, Address address, int index, Set<Integer> unknownKeyPositions) {
            this.idService = idService;
            this.address = address;
            this.index = index;
            this.unknownKeyPositions = unknownKeyPositions;
        }

        @Override
        public Optional<byte[]> call() throws Exception {
            final Optional<byte[]> maybeKey = address.toReadKey(idService);
            if (!maybeKey.isPresent()) {
                unknownKeyPositions.add(index);
            }

            return maybeKey;
        }
    }
}
