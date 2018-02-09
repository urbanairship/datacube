package com.urbanairship.datacube;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.urbanairship.datacube.metrics.Metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ThreadedIdServiceLookup {

    private Timer LATENCY_TIMER;
    private Histogram BATCH_SIZE_HISTO;

    private final IdService idService;
    private final Set<Integer> unknownKeyPositions;
    private final int threads;

    /**
     * This utility class provides threaded Address => Row Key lookups for a list of addresses
     * against an IdService implementation
     *
     * @param idService id service that lookups will be executed against
     * @param unknownKeyPositions this should be a threadsafe implementation of a Set and will be populated with failed lookups
     * @param threads concurrency level
     */
    public ThreadedIdServiceLookup(IdService idService, Set<Integer> unknownKeyPositions, int threads, String metricsScope) {
        this.idService = idService;
        this.threads = threads;
        this.unknownKeyPositions = unknownKeyPositions;

        LATENCY_TIMER = Metrics.timer(ThreadedIdServiceLookup.class, "latency", metricsScope);
        BATCH_SIZE_HISTO = Metrics.histogram(ThreadedIdServiceLookup.class, "batch_size", metricsScope);
    }

    public List<Optional<byte[]>> execute(List<Address> addresses) throws IOException, InterruptedException {
        Timer.Context timer = LATENCY_TIMER.time();
        BATCH_SIZE_HISTO.update(addresses.size());

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        List<Callable<Optional<byte[]>>> callableList = new ArrayList<Callable<Optional<byte[]>>>(addresses.size());
        List<Optional<byte[]>> keys = new ArrayList<Optional<byte[]>>(addresses.size());

        for (int i = 0; i < addresses.size(); i++) {
            Address address = addresses.get(i);
            callableList.add(new ReadKeyCallable(idService, address, i));
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

            throw new RuntimeException(e);
        }

        executorService.shutdown();

        timer.stop();
        return keys;
    }

    private class ReadKeyCallable implements Callable<Optional<byte[]>> {

        private final IdService idService;
        private final Address address;
        private final int index;

        private ReadKeyCallable(IdService idService, Address address, int index) {
            this.idService = idService;
            this.address = address;
            this.index = index;
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
