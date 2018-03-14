package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.urbanairship.datacube.ops.LongOp;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ThreadsafeBatchTest {
    @org.junit.Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testQueToBatch() throws Exception {
        LinkedBlockingQueue<ThreadsafeBatch.Update<Long, Long>> updates = new LinkedBlockingQueue<>();

        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(1L, 1L), System.currentTimeMillis(), new CompletableFuture<>()));
        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(1L, 1L), System.currentTimeMillis(), new CompletableFuture<>()));
        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(3L, 2L), System.currentTimeMillis(), new CompletableFuture<>()));

        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(1L, 1L), System.currentTimeMillis(), new CompletableFuture<>()));
        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(5L, 2L), System.currentTimeMillis(), new CompletableFuture<>()));
        updates.add(new ThreadsafeBatch.Update<>(ImmutableMap.of(1L, 2L, 9L, 2L, 4L, 1L), System.currentTimeMillis(), new CompletableFuture<>()));

        AtomicLong drained = new AtomicLong();

        ThreadsafeBatch.Update<Long, Long> firstBatch = ThreadsafeBatch.<Long, Long>condense(updates, Long::sum, 3, drained).get();

        assertEquals(2, firstBatch.getMap().size());
        assertEquals(firstBatch.getMap().get(1L).longValue(), 2L);
        assertEquals(firstBatch.getMap().get(3L).longValue(), 2L);

        Map<Long, Long> secondBatch = ThreadsafeBatch.<Long, Long>condense(updates, Long::sum, 3, drained).get().getMap();
        assertEquals(secondBatch.size(), 4);
        assertEquals(secondBatch.get(1L).longValue(), 3L);
        assertEquals(secondBatch.get(5L).longValue(), 2L);
        assertEquals(secondBatch.get(4L).longValue(), 1L);
        assertEquals(secondBatch.get(9L).longValue(), 2L);
        assertTrue(updates.isEmpty());

        assertEquals(drained.get(), 6);

        assertFalse(ThreadsafeBatch.<Long, Long>condense(updates, Long::sum, 2, drained).isPresent());
    }

    @org.junit.Test
    public void test() throws Exception {

        ExecutorService threads = Executors.newFixedThreadPool(100);

        Bucketer<CSerializable> bucketer = new Bucketer.IdentityBucketer();

        Dimension dimension = new Dimension<CSerializable>("dim", bucketer, false, Long.BYTES);

        DataCube<LongOp> cube = new DataCube<LongOp>(ImmutableList.<Dimension<?>>of(dimension), ImmutableList.<Rollup>of(new Rollup(dimension)));

        ThreadsafeBatch<LongOp> batch = new ThreadsafeBatch<LongOp>(1000, 90, (a, b) -> new LongOp(a.getLong() + b.getLong()));

        ConcurrentHashMap<Address, Long> expected = new ConcurrentHashMap<>();

        int each = 10;
        int parallelism = 10;

        AtomicInteger expectedCount = new AtomicInteger(5 * parallelism * each);


        Address one = address(dimension, cube, 1);
        Address two = address(dimension, cube, 2);
        Address three = address(dimension, cube, 3);
        Address four = address(dimension, cube, 4);
        Address five = address(dimension, cube, 5);

        expected.put(one, (long) parallelism * each * 5);
        expected.put(two, (long) parallelism * each * 4);
        expected.put(three, (long) parallelism * each * 3);
        expected.put(four, (long) parallelism * each * 2);
        expected.put(five, (long) parallelism * each * 1);


        CompletableFuture producers = new CompletableFuture();
        producers.complete(null);
        for (int i = 0; i < parallelism; ++i) {
            producers = CompletableFuture.allOf(
                    producers,
                    CompletableFuture.supplyAsync(new Adder(each, batch, one), threads),
                    CompletableFuture.supplyAsync(new Adder(each, batch, one, two), threads),
                    CompletableFuture.supplyAsync(new Adder(each, batch, one, two, three), threads),
                    CompletableFuture.supplyAsync(new Adder(each, batch, one, two, three, four), threads),
                    CompletableFuture.supplyAsync(new Adder(each, batch, one, two, three, four, five), threads)
            );
        }

        threads.shutdown();

        ImmutableMap.Builder<Address, Long> builder = ImmutableMap.<Address, Long>builder();
        LinkedBlockingQueue<ThreadsafeBatch.Update<Address, LongOp>> queue = new LinkedBlockingQueue<>();

        AtomicBoolean keepGoing = new AtomicBoolean(true);

        CompletableFuture<Void> consumers = new CompletableFuture<Void>();
        consumers.complete(null);
        for (int i = 0; i < parallelism; ++i) {
            CompletableFuture.allOf(
                    consumers,
                    CompletableFuture.runAsync(() -> {
                        while (keepGoing.get() || !batch.isEmpty()) {
                            try {
                                Thread.sleep(100);
                                queue.put(batch.drain().get());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    })
            );
        }

        producers.thenRun(() -> keepGoing.set(false));

        producers.join();
        consumers.join();

        queue.put(batch.drain().get());
        ThreadsafeBatch.condense(queue, (a, b) -> (LongOp) a.add(b), expectedCount.get(), new AtomicLong())
                .get()
                .getMap().forEach((key, value) -> builder.put(key, value.getLong()));


        assertEquals(0, batch.getPending());
        assertEquals(expectedCount.get(), batch.getTotalUpdatesSeen());

        assertEquals(batch.toString(), ImmutableSet.copyOf(expected.entrySet()), ImmutableSet.copyOf(builder.build().entrySet()));
    }

    private Address address(Dimension dimension, DataCube<LongOp> cube, int finalI) {
        final Address address = new Address(cube);
        address.at(dimension, Ints.toByteArray(finalI));
        return address;
    }

    private static class Adder implements Supplier<Boolean> {
        private final int each;
        private final ThreadsafeBatch<LongOp> batch;
        private Address[] address;

        public Adder(int each, ThreadsafeBatch<LongOp> batch, Address... address) {
            this.each = each;
            this.batch = batch;
            this.address = address;
        }

        @Override
        public Boolean get() {
            for (int i = 0; i < each; ++i) {
                try {
                    ImmutableMap.Builder<Address, LongOp> builder = ImmutableMap.builder();

                    for (Address a : address) {
                        builder.put(a, new LongOp(1));
                    }

                    batch.put(builder.build());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return true;
        }
    }
}