package com.urbanairship.datacube;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;

/**
 * Multiple threads can put against or drain the batch at once.
 *
 * @param <T>
 */
public class ThreadsafeBatch<T> {
    private final BlockingQueue<Update<Address, T>> updates;
    private final BinaryOperator<T> binaryOperator;

    private final long maxAge;
    private final int maxSize;

    private final AtomicLong drained = new AtomicLong();
    private final AtomicLong totalUpdatesSeen = new AtomicLong();

    private final AtomicLong rowsUpdated = new AtomicLong();
    private final AtomicLong rowsSeen = new AtomicLong();

    /**
     * @param maxAge         The maximum age before we will reports "full" to clients who ask
     * @param maxSize        The maximum number of batches before we block waiting flush, and maximum number of elements
     *                       we
     *                       will pull out of the queue when we flush.
     * @param binaryOperator
     */
    public ThreadsafeBatch(long maxAge, int maxSize, BinaryOperator<T> binaryOperator) {
        this.maxAge = maxAge;
        this.maxSize = maxSize;
        this.binaryOperator = binaryOperator;
        this.updates = new LinkedBlockingQueue<Update<Address, T>>(maxSize);
    }

    public Optional<CompletableFuture<Void>> offer(Map<Address, T> other) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        boolean offer = updates.offer(new Update<>(other, System.currentTimeMillis(), future));
        if (!offer) {
            return Optional.empty();
        }
        mark(other);
        return Optional.of(future);
    }

    public CompletableFuture<Void> put(Map<Address, T> other) throws InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        updates.put(new Update<>(other, System.currentTimeMillis(), future));
        mark(other);
        return future;
    }

    private void mark(Map<Address, T> other) {
        totalUpdatesSeen.incrementAndGet();
        rowsSeen.addAndGet(other.size());
    }

    public long getPending() {
        return Math.max(totalUpdatesSeen.get() - drained.get(), updates.size());
    }

    public long getTotalUpdatesSeen() {
        return totalUpdatesSeen.get();
    }

    public long getRowWrites() {
        return rowsUpdated.get();
    }

    public long getRowsSeen() {
        return rowsSeen.get();
    }

    public Optional<Update<Address, T>> drain() {
        Optional<Update<Address, T>> update = condense(updates, binaryOperator, maxSize, drained);
        update.ifPresent(u -> rowsUpdated.addAndGet(u.getMap().size()));
        return update;
    }

    /**
     * Compact the list of updates to a single updates, resolving cell collisions with {@param binaryOperator}
     *
     * @param updates        A queue of updates that supports concurrent access
     * @param binaryOperator Applied to values elements to resolve key collisions
     * @param maxSize        The maximum number of udpates we'll consider
     * @param drained        We'll increment this with the number of elements we drained
     * @param <K>            The key type of the Update's internal map.
     * @param <V>            The value type of the Updates internal map
     *
     * @return present if there were any updates to condense, absent otherwise. A single update whose internal map
     * represents all the update operations condensed.
     */
    public static <K, V> Optional<Update<K, V>> condense(BlockingQueue<Update<K, V>> updates, BinaryOperator<V> binaryOperator, int maxSize, AtomicLong drained) {
        Update<K, V> first = updates.poll();
        if (first != null) {
            Collection<Update<K, V>> list = new ArrayList<>();
            list.add(first);
            updates.drainTo(list, maxSize - 1);
            drained.addAndGet(list.size());
            return Optional.of(condense(binaryOperator, list));
        }
        return Optional.empty();
    }

    private static <K, V> Update<K, V> condense(BinaryOperator<V> binaryOperator, Collection<Update<K, V>> list) {
        HashMap<K, V> builder = new HashMap<>();
        long oldestEventBirthday = 0;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Update<K, V> update : list) {
            oldestEventBirthday = Math.min(oldestEventBirthday, update.birthday);
            futures.add(update.future);
            for (Map.Entry<K, V> entry : update.map.entrySet()) {
                builder.merge(entry.getKey(), entry.getValue(), binaryOperator);
            }
        }
        return new Update<K, V>(ImmutableMap.copyOf(builder), oldestEventBirthday, CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])));
    }

    public boolean isEmpty() {
        return updates.isEmpty();
    }


    public synchronized boolean isFull() {
        // the first one is oldest.
        return getPending() > maxSize || (updates.peek() != null && System.currentTimeMillis() - updates.peek().birthday > maxAge);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("updates", updates)
                .add("maxAge", maxAge)
                .add("maxSize", maxSize)
                .toString();
    }

    public static class Update<K, T> implements Comparable<Update<?, ?>> {
        public final CompletableFuture<Void> future;
        public Map<K, T> map;
        public long birthday;

        public Update(Map<K, T> map, long birthday, CompletableFuture<Void> future) {
            this.map = map;
            this.birthday = birthday;
            this.future = future;
        }

        public CompletableFuture<Void> getFuture() {
            return future;
        }

        public Map<K, T> getMap() {
            return map;
        }

        public long getBirthday() {
            return birthday;
        }

        @Override
        public int compareTo(Update o) {
            return Long.compare(this.birthday, o.birthday);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Update)) return false;
            Update<?, ?> update = (Update<?, ?>) o;
            return birthday == update.birthday &&
                    Objects.equal(future, update.future) &&
                    Objects.equal(map, update.map);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(future, map, birthday);
        }
    }
}
