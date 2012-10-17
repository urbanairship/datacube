/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.ArrayList;
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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.NamedThreadFactory;
import com.urbanairship.datacube.Op;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

public class HBaseDbHarness<T extends Op> implements DbHarness<T> {
    
    private static final Logger log = LoggerFactory.getLogger(HBaseDbHarness.class);

    public final static byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;
        
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
    
    private final Set<Batch<T>> batchesInFlight = Sets.newHashSet();
    
    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName, 
            byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType) 
                    throws IOException {
        this(pool, uniqueCubeName, tableName, cf, deserializer, idService, commitType,
                5, 5, 10, null);
    }
    
    public HBaseDbHarness(HTablePool pool, byte[] uniqueCubeName, byte[] tableName, 
            byte[] cf, Deserializer<T> deserializer, IdService idService, CommitType commitType, 
            int numFlushThreads, int numIoeTries, int numCasTries, String metricsScope)
                    throws IOException {
        this.pool = pool;
        this.deserializer = deserializer;
        this.uniqueCubeName = uniqueCubeName;
        this.tableName = tableName;
        this.cf = cf;
        this.idService = idService;
        this.commitType = commitType;
        this.numIoeTries = numIoeTries;
        this.numCasTries = numCasTries;
        
        flushSuccessTimer = Metrics.newTimer(HBaseDbHarness.class, "successfulBatchFlush", 
                metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        flushFailTimer = Metrics.newTimer(HBaseDbHarness.class, "failedBatchFlush", 
                metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        singleWriteTimer = Metrics.newTimer(HBaseDbHarness.class, "singleWrites", 
                metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        incrementSize = Metrics.newHistogram(HBaseDbHarness.class, "incrementSize", 
                metricsScope, true);
        
        
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(numFlushThreads);
        this.flushExecutor = new ThreadPoolExecutor(numFlushThreads, numFlushThreads, 1,
                TimeUnit.MINUTES, workQueue, new NamedThreadFactory("HBase DB flusher"));

        Metrics.newGauge(HBaseDbHarness.class, "asyncFlushQueueDepth", metricsScope, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return flushExecutor.getQueue().size();
            }
        });
        
        Metrics.newGauge(HBaseDbHarness.class, "asyncFlushersActive", metricsScope, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return flushExecutor.getActiveCount();
            }
        });
    }

    @Override
    public Optional<T> get(Address c) throws IOException, InterruptedException  {
        final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, c.toKey(idService));
        
        Get get = new Get(rowKey);
        get.addFamily(cf);
        Result result = WithHTable.get(pool, tableName, get);
        if(result == null || result.isEmpty()) {
            if(log.isDebugEnabled()) {
                log.debug("Returning absent for cube:" + Arrays.toString(uniqueCubeName) + 
                        " for address:" + c + " key " + Base64.encodeBase64String(rowKey));
            }
            return Optional.absent();
        } else {
            T deserialized = deserializer.fromBytes(result.value());
            if(log.isDebugEnabled()) {
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
            synchronized(batchesInFlight) {
                batchesInFlight.add(batch);
            }
            return flushExecutor.submit(new FlushWorkerRunnable(batch, afterExecute));
        } catch (RejectedExecutionException ree) {
            throw new FullQueueException();
        }
    }
    
    private class FlushWorkerRunnable implements Callable<Object> {
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
                for(int attempt=0; attempt<numIoeTries; attempt++) {
                    try {
                        flushBatch(batch);
                        afterExecute.afterExecute(null); // null => no exception
                        return null; // The return value of this callable is ignored
                    } catch (IOException e) {
                        lastIOException = e;
                        log.error("IOException in worker thread flushing to HBase on attempt " + 
                                attempt + "/" + numIoeTries + ", will retry", e);
                        Thread.sleep(500);
                    }
                }
            } catch (Exception e) {
                afterExecute.afterExecute(e);
                throw e;
            } finally {
                synchronized(batchesInFlight) {
                    batchesInFlight.remove(batch);
                }
            }
            
            afterExecute.afterExecute(lastIOException);
            throw lastIOException;
        }
    }

    private void increment(byte[] rowKey, T op) throws IOException {
        long amount = Bytes.toLong(op.serialize());
        incrementSize.update(amount);
        WithHTable.increment(pool, tableName, rowKey, cf, QUALIFIER, amount);
    }
    
    @SuppressWarnings("unchecked")
    private void readCombineCas(byte[] rowKey, T newOp) throws IOException {
        Get get = new Get(rowKey);
        get.addColumn(cf, QUALIFIER);
        Result result = WithHTable.get(pool, tableName, get);
        
        byte[] prevSerializedOp = result.getValue(cf, QUALIFIER);
        T combinedOp;
        if(prevSerializedOp == null) {
            combinedOp = newOp;
        } else {
            T previousOp = (T)deserializer.fromBytes(prevSerializedOp);
            combinedOp = (T)previousOp.add(newOp);
        }
        
        
        Put put = new Put(rowKey);
        put.add(cf, QUALIFIER, combinedOp.serialize());
        
        for(int i=0; i<numCasTries; i++) {
            if(WithHTable.checkAndPut(pool, tableName, rowKey, cf, QUALIFIER, prevSerializedOp, put)) {
                return; // successful write
            } else {
                log.warn("checkAndPut failed on try " + (i+1) + " out of " + numCasTries);
            }
        }
        
        throw new IOException("Exhausted retries doing checkAndPut after " + numCasTries + 
                " tries");
    }
    
    private void overwrite(byte[] rowKey, T op) throws IOException {
        Put put = new Put(rowKey);
        put.add(cf, QUALIFIER, op.serialize());
        WithHTable.put(pool, tableName, put);
    }
    
    private void flushBatch(Batch<T> batch) throws IOException, InterruptedException  {
        Map<Address,T> batchMap = batch.getMap();
        
        List<Address> succesfullyWritten = new ArrayList<Address>(batch.getMap().size());
        
        long nanoTimeBeforeBatch = System.nanoTime();
        
        try {
            for(Map.Entry<Address,T> entry: batchMap.entrySet()) {
                Address address = entry.getKey();
                T op = entry.getValue();

                byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, address.toKey(idService));
                
                long nanoTimeBeforeWrite = System.nanoTime();
                
                switch(commitType) {
                case INCREMENT:
                    increment(rowKey, op);
                    break;
                case READ_COMBINE_CAS:
                    readCombineCas(rowKey, op);
                    break;
                case OVERWRITE:
                    overwrite(rowKey, op);
                    break;
                default:
                    throw new RuntimeException("Unsupported commit type " + commitType);
                }
                long writeDurationNanos = System.nanoTime() - nanoTimeBeforeWrite;
                singleWriteTimer.update(writeDurationNanos, TimeUnit.NANOSECONDS);
                
                if(log.isDebugEnabled()) {
                    log.debug("Succesfully wrote cube:" + Arrays.toString(uniqueCubeName) + 
                            " address:" + address);
                }
                succesfullyWritten.add(address);
            }
        } catch (IOException e) {
            // There was an IOException while attempting to use the DB. If we were successful with some
            // of the pending writes, they should be removed from the batch so they are not retried later.
            // The operations that didn't get into the DB should be left in the batch to be retried later.
            log.warn("IOException when flushing batch to HBase", e);
            for(Address address: succesfullyWritten) {
                batch.getMap().remove(address);
            }
            
            long batchDurationNanos = System.nanoTime() - nanoTimeBeforeBatch;
            flushFailTimer.update(batchDurationNanos, TimeUnit.NANOSECONDS);

            throw e;
        } 
        
        long batchDurationNanos = System.nanoTime() - nanoTimeBeforeBatch;
        flushSuccessTimer.update(batchDurationNanos, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void flush() throws InterruptedException {
        // Algorithm: get the set of batches that are in flight at the moment we start flushing.
        // As soon as all those batches are done, the "flush" is complete. Disregard all batches
        // that arrive after the flush starts.
        
        Set<Batch<T>> batchesToWaitFor;
        synchronized(batchesInFlight) {
            batchesToWaitFor = Sets.newHashSet(batchesInFlight);
        }
        while(true) {
            Set<Batch<T>> justFinished = Sets.newHashSet();
            synchronized(batchesInFlight) {
                for(Batch<T> batch: batchesToWaitFor) {
                    if(!batchesInFlight.contains(batch)) {
                        justFinished.add(batch);
                    }
                }
            }
            
            for(Batch<T> finishedBatch: justFinished) {
                batchesToWaitFor.remove(finishedBatch);
            }
            
            if(batchesToWaitFor.isEmpty()) {
                return;
            }
            
            Thread.sleep(100);
        }
    }
}
