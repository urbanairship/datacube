package com.urbanairship.datacube;

import java.io.IOException;

import com.google.common.base.Optional;

/**
 * A DataCube does no IO, it merely returns batches that can be executed. This class wraps
 * around a DataCube and does IO against a storage backend.
 * 
 * Thread safe. Writes can block for a long time if another thread is flushing to the database.
 */
public class DataCubeIo<T extends Op> {
    private final DbHarness<T> db;
    private final DataCube<T> cube;
    private final int batchSize;
    private final long maxBatchAgeMs;
    
    private Batch<T> batchInProgress = new Batch<T>();
    private long batchStartTimeMs;
    
    private final Object lock = new Object();
    
    public DataCubeIo(DataCube<T> cube, DbHarness<T> db, int batchSize) {
        this(cube, db, batchSize, Long.MAX_VALUE);
    }
    
    /**
     * @param batchSize if after doing a write the number of rows to be written to the database
     * exceeds this number, a flush will be done.
     * @param maxBatchAgeMs if after doing a write the batch's oldest write was more than this long ago,
     * a flush will be done. This is not a hard ceiling on the age of writes in the batch, because the
     * batch will not be flushed until the *next* write arrives after the timeout.
     */
    public DataCubeIo(DataCube<T> cube, DbHarness<T> db, int batchSize, long maxBatchAgeMs) {
        this.cube = cube;
        this.db = db;
        this.batchSize = batchSize;
        this.maxBatchAgeMs = maxBatchAgeMs;
    }
    
    private void updateBatchInMemory(WriteBuilder writeBuilder, T op) {
        Batch<T> newBatch = cube.getWrites(writeBuilder, op);
        synchronized (lock) {
            if(batchInProgress.getMap().isEmpty()) {
                // Start the timer for this batch, it should be flushed when it becomes old
                batchStartTimeMs = System.currentTimeMillis();
            }
            batchInProgress.putAll(newBatch);
        }
    }
    
    public void writeNoFlush(T op, WriteBuilder c) {
        updateBatchInMemory(c, op);
    }
    
    /**
     * Do some writes into the in-memory batch, possibly flushing to the backing database.
     */
    public void write(T op, WriteBuilder c) throws IOException {
        updateBatchInMemory(c, op);
        
        synchronized (lock) {
            boolean shouldFlush = false;
            
            if(batchInProgress.getMap().size() >= batchSize) {
                shouldFlush = true;
            } else if(System.currentTimeMillis() > batchStartTimeMs + maxBatchAgeMs) {
                shouldFlush = true;
            }
            
            if(shouldFlush) {
                flush();
            }
        }
    }
    
    /**
     * @return absent if the bucket doesn't exist, or the bucket if it does.
     */
    public Optional<T> get(Address addr) throws IOException {
        cube.checkValidReadOrThrow(addr);
        return db.get(addr);
    }
    
    public Optional<T> get(ReadAddressBuilder readBuilder) throws IOException {
        return this.get(readBuilder.build());
    }
    
    public void flush() throws IOException {
        synchronized (lock) {
            DebugHack.log("Flushing cube with " + batchInProgress.getMap().size() + " writes");

            // We hold the lock while doing database IO. If the DB takes a long time,
            // then other threads may block for a long time.
            db.runBatch(batchInProgress);
            batchInProgress = new Batch<T>();
        }
    }
}
