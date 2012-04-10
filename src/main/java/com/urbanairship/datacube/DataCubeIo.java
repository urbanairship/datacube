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

    private Batch<T> batchInProgress = new Batch<T>();
    private int numUpdatesSinceFlush = 0;

    private final Object lock = new Object();
    
    public DataCubeIo(DataCube<T> cube, DbHarness<T> db, int batchSize) {
        this.cube = cube;
        this.db = db;
        this.batchSize = batchSize;
    }
    
    private void updateBatchInMemory(WriteBuilder writeBuilder, T op) {
        Batch<T> newBatch = cube.getWrites(writeBuilder, op);
        synchronized (lock) {
            batchInProgress.putAll(newBatch);
            numUpdatesSinceFlush++;
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
            if(numUpdatesSinceFlush >= batchSize) {
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
            numUpdatesSinceFlush = 0;
            batchInProgress = new Batch<T>();
        }
    }
}
