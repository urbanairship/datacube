package com.urbanairship.datacube;

import java.io.IOException;

import com.google.common.base.Optional;

/**
 * A DataCube does no IO, it merely returns batches that can be executed. This class wraps
 * around a DataCube and does IO against a storage engine.
 */
public class DataCubeIo<T extends Op> {
    private final DbHarness<T> db;
    private final DataCube<T> cube;
    private final int batchSize;

    private Batch<T> batchInProgress = new Batch<T>();
    private int numUpdatesSinceFlush = 0;

    public DataCubeIo(DataCube<T> cube, DbHarness<T> db, int batchSize) {
        this.cube = cube;
        this.db = db;
        this.batchSize = batchSize;
    }
    
    private void updateBatchInMemory(WriteBuilder writeBuilder, T op) {
        Batch<T> newBatch = cube.getWrites(writeBuilder, op);
        batchInProgress.putAll(newBatch);
        numUpdatesSinceFlush++;
    }
    
    synchronized public void writeNoFlush(WriteBuilder c, T op) {
        updateBatchInMemory(c, op);
    }
    
    /**
     * Do some writes into the in-memory batch, possibly flushing to the backing database.
     */
    synchronized public void write(T op, WriteBuilder c) throws IOException {
        updateBatchInMemory(c, op);
        
        if(numUpdatesSinceFlush >= batchSize) {
            flush();
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
    
    synchronized public void flush() throws IOException {
        db.runBatch(batchInProgress);
        numUpdatesSinceFlush = 0;
        batchInProgress = new Batch<T>();
    }
    
    synchronized void clearBatch() {
        batchInProgress = new Batch<T>();
    }
}
