package com.urbanairship.datacube;

import java.io.IOException;

public class CubeWriter<T extends Op> {
    public static final int FLUSH_THRESHOLD = 1000;
    
    private final DbHarness<T> batchRunner;
    private final DataCube<T> cube;

    private Batch<T> batchInProgress;
    private int numUpdatesSinceFlush = 0;

    public CubeWriter(DataCube<T> cube, DbHarness<T> batchRunner) {
        this.cube = cube;
        this.batchRunner = batchRunner;
    }
    
    private void updateBatchInMemory(Coords c, T op) {
        Batch<T> newBatch = cube.getBatch(c, op);
        batchInProgress.putAll(newBatch);
        numUpdatesSinceFlush++;
    }
    
    synchronized public void updateNoFlush(Coords c, T op) {
        updateBatchInMemory(c, op);
    }
    
    synchronized public void updateMaybeFlush(Coords c, T op) throws IOException {
        updateBatchInMemory(c, op);
        
        if(numUpdatesSinceFlush > FLUSH_THRESHOLD) {
            flush();
        }
    }
    
    synchronized public void flush() throws IOException {
        batchRunner.runBatch(batchInProgress);
        numUpdatesSinceFlush = 0;
        batchInProgress = new Batch<T>();
    }
}
