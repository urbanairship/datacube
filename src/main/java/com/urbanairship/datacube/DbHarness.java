/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Optional;
import com.urbanairship.datacube.dbharnesses.AfterExecute;
import com.urbanairship.datacube.dbharnesses.FullQueueException;
import com.urbanairship.datacube.ops.SerializableOp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Interface for drivers connecting a cube to a backing database.
 *
 * Implementations are expected to be thread-safe.
 */
public interface DbHarness<T extends SerializableOp> {
    /**
     * Apply some ops to the database, combining them with ops that might already be stored there.
     * The passed batch will be modified by runBatch() to remove all ops that were successfully
     * stored, so the batch will be empty when this function returns if there was no IOException.
     * If this function throws IOException, the batch will contain one or more ops that were not
     * successfully applied to the DB.
     *
     * This is asynchronous, meaning that the given batch will probably be executed soon, but not
     * necessarily by the time this function returns.
     *
     * @throws AsyncException if the DbHarness is in a bad state as a result of an earlier
     * RuntimeException and will not accept any more batches. See {@link AsyncException} for
     * more info.
     * @throws FullQueueException if the queue of pending batches to be flushed is full, and no more
     * batches can be accepted right now. The caller can retry soon.
     */
    public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute) throws FullQueueException;

    /**
     * @return absent if the bucket doesn't exist, or the bucket if it does.
     * @throws InterruptedException
     */
    public Optional<T> get(Address c) throws IOException, InterruptedException;

    /**
     * Performs a read of a preaggregated slice operation and returns a Map
     * of the results
     * @param sliceAddr the slices' address
     * @return An optional Map with Dimension values as keys and counter as value
     */
    Optional<Map<BoxedByteArray, T>> getSlice(Address sliceAddr) throws IOException, InterruptedException;

    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException;

    /**
     * When it's time to write a batch to the database, there are a couple of ways it can
     * be done.
     *
     *  READ_COMBINE_CAS: read the existing cell value from the database, deserialize
     *  it into an Op, combine it with the Op to be written, and write the combined Op back
     *  to the database using a compare-and-swap operation to make sure it hasn't changed
     *  since we read it. If the CAS fails because the cell value was concurrently modifed by
     *  someone else, start over at the beginning and re-read the value.
     *
     *  INCREMENT: for numeric values like counters, if the database supports server side
     *  incrementing, the serialized Op will be passed as the amount to be incremented. This
     *  means that no reads or combines will be done in the client (the harness code).
     *
     *  OVERWRITE: without reading from the database, overwrite the cell value with the value
     *  from the batch in memory. This will wipe out the existing count, so don't use this
     *  unless you're sure you know what you're doing.
     */
    public enum CommitType {
        READ_COMBINE_CAS,
        INCREMENT,
        OVERWRITE
    }

    /**
     * Wait until all currently outstanding writes have been written to the DB. or failed.
     */
    void flush() throws InterruptedException;
}
