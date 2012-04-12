package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;

/**
 * Interface for drivers connecting a cube to a backing database.
 * 
 * Implementations are not expected to be thread-safe.
 */
public interface DbHarness<T extends Op> {
    /**
     * Apply some ops to the database, combining them with ops that might already be stored there.
     * The passed batch will be modified by runBatch() to remove all ops that were successfully
     * stored, so the batch will be empty when this function returns if there was no IOException.
     */
    public void runBatch(Batch<T> batch) throws IOException;
    
    /**
     * @return absent if the bucket doesn't exist, or the bucket if it does.
     */
    public Optional<T> get(Address c) throws IOException;
    
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
}
