package com.urbanairship.datacube.dbharnesses;

import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.ops.LongOp;

import java.io.IOException;
import java.util.Map;


/**
 * Synchronously executes the operations with the minimum number of database requests possible.
 */
public interface BatchDbHarness {
    /**
     * Issue a single request incrementing every provided row in the operation.
     *
     * @param batch   A map from Address to increment operations, defining all the updates you'd like to accomplish
     * @param retryPolicy Defines how you'd like to address write failures, either due to IOExceptions submitting the
     *                whole batch to the underlying data store, or due to failure to increment a given row.
     *
     *                The method can still throw an IOException due to failure to retrieve the mapped id from the
     *                underlying data store
     *
     * @return Assuming normal completion, any entries which were not written to the database even after retry are
     * returned here.
     *
     * @throws IOException          We had an error talking to the underlying database. Some of the batch operations may
     *                              have executed, depending on the guarantees of the underlying database.
     * @throws InterruptedException We had an error talking to the underlying database. Some of the batch operations may
     *                              have executed, depending on the guarantees of the underlying database.
     */
    void increment(Map<Address, LongOp> batch, RetryPolicy retryPolicy) throws InterruptedException, IOException;


    interface RetryPolicy {
        /**
         * blocks until an appropriate duration has elapsed
         *
         * @param attempt the try number for which we should sleep. A constant or random jittered retry policy would
         *                ignore this parameter. An exponential backoff would wait {@code Math.pow(base_period,
         *                attempt)} before the next attempt, &c.
         *
         * @return false if we have exhausted our retries, true otherwise.
         *
         * @throws InterruptedException
         */
        boolean sleep(int attempt) throws InterruptedException;
    }

    interface BlockingIO<V, R> {
        R apply(V v) throws IOException, InterruptedException;
    }
}
