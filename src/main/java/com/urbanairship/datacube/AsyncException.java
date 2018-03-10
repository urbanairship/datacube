/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

/**
 * This exception is thrown when a batch is submitted to a DbHarness for execution, but the DbHarness
 * is in an error state as a result of a previous batch failing.
 *
 * This is complicated but necessary due to the way that batches are flushed asynchronously. When a
 * batch is submitted, there may be a runtime exception later when saving it to the database. In this
 * case, the datacube must inform the caller somehow so the caller can stop trying to save batches,
 * which will result in even more data loss. This exception class is our way of informing the caller
 * about the fact that the DbHarness is in an error state and will not accept any more batches.
 */
public class AsyncException extends Exception {
    private static final long serialVersionUID = 1L;

    public AsyncException(Throwable t) {
        super(t);
    }

    public AsyncException(String msg) {
        super(msg);
    }
}
