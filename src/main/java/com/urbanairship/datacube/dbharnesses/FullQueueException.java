package com.urbanairship.datacube.dbharnesses;

/**
 * Thrown when a DbHarness cannot accept a batch for writing because it has reached the limit on
 * outstanding batches. The caller may submit the batch again soon.
 */
public class FullQueueException extends Exception {
    private static final long serialVersionUID = 1L;
}
