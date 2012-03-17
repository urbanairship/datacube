package com.urbanairship.datacube;

import java.io.IOException;

/**
 * A service for translating byte[]s consistently into unique IDs. If the given byte[] has been 
 * seen before, the same identifier will be returned as the previous call. If this is the first 
 * time byte[] has been seen, a new identifier will be assigned.
 * 
 * Different identifier lengths can be used to save space. For example, if you have a dimension
 * that stores a boolean, it can only have two distinct values, so you only need one byte to encode
 * datapoints in that dimension. On the other hand, to store the name of a city, you would need
 * more bytes to encode it since there are many cities.
 * 
 * Implementations are not expected to be thread safe. However, multiple ID services may access
 * the same backing storage simultaneously, so some kind of concurrency control is required when
 * assigning IDs. It's not OK for the same input to be translated differently; if a
 * particular input translates into a particular unique ID once, it must translate in the same
 * way everywhere else forever. Also, a unqiue ID must never be reused for different inputs. This 
 * means you must use locks, transactions, or compare-and-swap on your database when assigning 
 * IDs.
 */

public interface IdService {
    public byte[] getId(Dimension<?> dimension, byte[] bytes) throws IOException;
}
