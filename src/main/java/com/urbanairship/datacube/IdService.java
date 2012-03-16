package com.urbanairship.datacube;

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
 * Implementations are not expected to be thread safe.
 */

public interface IdService {
    public byte[] getId(Dimension<?> dimension, byte[] bytes, int len);
}
