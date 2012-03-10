package com.urbanairship.datacube;

/**
 * A service for translating Strings consistently into unqiue IDs. If the String has been
 * seen before, the same identifier will be returned as the previous call. If this is the first time
 * the String has been seen, a new identifier will be assigned.
 * 
 * Different identifier lengths can be used to save space. For example, if you have a dimension
 * that stores a boolean, it can only have two distinct values, so you only need one byte to encode
 * datapoints in that dimension. On the other hand, for a timestamp you may need a large number of
 * bytes to encode every unique coord.
 * 
 * Each identifier length has its own space of identifiers. For example, the result of getId(purple,8)
 * is totally unrelated to the result of getId(purple,4).
 * 
 * To save space, we don't store the actual cube coordinates inside row keys. If the coordinates are
 * (purple, tuesday, 1000), 
 */

public interface IdService {
    public byte[] getId(byte[] bytes, int len);
}
