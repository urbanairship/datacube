package com.urbanairship.datacube;

/**
 * A deserializer is responsible from converting a serialized Op from a byte array back
 * into an Op object.
 * 
 * Deserializers must also have a no-argument constructor for backfilling to work.
 */
public interface Deserializer<O extends Op> {
    public O fromBytes(byte[] bytes);
}
