package com.urbanairship.datacube;

public interface Deserializer<O extends Op> {
    public O fromBytes(byte[] bytes);
}
