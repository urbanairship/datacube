package com.urbanairship.datacube;

import java.nio.ByteBuffer;

public class Util {
    public static byte[] longToBytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }
    
    public static byte[] longToBytesWithPrefix(byte prefix, long l) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 8);
        bb.put(prefix);
        bb.putLong(l);
        return bb.array();
    }

    public static long bytesToLong(byte[] bytes) {
        if(bytes.length < 8) {
            throw new IllegalArgumentException("Input array was too small");
        }
        
        return ByteBuffer.wrap(bytes).getLong();
    }
}
