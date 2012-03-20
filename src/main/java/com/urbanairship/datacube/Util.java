package com.urbanairship.datacube;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Util {
    public static byte[] longToBytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }
    
    public static long bytesToLong(byte[] bytes) {
        if(bytes.length < 8) {
            throw new IllegalArgumentException("Input array was too small: " +
                    Arrays.toString(bytes));
        }
        
        return ByteBuffer.wrap(bytes).getLong();
    }
    
    /**
     * Call this if you have a byte array to convert to long, but your array might need
     * to be left-padded with zeros (if it is less than 8 bytes long). 
     */
    public static long bytesToLongPad(byte[] bytes) {
        final int padZeros = Math.max(8-bytes.length, 0);
        ByteBuffer bb = ByteBuffer.allocate(8);
        for(int i=0; i<padZeros; i++) {
            bb.put((byte)0);
        }
        bb.put(bytes, 0, 8-padZeros);
        bb.flip();
        return bb.getLong();
    }
    
    public static byte[] intToBytes(int x) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(x);
        return bb.array();
    }

    public static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static byte[] leastSignificantBytes(long l, int numBytes) {
        byte[] all8Bytes = Util.longToBytes(l);
        return trailingBytes(all8Bytes, numBytes);
    }

    public static byte[] trailingBytes(byte[] bs, int numBytes) {
        return Arrays.copyOfRange(bs, bs.length-numBytes, bs.length);
    }
}
