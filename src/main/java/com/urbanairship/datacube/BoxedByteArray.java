package com.urbanairship.datacube;

import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class BoxedByteArray {
    private static final Logger log = LogManager.getLogger(BoxedByteArray.class);
    
    public final byte[] bytes;
    
    public BoxedByteArray(byte[] bytes) {
        this.bytes = bytes;
    }
    
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
    
    @Override
    public boolean equals(Object o) {
        return Arrays.equals(bytes, ((BoxedByteArray)o).bytes);
    }
    
    public String toString() {
        return Hex.encodeHexString(bytes);
    }
}