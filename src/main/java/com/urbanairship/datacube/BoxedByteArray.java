/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;

public class BoxedByteArray implements Serializable {
    
    private static final long serialVersionUID = 1L;

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