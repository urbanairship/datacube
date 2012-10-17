/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import java.util.Arrays;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

/**
 * When using an enum as a dimension bucket type, this wrapper eliminates the boilerplate
 * of serializing it to an ordinal. Use this in your bucketer.
 */
public class EnumSerializable implements CSerializable {
    private final int ordinal;
    private final int numFieldBytes;
    
    /**
     * @param numFieldBytes the number of bytes to produce for serialized version of this 
     * enum
     */
    public EnumSerializable(Enum<?> enumInstance, int numFieldBytes) {
        this.ordinal = enumInstance.ordinal();
        this.numFieldBytes = numFieldBytes;
        
        if(numFieldBytes < 1 || numFieldBytes > 4) {
            throw new IllegalArgumentException("numFieldBytes must be in [1..4]");
        }
    }
    
    @Override
    public byte[] serialize() {
        return staticSerialize(ordinal, numFieldBytes);
    }

    public static byte[] staticSerialize(Enum<?> enumInstance, int numFieldBytes) {
        return staticSerialize(enumInstance.ordinal(), numFieldBytes);
    }

    public static byte[] staticSerialize(int ordinal, int numFieldBytes) {
        return Util.intToBytesWithLen(ordinal, numFieldBytes);
    }
}
