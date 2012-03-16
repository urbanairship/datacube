package com.urbanairship.datacube;

import java.util.Arrays;

public class EnumSerializable implements CSerializable {
    private final int ordinal;
    private final int numFieldBytes;
    
    public EnumSerializable(Enum<?> enumInstance, int numFieldBytes) {
        this.ordinal = enumInstance.ordinal();
        this.numFieldBytes = numFieldBytes;
        
        if(numFieldBytes < 1 || numFieldBytes > 4) {
            throw new IllegalArgumentException("numFieldBytes must be in [1..4]");
        }
    }
    
    @Override
    public byte[] serialize() {
        byte[] ordinalBytes = Util.intToBytes(ordinal);
        
        int startAtIndex = 4-numFieldBytes;
        
        return Arrays.copyOfRange(ordinalBytes, startAtIndex, 4);
    }
    
}
