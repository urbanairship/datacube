/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

import java.util.Arrays;

/**
 * When using an enum as a dimension bucket type, this wrapper eliminates the boilerplate
 * of serializing it to an ordinal. Use this in your bucketer.
 */
public class EnumSerializable implements CSerializable<Enum<?>> {
    private final int ordinal;
    private final int numFieldBytes;
    private Enum<?> enumInstance;

    public <E extends Enum> EnumSerializable(E enumInstance, int numFieldBytes) {
        this.ordinal = enumInstance.ordinal();
        this.enumInstance = enumInstance;
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

    @Override
    public Enum<?> deserialize(byte[] serObj) {
        int enumOrdinal = Util.bytesToInt(serObj);
        return enumInstance.getClass().getEnumConstants()[enumOrdinal];
    }

}
