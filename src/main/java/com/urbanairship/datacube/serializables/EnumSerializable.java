/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.google.common.base.Preconditions;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

/**
 * When using an enum as a dimension bucket type, this wrapper eliminates the boilerplate
 * of serializing it to an ordinal. Use this in your bucketer.
 */
public class EnumSerializable<T extends Enum> implements CSerializable {
    private final int ordinal;
    private final int numFieldBytes;

    /**
     * @param numFieldBytes the number of bytes to produce for serialized version of this
     *                      enum
     */
    public EnumSerializable(T enumInstance, int numFieldBytes) {
        this.ordinal = enumInstance.ordinal();
        this.numFieldBytes = numFieldBytes;

        if (numFieldBytes < 1 || numFieldBytes > 4) {
            throw new IllegalArgumentException("numFieldBytes must be in [1..4]");
        }
    }

    @Override
    public byte[] serialize() {
        return staticSerialize(ordinal, numFieldBytes);
    }

    public static <T extends Enum> byte[] staticSerialize(T enumInstance, int numFieldBytes) {
        return staticSerialize(enumInstance.ordinal(), numFieldBytes);
    }

    public static byte[] staticSerialize(int ordinal, int numFieldBytes) {
        return Util.intToBytesWithLen(ordinal, numFieldBytes);
    }

    public static <T> T deserialize(Class<T> clazz, byte[] bytes) {
        T[] enumConstants = clazz.getEnumConstants();
        int i = Util.bytesToInt(bytes);
        if(i > enumConstants.length) {
            throw new EnumDeserializationException("deserialized unknown enum ordinal, " + i + " for enum " + clazz.getSimpleName());
        }
        return enumConstants[i];
    }

    public static class EnumDeserializationException extends RuntimeException {
        public EnumDeserializationException(String msg) {
            super(msg);
        }

    }
}
