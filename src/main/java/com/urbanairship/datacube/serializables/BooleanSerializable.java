/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.google.common.io.BaseEncoding;
import com.urbanairship.datacube.CSerializable;

import java.util.Arrays;

/**
 * Use this in your bucketer if you're using booleans as dimension coordinates.
 */
public class BooleanSerializable implements CSerializable {
    private final boolean bool;
    private static final byte[] FALSE_SERIAL = new byte[]{0};
    private static final byte[] TRUE_SERIAL = new byte[]{1};
    private static final BaseEncoding BASE_ENCODING = BaseEncoding.base64();

    public BooleanSerializable(boolean bool) {
        this.bool = bool;
    }

    @Override
    public byte[] serialize() {
        return staticSerialize(bool);
    }

    public static byte[] staticSerialize(boolean b) {
        if (b) {
            return TRUE_SERIAL;
        } else {
            return FALSE_SERIAL;
        }
    }

    public static boolean deserializ(byte[] bytes) {
        if (Arrays.equals(bytes, FALSE_SERIAL)) return false;
        if (Arrays.equals(bytes, TRUE_SERIAL)) return true;
        throw new RuntimeException("cannot deserialize byte array " + BASE_ENCODING.encode(bytes) + " as boolean");
    }
}
