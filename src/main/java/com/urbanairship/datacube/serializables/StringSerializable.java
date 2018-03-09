/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

import java.io.UnsupportedEncodingException;

/**
 * Use this in your bucketer if you want to use Strings as buckets.
 */
public class StringSerializable implements CSerializable {
    public static final String UTF_8 = "UTF8";
    private final String s;

    public StringSerializable(String s) {
        this.s = s;
    }

    @Override
    public byte[] serialize() {
        return staticSerialize(s);
    }

    public static byte[] staticSerialize(String s) {
        try {
            return s.getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            // No reasonable JVM will lack UTF8
            throw new RuntimeException(e);
        }
    }
    public static String deserialize(byte[] bytes) {
        try {
            return new String(bytes, UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
