/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import java.io.UnsupportedEncodingException;

import com.urbanairship.datacube.CSerializable;

/**
 * Use this in your bucketer if you want to use Strings as buckets.
 */
public class StringSerializable implements CSerializable {
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
            return s.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            // No reasonable JVM will lack UTF8
            throw new RuntimeException(e);
        }
    }
}
