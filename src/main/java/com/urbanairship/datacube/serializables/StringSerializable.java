/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

import java.io.UnsupportedEncodingException;

/**
 * Use this in your bucketer if you want to use Strings as buckets.
 */
public class StringSerializable implements CSerializable<String> {
    private final String s;
    private static final String ENCODING = "UTF8";

    public StringSerializable(String s) {
        this.s = s;
    }

    @Override
    public byte[] serialize() {
        try {
            return s.getBytes(ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String deserialize(byte[] serObj) {
        try {
            return new String(serObj, ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Bytes " + serObj + "cannot be decoded using encoding " + ENCODING, e);
        }
    }
}
