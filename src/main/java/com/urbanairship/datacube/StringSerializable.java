package com.urbanairship.datacube;

import java.io.UnsupportedEncodingException;

/**
 * Use this in your bucketer if you want to use Strings as dimension coordinates.
 */
public class StringSerializable implements CSerializable {
    private final String s;
    
    public StringSerializable(String s) {
        this.s = s;
    }

    @Override
    public byte[] serialize() {
        try {
            return s.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
