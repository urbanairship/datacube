package com.urbanairship.datacube;

public class StringSerializable implements CSerializable {
    private final String s;
    
    public StringSerializable(String s) {
        this.s = s;
    }

    @Override
    public byte[] serialize() {
        return s.getBytes();
    }
}
