package com.urbanairship.datacube;

public class BytesSerializable implements CSerializable {
    private final byte[] bytes;
    
    public BytesSerializable(byte[] bytes) {
        this.bytes = bytes;
    }
    
    @Override
    public byte[] serialize() {
        return bytes;
    }
}
