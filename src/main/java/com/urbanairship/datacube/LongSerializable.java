package com.urbanairship.datacube;

public class LongSerializable implements CSerializable {
    private final long l;
    
    public LongSerializable(long l) {
        this.l = l;
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(l);
    }
}
