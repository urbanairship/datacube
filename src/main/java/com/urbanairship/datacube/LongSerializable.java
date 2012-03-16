package com.urbanairship.datacube;

/**
 * Use this in your bucketer if you're using longs as dimension coordinates.
 */
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
