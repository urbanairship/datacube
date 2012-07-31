/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

/**
 * Use this in your bucketer if you're using longs as dimension coordinates.
 */
public class LongSerializable implements CSerializable<Long> {
    private final long l;

    public LongSerializable(long l) {
        this.l = l;
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(l);
    }

    @Override
    public Long deserialize(byte[] serObj) {
        return Util.bytesToLong(serObj);
    }
}
