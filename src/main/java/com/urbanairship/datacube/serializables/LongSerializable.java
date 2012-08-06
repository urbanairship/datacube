/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

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
        return staticSerialize(l);
    }

    public static byte[] staticSerialize(long l) {
        return Util.longToBytes(l);
    }
}
