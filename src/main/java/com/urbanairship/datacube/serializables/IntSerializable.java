/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

/**
 * Use this in your bucketer if you're using ints as dimension coordinates.
 */
public class IntSerializable implements CSerializable {
    private final int i;
    
    public IntSerializable(int l) {
        this.i = l;
    }

    @Override
    public byte[] serialize() {
        return Util.intToBytes(i);
    }
}
