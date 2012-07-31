/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

import java.util.Arrays;

/**
*   Use this in your bucketer if you're using booleans as dimension coordinates.
 */
public class BooleanSerializable implements CSerializable<Boolean> {
    private final boolean bool;
    private static final byte[] FALSE_SERIAL = new byte[]{0};
    private static final byte[] TRUE_SERIAL = new byte[]{1};

    public BooleanSerializable(boolean bool){
        this.bool = bool;
    }

    @Override
    public byte[] serialize() {
        if (this.bool){
            return TRUE_SERIAL;
        } else {
            return FALSE_SERIAL;
        }
    }

    @Override
    public Boolean deserialize(byte[] serObj) {
        if(Arrays.equals(serObj, TRUE_SERIAL)) {
            return true;
        } else {
            return false;
        }
    }

}
