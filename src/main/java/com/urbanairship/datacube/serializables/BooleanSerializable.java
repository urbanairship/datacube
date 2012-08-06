/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;
import org.apache.hadoop.hbase.util.Bytes;

/**
*   Use this in your bucketer if you're using booleans as dimension coordinates.
 */
public class BooleanSerializable implements CSerializable{
    private final boolean bool;
    private static final byte[] FALSE_SERIAL = new byte[]{0};
    private static final byte[] TRUE_SERIAL = new byte[]{1};

    public BooleanSerializable(boolean bool){
        this.bool = bool;
    }

    @Override
    public byte[] serialize() {
        return staticSerialize(bool);
    }

    public static byte[] staticSerialize(boolean b) {
        if (b) {
            return TRUE_SERIAL;
        } else {
            return FALSE_SERIAL;
        }
    }
}
