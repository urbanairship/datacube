/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

public class BytesSerializable implements CSerializable {
    private final byte[] bytes;

    public BytesSerializable(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] serialize() {
        return bytes;
    }

    public static byte[] deserialize(byte[] bytes) {
        return bytes;
    }
}
