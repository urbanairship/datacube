/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

public class BytesSerializable implements CSerializable<byte[]> {
    private final byte[] bytes;

    public BytesSerializable(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] serialize() {
        return bytes;
    }

    @Override
    public byte[] deserialize(byte[] serObj) {
        return serObj;
    }
}
