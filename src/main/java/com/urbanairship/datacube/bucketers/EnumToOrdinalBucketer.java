/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.serializables.BytesSerializable;
import com.urbanairship.datacube.serializables.EnumSerializable;

public class EnumToOrdinalBucketer<T extends Enum<?>> extends AbstractIdentityBucketer<T> {
    private final int numBytes;
    private Class<T> tClass;

    public EnumToOrdinalBucketer(int numBytes, Class<T> tClass) {
        this.numBytes = numBytes;
        this.tClass = tClass;
    }

    @Override
    public CSerializable makeSerializable(T coordinate) {
        int ordinal = coordinate.ordinal();
        byte[] bytes = Util.trailingBytes(Util.intToBytes(ordinal), numBytes);

        return new BytesSerializable(bytes);
    }

    @Override
    public T deserialize(byte[] coord, BucketType bucketType) {
        return EnumSerializable.<T>deserialize(tClass, coord);
    }
}
