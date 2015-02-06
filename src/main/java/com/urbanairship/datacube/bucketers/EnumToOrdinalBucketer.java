/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.serializables.BytesSerializable;
import com.urbanairship.datacube.serializables.EnumSerializable;

public class EnumToOrdinalBucketer<T extends Enum<?>> extends AbstractIdentityBucketer<T> {
    private final int numBytes;
    private final Class<T> enumClass;

    public EnumToOrdinalBucketer(int numBytes, Class<T> enumClass) {
        this.numBytes = numBytes;
        this.enumClass = enumClass;
    }
    
    @Override
    public CSerializable makeSerializable(T coordinate) {
        int ordinal = coordinate.ordinal();
        byte[] bytes = Util.trailingBytes(Util.intToBytes(ordinal), numBytes);
        
        return new BytesSerializable(bytes);
    }

    @Override
    public T deserialize(byte[] coord, BucketType bucketType) {
        if (coord == null || coord.length == 0) {
            throw new IllegalArgumentException("Null or Zero length byte array can not be " +
                    "deserialized");
        } else if (coord.length > 4) {
            throw new IllegalArgumentException("EnumToOrdinalBucketer can not have coordinate " +
                    "byte array size more than number of bytes require by Integer");
        }
        int ordinal = EnumSerializable.deserialize(coord);
        return enumClass.getEnumConstants()[ordinal];
    }
}
