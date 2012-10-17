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

public class EnumToOrdinalBucketer<T extends Enum<?>> extends AbstractIdentityBucketer<T> {
    private final int numBytes;
    
    public EnumToOrdinalBucketer(int numBytes) {
        this.numBytes = numBytes;
    }
    
    @Override
    public CSerializable makeSerializable(T coordinate) {
        int ordinal = coordinate.ordinal();
        byte[] bytes = Util.trailingBytes(Util.intToBytes(ordinal), numBytes);
        
        return new BytesSerializable(bytes);
    }
}
