package com.urbanairship.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.serializables.BytesSerializable;

public class EnumToOrdinalBucketer<T extends Enum<?>>  implements Bucketer<T> {
    private final int numBytes;
    
    public EnumToOrdinalBucketer(int numBytes) {
        this.numBytes = numBytes;
    }
    
    private CSerializable bucketInternal(T coordinate, BucketType bucketType) {
        if(bucketType != BucketType.IDENTITY) {
            throw new IllegalArgumentException("You can only use " + 
                    EnumToOrdinalBucketer.class.getSimpleName() + 
                    " with the default identity bucketer");
        }
        int ordinal = coordinate.ordinal();
        byte[] bytes = Util.trailingBytes(Util.intToBytes(ordinal), numBytes);
        
        return new BytesSerializable(bytes);
    }

    @Override
    public CSerializable bucketForWrite(T coordinate, BucketType bucketType) {
        return bucketInternal(coordinate, bucketType);
    }

    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        return bucketInternal((T)coordinate, bucketType);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }
}
