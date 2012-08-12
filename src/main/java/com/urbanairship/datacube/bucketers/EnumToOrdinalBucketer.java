/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.EnumSerializable;

import org.apache.commons.lang.NotImplementedException;

import java.util.List;

public class EnumToOrdinalBucketer<T extends Enum<?>>  implements Bucketer<T> {
    private final int numBytes;

    public EnumToOrdinalBucketer(int numBytes) {
        this.numBytes = numBytes;
    }

    private CSerializable<T> bucketInternal(T coordinate, BucketType bucketType) {
        if(bucketType != BucketType.IDENTITY) {
            throw new IllegalArgumentException("You can only use " +
                    EnumToOrdinalBucketer.class.getSimpleName() +
                    " with the default identity bucketer");
        }
        return new EnumSerializable<T>(coordinate, numBytes);
    }

    @Override
    public CSerializable<T> bucketForWrite(T coordinate, BucketType bucketType) {
        return bucketInternal(coordinate, bucketType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CSerializable<T> bucketForRead(Object coordinate, BucketType bucketType) {
        return bucketInternal((T)coordinate, bucketType);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    @Override
    public T readBucket(BoxedByteArray key, BucketType btype) {
        throw new NotImplementedException("EnumSerializable does not support deserialization right now");
    }
}
