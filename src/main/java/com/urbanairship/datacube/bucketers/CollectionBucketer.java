package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;

import java.util.Collection;

public abstract class CollectionBucketer<T> implements Bucketer<Collection<T>, T> {
    protected abstract CSerializable bucketForOneRead(T coordinate, BucketType bucketType);

    protected abstract T deserializeOne(byte[] coord);

    protected abstract CSerializable bucketForOneWrite(T coord);

    @Override
    public SetMultimap<BucketType, CSerializable> bucketForWrite(Collection<T> coordinates) {
        ImmutableSetMultimap.Builder<BucketType, CSerializable> builder = ImmutableSetMultimap.builder();

        for (T coord : coordinates) {
            builder.put(BucketType.IDENTITY, bucketForOneWrite(coord));
        }

        return builder.build();
    }

    @Override
    public CSerializable bucketForRead(T coordinate, BucketType bucketType) {
        return bucketForOneRead(coordinate, bucketType);
    }

    @Override
    public T deserialize(byte[] coord) {
        return deserializeOne(coord);
    }
}
