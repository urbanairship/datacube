package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.EnumSerializable;

import java.util.List;
import java.util.Map;

public class MultiEnumBucketer extends Bucketer.CollectionBucketer<Enum<?>> {
    private Map<BucketType, Class<? extends Enum>> bucketTypeMap;
    private int maxEnumSize;

    public MultiEnumBucketer(Map<BucketType, Class<? extends Enum>> bucketTypeMap, int maxEnumSize) {
        this.bucketTypeMap = bucketTypeMap;
        this.maxEnumSize = maxEnumSize;
    }


    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.copyOf(bucketTypeMap.keySet());
    }

    @Override
    protected CSerializable bucketForOneRead(Enum<?> coordinate, BucketType bucketType) {
        return new EnumSerializable<>(coordinate, maxEnumSize);
    }

    @Override
    protected Enum<?> deserializeOne(byte[] coord, BucketType bucketType) {
        return EnumSerializable.deserialize(bucketTypeMap.get(bucketType), coord);
    }

    @Override
    protected CSerializable bucketForOneWrite(Enum<?> coord) {
        return new EnumSerializable<>(coord, maxEnumSize);
    }
}
