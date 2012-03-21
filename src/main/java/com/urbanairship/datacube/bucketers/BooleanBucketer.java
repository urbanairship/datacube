package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.BooleanSerializable;
import com.urbanairship.datacube.serializables.LongSerializable;

import java.util.List;

/**
 *
 */

public class BooleanBucketer implements Bucketer<Boolean> {
    private static final BooleanBucketer instance = new BooleanBucketer();

    @Override
    public CSerializable bucketForWrite(Boolean coordinateField, BucketType bucketType) {
        return bucket(coordinateField, bucketType);
    }

    @Override
    public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
        return bucket((Boolean)coordinateField, bucketType);
    }

    private CSerializable bucket(Boolean coordinateField, BucketType bucketType) {
        return new BooleanSerializable(coordinateField);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    public static final BooleanBucketer getInstance() {
        return instance;
    }
}

