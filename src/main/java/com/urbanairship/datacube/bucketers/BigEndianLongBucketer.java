package com.urbanairship.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.LongSerializable;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Long.
 */
public class BigEndianLongBucketer implements Bucketer<Long> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    
    @Override
    public CSerializable bucketForWrite(Long coordinate, BucketType bucketType) {
        assert bucketType == BucketType.IDENTITY;
        return new LongSerializable(coordinate);
    }

    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        assert bucketType == BucketType.IDENTITY;
        return new LongSerializable((Long)coordinate);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }
}
