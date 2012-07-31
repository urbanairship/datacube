/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.LongSerializable;

import java.util.List;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Long.
 */
public class BigEndianLongBucketer implements Bucketer<Long> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    private final static LongSerializable LONG_SERIALIZABLE = new LongSerializable(0L);

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

    @Override
    public Long readBucket(BoxedByteArray key, BucketType btype) {
        return LONG_SERIALIZABLE.deserialize(key.bytes);
    }
}
