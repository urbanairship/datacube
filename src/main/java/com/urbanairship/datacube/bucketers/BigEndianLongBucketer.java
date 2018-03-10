/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.UniBucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.serializables.LongSerializable;

import java.util.List;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Long.
 */
public class BigEndianLongBucketer implements UniBucketer<Long> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);

    @Override
    public SetMultimap<BucketType, CSerializable> bucketForWrite(Long coordinate) {
        return ImmutableSetMultimap.<BucketType, CSerializable>of(
                BucketType.IDENTITY, new LongOp(coordinate));
    }

    @Override
    public CSerializable bucketForRead(Long coordinate, BucketType bucketType) {
        assert bucketType == BucketType.IDENTITY;
        return new LongSerializable(coordinate);
    }

    @Override
    public Long deserialize(byte[] coord) {
        return LongSerializable.deserialize(coord);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }
}
