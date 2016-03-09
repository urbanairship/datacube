/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.serializables.LongSerializable;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Long.
 */
public class BigEndianLongBucketer implements Bucketer<Long> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    
    @Override
    public SetMultimap<BucketType,CSerializable> bucketForWrite(Long coordinate) {
        return ImmutableSetMultimap.<BucketType,CSerializable>of(
                BucketType.IDENTITY, new LongOp(coordinate));
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
    public Long deserialize(byte[] coord, BucketType bucketType) {
        if (coord == null || coord.length == 0) {
            throw new IllegalArgumentException(
                    "Null or Zero length byte array can not be" + " deserialized");
        } else if (coord.length > 8) {
            throw new IllegalArgumentException("BigEndianLongBucketer can not have coordinate " +
                    "byte array size more than number of bytes require by Long");
        }
        return LongSerializable.deserialize(coord);
    }
}
