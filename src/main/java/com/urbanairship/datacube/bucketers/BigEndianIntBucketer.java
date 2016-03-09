/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.IntSerializable;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Integer.
 */
public class BigEndianIntBucketer extends AbstractIdentityBucketer<Integer> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);

    @Override
    public CSerializable makeSerializable(Integer coord) {
        return new IntSerializable(coord);
    }

    @Override
    public Integer deserialize(byte[] coord, BucketType bucketType) {
        if (coord.length > 4)
            throw new IllegalArgumentException("BigEndianIntBucketer can not have coordinate " +
                    "byte array size more than number of bytes require by Integer");
        return IntSerializable.deserialize(coord);
    }
}
