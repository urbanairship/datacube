/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.List;

public interface Bucketer<F> {
    /**
     * When writing to the cube at some address, the address will have one coordinate for each
     * dimension in the cube, for example (time: 348524388, location: portland). For each
     * dimension, for each bucket type within that dimension, the bucketer must transform the
     * input data into the bucket that should be used to store the data.
     */
    SetMultimap<BucketType, CSerializable> bucketForWrite(F coordinate);

    /**
     * When reading from the cube, the reader specifies some coordinates from which to read.
     * The bucketer can choose which cube coordinates to read from based on these input
     * coordinates. For example, if the reader asks for hourly counts (the Hourly BucketType) and
     * passes a timestamp, the bucketer could return the timestamp rounded down to the hour floor.
     */
    CSerializable bucketForRead(Object coordinate, BucketType bucketType);

    /**
     * Return all bucket types that exist in this dimension.
     */
    List<BucketType> getBucketTypes();


    /**
     * This identity/no-op bucketer class is implicitly used for dimensions that don't choose a
     * bucketer.
     */
    class IdentityBucketer implements Bucketer<CSerializable> {
        private final List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);

        @Override
        public SetMultimap<BucketType, CSerializable> bucketForWrite(CSerializable coordinate) {
            return ImmutableSetMultimap.of(BucketType.IDENTITY, coordinate);
        }

        @Override
        public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
            return (CSerializable) coordinateField;
        }

        @Override
        public List<BucketType> getBucketTypes() {
            return bucketTypes;
        }
    }
}
