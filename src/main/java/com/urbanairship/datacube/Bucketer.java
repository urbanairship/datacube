/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
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
    CSerializable bucketForRead(F coordinate, BucketType bucketType);

    F deserialize(byte[] coord, BucketType bucketType);

    /**
     * Return all bucket types that exist in this dimension.
     */
    List<BucketType> getBucketTypes();


    /**
     * This identity/no-op bucketer class is implicitly used for dimensions that don't choose a
     * bucketer.
     */
    abstract class IdentityBucketer implements Bucketer<CSerializable> {
        private final List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);

        @Override
        public SetMultimap<BucketType, CSerializable> bucketForWrite(CSerializable coordinate) {
            return ImmutableSetMultimap.of(BucketType.IDENTITY, coordinate);
        }

        @Override
        public CSerializable bucketForRead(CSerializable coordinateField, BucketType bucketType) {
            return coordinateField;
        }

        @Override
        public abstract CSerializable deserialize(byte[] coordinate, BucketType bucketType);

        @Override
        public List<BucketType> getBucketTypes() {
            return bucketTypes;
        }
    }

    abstract class CollectionBucketer<T> implements Bucketer<Collection<T>> {
        protected abstract CSerializable bucketForOneRead(T coordinate, BucketType bucketType);

        protected abstract T deserializeOne(byte[] coord, BucketType bucketType);

        protected abstract CSerializable bucketForOneWrite(T coord);

        @Override
        public SetMultimap<BucketType, CSerializable> bucketForWrite(Collection<T> coordinates) {
            ImmutableSetMultimap.Builder<BucketType, CSerializable> builder = ImmutableSetMultimap.builder();

            for (T coord : coordinates) {
                builder.put(BucketType.IDENTITY, bucketForOneWrite(coord));
            }

            return builder.build();
        }


        public CSerializable bucketForRead(Collection<T> coordinate, BucketType bucketType) {
            Preconditions.checkState(coordinate.size() == 1);
            return bucketForOneRead(coordinate.iterator().next(), bucketType);
        }

        @Override
        public Collection<T> deserialize(byte[] coord, BucketType bucketType) {
            return ImmutableSet.of(deserializeOne(coord, bucketType));
        }
    }
}
