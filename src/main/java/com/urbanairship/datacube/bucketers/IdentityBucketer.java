package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.UniBucketer;

import java.util.List;

/**
 * This identity/no-op bucketer class is implicitly used for dimensions that don't choose a
 * bucketer.
 */
public abstract class IdentityBucketer implements UniBucketer<CSerializable> {
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
    public abstract CSerializable deserialize(byte[] coordinate);

    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }
}
