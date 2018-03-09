package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.StringSerializable;

import java.util.List;

/**
 * A Bucketer that accepts a list of strings and returns each one as a bucket. For example, this is useful when an
 * input type has an arbitrary number of tags associated with it (there would be a single "tags" dimension").
 */
public class TagsBucketer extends Bucketer.CollectionBucketer<String> {

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    @Override
    protected CSerializable bucketForOneRead(String coordinate, BucketType bucketType) {
        return new StringSerializable(coordinate);
    }

    @Override
    protected String deserializeOne(byte[] coord, BucketType bucketType) {
        return StringSerializable.deserialize(coord);
    }

    @Override
    protected CSerializable bucketForOneWrite(String coord) {
        return new StringSerializable(coord);
    }
}
