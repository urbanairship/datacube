package com.urbanairship.datacube.bucketers;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.StringSerializable;

/**
 * A Bucketer that accepts a list of strings and returns each one as a bucket. For example, this is useful when an
 * input type has an arbitrary number of tags associated with it (there would be a single "tags" dimension").
 */
public class TagsBucketer implements Bucketer<Collection<String>> {

    @Override
    public SetMultimap<BucketType, CSerializable> bucketForWrite(
            Collection<String> tags) {
        ImmutableSetMultimap.Builder<BucketType,CSerializable> builder =
                ImmutableSetMultimap.builder();
        for(String tag: tags) {
            builder.put(BucketType.IDENTITY, new StringSerializable(tag));
        }
        return builder.build();
    }

    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        return new StringSerializable((String)coordinate);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }
}
