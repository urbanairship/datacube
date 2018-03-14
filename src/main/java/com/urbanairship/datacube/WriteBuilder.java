/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.Map;
import java.util.Set;

public class WriteBuilder {
    private final Map<Dimension<?>, SetMultimap<BucketType, byte[]>> buckets;

    public WriteBuilder() {
        buckets = Maps.newHashMap();
    }

    public <O> WriteBuilder at(Dimension<O> dimension, O coord) {
        Bucketer<O> bucketer = dimension.getBucketer();

        SetMultimap<BucketType, CSerializable> bucketsAndCoords = bucketer.bucketForWrite(coord);
        buckets.put(dimension, serializeCoords(bucketsAndCoords));

        return this;
    }

    Map<Dimension<?>, SetMultimap<BucketType, byte[]>> getBuckets() {
        return buckets;
    }

    private static SetMultimap<BucketType, byte[]> serializeCoords(
            Multimap<BucketType, CSerializable> inputMap) {
        SetMultimap<BucketType, byte[]> outputMap = HashMultimap.create();
        for (Map.Entry<BucketType, CSerializable> e : inputMap.entries()) {
            outputMap.put(e.getKey(), e.getValue().serialize());
        }
        return outputMap;
    }

    public String toString() {
        return buckets.toString();
    }
}
