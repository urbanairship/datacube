/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

public class WriteBuilder {
    private final Map<Dimension<?>,SetMultimap<BucketType,byte[]>> buckets;
	private Map<RollupFilter,Object> filterAttachments = Maps.newHashMap();
    
    public WriteBuilder(DataCube<?> cube) {
        buckets = Maps.newHashMap();
    }
    
    public <O> WriteBuilder at(Dimension<O> dimension, O coord) {
        int expectedBucketLen = dimension.getNumFieldBytes(); 
        Bucketer<O> bucketer = dimension.getBucketer();

        SetMultimap<BucketType,CSerializable> bucketsAndCoords = bucketer.bucketForWrite(coord);//.serialize();
        buckets.put(dimension, serializeCoords(bucketsAndCoords));
            
        return this;
    }
    
    /**
     * Attach an arbitrary object to this write. This object will be presented to the rollup
     * filters, so they can use it to make filtering decisions.
     */
    public WriteBuilder attachForRollupFilter(RollupFilter rollupFilter, Object attachment) {
        filterAttachments.put(rollupFilter, attachment);
        return this;
    }
    
    Map<RollupFilter,Object> getRollupFilterAttachments() {
        return filterAttachments;
    }
    
    Map<Dimension<?>, SetMultimap<BucketType,byte[]>> getBuckets() {
        return buckets;
    }
    
    private static SetMultimap<BucketType,byte[]> serializeCoords(
            Multimap<BucketType,CSerializable> inputMap) {
        SetMultimap<BucketType,byte[]> outputMap = HashMultimap.create();
        for(Map.Entry<BucketType,CSerializable> e: inputMap.entries()) {
            outputMap.put(e.getKey(), e.getValue().serialize());
        }
        return outputMap;
    }
    
    public String toString() {
        return buckets.toString();
    }
}
