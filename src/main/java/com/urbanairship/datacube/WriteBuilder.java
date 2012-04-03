package com.urbanairship.datacube;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class WriteBuilder {
    private final Multimap<Dimension<?>,BucketType> bucketsOfInterest;
    private final Map<DimensionAndBucketType,byte[]> buckets = Maps.newHashMap(); 
    private Map<RollupFilter,Object> filterAttachments = Maps.newHashMap();
    
    public WriteBuilder(DataCube<?> cube) {
        this.bucketsOfInterest = cube.getBucketsOfInterest();
    }
    
    public <O> WriteBuilder at(Dimension<O> dimension, O coord) {
        int expectedBucketLen = dimension.getNumFieldBytes(); 
        Bucketer<O> bucketer = dimension.getBucketer();
        Collection<BucketType> bucketTypesToEvaluate = bucketsOfInterest.get(dimension);
        // Only evaluate the bucketer for the bucket types that are used by a Rollup. This saves
        // resources by not evaluating bucket types that won't be stored.
        for(BucketType bucketType: bucketTypesToEvaluate) {
            byte[] bucket = bucketer.bucketForWrite(coord, bucketType).serialize();
            
            if(bucket.length != expectedBucketLen && !dimension.getDoIdSubstitution()) {
                throw new IllegalArgumentException("Bucket serialized to " + bucket.length + 
                        " bytes but should have been " + expectedBucketLen + " bytes");
            }
            buckets.put(new DimensionAndBucketType(dimension, bucketType), bucket);
        }
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
    
    public Map<DimensionAndBucketType,byte[]> getBuckets() {
        return buckets;
    }
    
}
