package com.urbanairship.datacube;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * If you're an API client, you probably want to use {@link WriteAddressBuilder} instead.
 */
public class WriteAddress {
    private final Map<DimensionAndBucketType,byte[]> buckets = Maps.newHashMap(); 
    private final Multimap<Dimension<?>,BucketType> bucketsOfInterest;
    
    public WriteAddress(DataCube<?> cube) {
        this.bucketsOfInterest = cube.getBucketsOfInterest();
    }
    
    public <O> void at(Dimension<O> dimension, O coord) {
        int expectedBucketLen = dimension.getNumFieldBytes(); 
        Bucketer<O> bucketer = dimension.getBucketer();
        Collection<BucketType> bucketTypesToEvaluate = bucketsOfInterest.get(dimension);
        // Only evaluate the bucketer for the bucket types that are used by a Rollup. This saves
        // resources by not evaluating bucket types that won't be stored.
        for(BucketType bucketType: bucketTypesToEvaluate) {
            byte[] bucket = bucketer.bucketForWrite(coord, bucketType).serialize();
            if(bucket.length != expectedBucketLen) {
                throw new IllegalArgumentException("Bucket serialized to " + bucket.length + 
                        " bytes but should have been " + expectedBucketLen + " bytes");
            }
            buckets.put(new DimensionAndBucketType(dimension, bucketType), bucket);
        }
    }
    
    Map<DimensionAndBucketType,byte[]> getBuckets() {
        return buckets;
    }
    
//    public <O> void at(Dimension<?> dimension, BucketType bucketType, O bucket) {
//        super.at(dimension, bucketType, 
//                dimension.getBucketer().bucketForRead(bucket, bucketType).serialize());
//    }
    
    
    
//    public <F> void at(Dimension<F> dimension, F bucket) {
//        this.at(dimension, BucketType.IDENTITY, bucket);
//    }
//    
//    public <F> void at(Dimension<F> dimension, BucketType bucketType, F bucket) {
//        CSerializable bucket = dimension.getBucketer().bucketForWrite(bucket, bucketType);
//        super.at(dimension, BucketType.IDENTITY, bucket.serialize());
//    }
}
