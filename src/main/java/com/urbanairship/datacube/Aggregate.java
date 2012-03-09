package com.urbanairship.datacube;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;


/**
 * Use this class to describe an aggregate value that you want the datacube to keep.
 * 
 * For example, if your counting events with the dimensions (color, size, flavor), there will be no 
 * way to get a total count for given color unless you use an Aggregate.
 */
public class Aggregate {
    public Aggregate(List<DimensionAndBucket> toAggregate) {
        throw new NotImplementedException();
    }
    
    /**
     * Use this factory method when you want an aggregate counter for 
     */
    public static Aggregate simpleAggregate(List<Dimension> dimensions) {
        throw new NotImplementedException();
    }
    
    public static class DimensionAndBucket {
        public DimensionAndBucket(Dimension dimension) {
            throw new NotImplementedException();
        }
        
        public DimensionAndBucket(Dimension dimension, BucketType bucketType) {
            throw new NotImplementedException();
        }
    }
    
}
