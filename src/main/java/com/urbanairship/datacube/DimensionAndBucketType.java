/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.List;

/**
 * A struct for specifying a dimension and bucket type to be used in a rollup.
 */
public class DimensionAndBucketType {
    public final Dimension<?> dimension;
    public final BucketType bucketType;
    
    public DimensionAndBucketType(Dimension<?> dimension, BucketType bucketType) {
//        List<BucketType> knownBucketTypes = dimension.getBucketer().getBucketTypes(); 
//        if(!knownBucketTypes.contains(bucketType)) {
//            throw new IllegalArgumentException("The given dimension " + dimension + 
//                    " doesn't have a bucket type " + bucketType + ". Its available bucket types are" +
//                    knownBucketTypes);
//        }
        this.dimension = dimension;
        this.bucketType = bucketType;
    }
    
    /**
     * Use this constructor when you're specifying an Rollup involving a dimension that
     * doesn't use bucketing.
     */
    public DimensionAndBucketType(Dimension<?> dimension) {
//        if(dimension.isBucketed()) {
//            List<BucketType> knownBucketTypes = dimension.getBucketer().getBucketTypes();
//            throw new IllegalArgumentException("The dimension " + dimension + 
//                    " is a bucketed dimension, you must choose a bucket when specifying an" +
//                    " Rollup. The available buckets are: " + knownBucketTypes); 
//        }
        this.dimension = dimension;
        this.bucketType = BucketType.IDENTITY;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(dimension);
        sb.append("+");
        sb.append(bucketType);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucketType == null) ? 0 : bucketType.hashCode());
        result = prime * result + ((dimension == null) ? 0 : dimension.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        // Eclipse-generated and slightly modified
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DimensionAndBucketType other = (DimensionAndBucketType) obj;
        if (bucketType == null) {
            if (other.bucketType != null)
                return false;
        } else if (bucketType != other.bucketType)
            return false;
        if (dimension == null) {
            if (other.dimension != null)
                return false;
        } else if (dimension != other.dimension)
            return false;
        return true;
    }
}