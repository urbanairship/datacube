package com.urbanairship.datacube;

import java.util.List;

/**
 * Describes a high-level dimension of the hypercube. For example, "time", "location",
 * and "color" would be possible dimensions.
 * 
 * @param <F> the type of the coordinates for this dimension. For example, a time dimension might use the type 
 * DateTime or Long for its coordinates. As another example, a location dimension might use a custom LatLong type 
 * as its input coordinate. This is the type that will be passed as input to the bucketer for this dimension.
 */
public class Dimension<F> {
    private final String name;
    private final Bucketer<F> bucketer;
    private final boolean doIdSubstitution; 
    private final int numFieldBytes;
    private final int bucketPrefixSize;
    private final boolean isBucketed;
    
    /**
     * For dimensions where a single input bucket affects multiple buckets within that dimension.
     * For example, a single input data point might affect hourly, daily, and monthly counts.
     * 
     * @param doIdSubstitution whether to use the immutable bucket->uniqueId substition service
     */
    public Dimension(String name, Bucketer<F> bucketer, 
            boolean doIdSubstitution, int fieldBytes) {
        this.name = name;
        this.bucketer = bucketer;
        this.doIdSubstitution = doIdSubstitution;
        this.numFieldBytes = fieldBytes;
        
        // Make sure all bucket unique id prefixes have the same length
        Integer previousLen = null;
        List<BucketType> bucketTypes = bucketer.getBucketTypes();
        if(bucketTypes.size() == 0) {
            throw new IllegalArgumentException("Invalid bucketer. There must at least one bucket type");
        } else if(bucketTypes.size() == 1 || bucketTypes.get(0) == BucketType.IDENTITY) {
            isBucketed = false;
            bucketPrefixSize = 0;
        } else {
            for(BucketType bucketType: bucketTypes) {
                int thisBucketLen = bucketType.getUniqueId().length;
                if(previousLen == null) {
                    previousLen = thisBucketLen;
                } else {
                    if(previousLen != thisBucketLen) {
                        throw new IllegalArgumentException("BucketTypes for dimension " + 
                                name + " had different lengths " + previousLen + " and "
                                + thisBucketLen);
                    }
                }
            }
            bucketPrefixSize = previousLen;
            isBucketed = true;
        }
    }
    
    public String getName() {
        return name;
    }
    
    public int getNumFieldBytes() {
        return numFieldBytes;
    }
    
    public String toString() {
        return name;
    }
    
    Bucketer<F> getBucketer() {
        return bucketer;
    }
    
    public boolean getDoIdSubstitution() {
        return doIdSubstitution;
    }
    
    int getBucketPrefixSize() {
        return bucketPrefixSize;
    }
    
    boolean isBucketed() {
        return isBucketed;
    }
}
