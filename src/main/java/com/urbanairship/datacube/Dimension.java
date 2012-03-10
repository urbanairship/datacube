package com.urbanairship.datacube;


public class Dimension {
    private final String name;
    private final Bucketer bucketer;
    private final boolean doIdSubstitution; 
    private final int numFieldBytes;
    private final int bucketPrefixSize;
    
    /**
     * For dimensions where a single input coord affects multiple buckets within that dimension.
     * For example, a single input data point might affect hourly, daily, and monthly counts.
     * 
     * @param doIdSubstitution whether to use the immutable coord->uniqueId substition service
     */
    public Dimension(String name, Bucketer bucketer, 
            boolean doIdSubstitution, int fieldBytes) {
        this.name = name;
        this.bucketer = bucketer;
        this.doIdSubstitution = doIdSubstitution;
        this.numFieldBytes = fieldBytes;
        
        // Make sure all bucket unique id prefixes have the same length
        if(bucketer != null) {
            Integer previousLen = null;
            for(BucketType bucketType: bucketer.getBucketTypes()) {
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
        } else {
            bucketPrefixSize = 0;
        }
    }
    
    /**
     * For simple dimensions without bucketing. For example, if you're counting people by zip code,
     * a single person lives in a single zip code, and not multiple zip codes.
     *
     * @param doIdSubstitution whether to use the immutable coord->uniqueId substition service
     */
    public Dimension(String name, boolean doIdSubstitution, int fieldBytes) {
        this(name, Bucketer.IDENTITY, doIdSubstitution, fieldBytes);
    }
    
    public String getName() {
        return name;
    }
    
    int getNumFieldBytes() {
        return numFieldBytes;
    }
    
    public String toString() {
        return name;
    }
    
    Bucketer getBucketer() {
        return bucketer;
    }
    
    boolean getDoIdSubstitution() {
        return doIdSubstitution;
    }
    
    int getBucketPrefixSize() {
        return bucketPrefixSize;
    }
}
