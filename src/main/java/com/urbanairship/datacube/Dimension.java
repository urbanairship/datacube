package com.urbanairship.datacube;

import java.util.List;

public class Dimension {
    private final String name;
    private final List<BucketType> bucketTypes;
    private final Bucketer bucketer;
    private final boolean doIdSubstitution; 
    private final int numFieldBytes;
    
    /**
     * For dimensions where a single input value affects multiple buckets within that dimension.
     * For example, a single input data point might affect hourly, daily, and monthly counts.
     * 
     * @param doTagSubstitution whether to use the immutable value->uniqueId substition service
     */
    public Dimension(String name, List<BucketType> bucketTypes, Bucketer bucketer, 
            boolean doIdSubstitution, int fieldBytes) {
        this.name = name;
        this.bucketTypes = bucketTypes;
        this.bucketer = bucketer;
        this.doIdSubstitution = doIdSubstitution;
        this.numFieldBytes = fieldBytes;
    }
    
    /**
     * For simple dimensions without bucketing. For example, if you're counting people by zip code,
     * a single person lives in a single zip code, and not multiple zip codes.
     *
     * @param doIdSubstitution whether to use the immutable value->uniqueId substition service
     */
    public Dimension(String name, boolean doIdSubstitution, int fieldBytes) {
        this.name = name;
        this.bucketTypes = null;
        this.bucketer = null;
        this.doIdSubstitution = doIdSubstitution;
        this.numFieldBytes = fieldBytes;
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
}
