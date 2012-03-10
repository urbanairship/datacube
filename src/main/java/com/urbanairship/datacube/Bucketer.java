package com.urbanairship.datacube;

import java.util.List;

import com.google.common.collect.ImmutableList;

public interface Bucketer {
    public static final Bucketer IDENTITY = new IdentityBucketer();
    
    public byte[] getBucket(byte[] coord, BucketType bucketType);
    
    public List<BucketType> getBucketTypes();
    
    /**
     * This bucketer is implicitly used for dimensions that are not bucketed.
     */
    public class IdentityBucketer implements Bucketer {
        @Override
        public byte[] getBucket(byte[] coord, BucketType bucketType) {
            if(bucketType != BucketType.IDENTITY) {
                throw new AssertionError();
            }
            return coord;
        }

        @Override
        public List<BucketType> getBucketTypes() {
            return ImmutableList.of(BucketType.IDENTITY);
        }
    }
}
