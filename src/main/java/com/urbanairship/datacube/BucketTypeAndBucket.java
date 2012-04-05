package com.urbanairship.datacube;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;

/**
 * A simple struct to hold a bucket type along with a corresponding bucket.
 * 
 * For example, bucket type "city" and bucket "portland."
 */
public class BucketTypeAndBucket {
    static final BucketTypeAndBucket WILDCARD = new BucketTypeAndBucket(BucketType.WILDCARD,
            ArrayUtils.EMPTY_BYTE_ARRAY);
    
    public final BucketType bucketType;
    public final byte[] bucket;
    
    public BucketTypeAndBucket(BucketType bucketType, byte[] bucket) {
        this.bucketType = bucketType;
        this.bucket = bucket;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(bucketType.toString());
        sb.append(": ");
        sb.append(Hex.encodeHex(bucket));
        sb.append(")");
        return sb.toString();
    }
}