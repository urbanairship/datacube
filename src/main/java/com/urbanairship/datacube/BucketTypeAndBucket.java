/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bucket);
        result = prime * result + ((bucketType == null) ? 0 : bucketType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BucketTypeAndBucket other = (BucketTypeAndBucket) obj;
        if (!Arrays.equals(bucket, other.bucket))
            return false;
        if (bucketType == null) {
            if (other.bucketType != null)
                return false;
        } else if (!bucketType.equals(other.bucketType))
            return false;
        return true;
    }
}