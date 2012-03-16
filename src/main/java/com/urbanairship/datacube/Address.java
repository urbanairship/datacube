package com.urbanairship.datacube;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class Address {
    private static final Logger log = LogManager.getLogger(Address.class);
    
    private final Map<Dimension<?>,BucketTypeAndBucket> buckets;
    
    private static final byte[] WILDCARD_FIELD = new byte[] {0};
    private static final byte[] NON_WILDCARD_FIELD = new byte[] {1};
    
    public Address() {
        buckets = Maps.newHashMap();
    }
    
    public void at(Dimension<?> dimension, byte[] value) {
        if(dimension.isBucketed()) {
            throw new IllegalArgumentException("Dimension " + dimension + 
                    " is a bucketed dimension. You can't query it without a bucket.");
        }
        at(dimension, BucketType.IDENTITY, value);
    }
    
    void at(Dimension<?> dimension, BucketType bucketType, byte[] bucket) {
        buckets.put(dimension, new BucketTypeAndBucket(bucketType, bucket));
    }
    
    void at(Dimension<?> dimension, BucketTypeAndBucket bucketAndCoord) {
        buckets.put(dimension, bucketAndCoord);
    }
    
    public BucketTypeAndBucket get(Dimension<?> dimension) {
        return buckets.get(dimension);
    }
    
    Map<Dimension<?>,BucketTypeAndBucket> getBuckets() {
        return buckets;
    }
    
    /**
     * Get a byte array encoding the buckets of this cell in the Cube. For internal use only.
     */
    byte[] toKey(List<Dimension<?>> dimensions) {
        boolean sawOnlyWildcardsSoFar = true;
        List<byte[]> reversedKeyElems = Lists.newArrayListWithCapacity(dimensions.size());
        
        // We build up the key in reverse order so we can leave off wildcards at the end of the key.
        // The reasoning for this is complicated, please see design docs.
        for(int i=dimensions.size()-1; i >= 0; i--) {
            Dimension<?> dimension = dimensions.get(i);
            BucketTypeAndBucket bucketAndCoord = buckets.get(dimension);
            
            int thisDimBucketLen = dimension.getNumFieldBytes();
            int thisDimBucketTypeLen = dimension.getBucketPrefixSize();
            
            if(bucketAndCoord == BucketTypeAndBucket.WILDCARD || bucketAndCoord == null) {
                // Special logic, wildcards at the end of the key are omitted
                if(sawOnlyWildcardsSoFar) {
                    continue;
                }
                reversedKeyElems.add(new byte[thisDimBucketTypeLen + thisDimBucketLen]);
                reversedKeyElems.add(WILDCARD_FIELD);
            } else {
                sawOnlyWildcardsSoFar = false;
                
                if(bucketAndCoord.bucket.length != thisDimBucketLen) {
                    throw new IllegalArgumentException("Field length was wrong (after bucketing). " +
                            " For dimension " + dimension + ", expected length " + 
                            dimension.getNumFieldBytes() + " but was " + bucketAndCoord.bucket.length);
                }
                
                byte[] bucketTypeId = bucketAndCoord.bucketType.getUniqueId(); 
                if(bucketTypeId.length != thisDimBucketTypeLen) {
                    throw new RuntimeException("Bucket prefix length was wrong. For dimension " + 
                            dimension + ", expected bucket prefix of length " + dimension.getBucketPrefixSize() +
                            " but the bucket prefix was " + Arrays.toString(bucketTypeId) +
                            " which had length" + bucketTypeId.length);
                }
                reversedKeyElems.add(bucketAndCoord.bucket);
                reversedKeyElems.add(bucketTypeId);
                reversedKeyElems.add(NON_WILDCARD_FIELD);
            }
        }
        
        List<byte[]> keyElemsInOrder = Lists.reverse(reversedKeyElems);
        
        int totalKeySize = 0;
        for(byte[] keyElement: keyElemsInOrder) {
            totalKeySize += keyElement.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(totalKeySize);
        
        
        for(byte[] keyElement: keyElemsInOrder) {
            bb.put(keyElement);
        }
        
        if(bb.remaining() != 0) {
            throw new AssertionError("Key length calculation was somehow wrong, " + 
                    bb.remaining() + " bytes remaining");
        }
        return bb.array();
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        boolean firstLoop = true;
        for(Entry<Dimension<?>,BucketTypeAndBucket> e: buckets.entrySet()) {
            if(!firstLoop) {
                sb.append(", ");
            }
            firstLoop = false;
            Dimension<?> dimension = e.getKey();
            sb.append(dimension);
            sb.append(": ");
            sb.append(e.getValue());
        }
        sb.append(")");
        return sb.toString();
    }
}
