package com.urbanairship.datacube;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class ExplodedAddress {
    Map<Dimension,BucketTypeAndCoord> coordinates = Maps.newHashMap();
    
    public void at(Dimension dimension, byte[] value) {
        if(dimension.getBucketer() != Bucketer.IDENTITY) {
            throw new IllegalArgumentException("Dimension " + dimension + 
                    " is a bucketed dimension. You can't query it without a bucket.");
        }
        at(dimension, BucketType.IDENTITY, value);
    }
    
    public void at(Dimension dimension, BucketType bucketType, byte[] coordinate) {
        coordinates.put(dimension, new BucketTypeAndCoord(bucketType, coordinate));
    }
    
    public void at(Dimension dimension, BucketTypeAndCoord bucketAndCoord) {
        coordinates.put(dimension, bucketAndCoord);
    }
    
    public BucketTypeAndCoord get(Dimension dimension) {
        return coordinates.get(dimension);
    }
    
    /**
     * Get a byte array encoding the coordinates of this cell in the Cube. For internal use only.
     * @return
     */
    byte[] toKey(List<Dimension> dimensions) {
        int totalKeySize = 0;
        for(Dimension dimension: dimensions) {
            totalKeySize += dimension.getNumFieldBytes();
            totalKeySize += dimension.getBucketPrefixSize();
        }
        ByteBuffer bb = ByteBuffer.allocate(totalKeySize);
        
        for(Dimension dimension: dimensions) {
            BucketTypeAndCoord bucketAndCoord = coordinates.get(dimension);
            if(bucketAndCoord == null) {
                throw new IllegalArgumentException("No coordinate for the dimension " +
                        dimension.getName());
            }
            
            if(bucketAndCoord.coord.length != dimension.getNumFieldBytes()) {
                throw new IllegalArgumentException("Field length was wrong. For dimension " +
                        dimension + ", expected length " + dimension.getNumFieldBytes() + 
                        " but was " + bucketAndCoord.coord.length);
            }
            
            byte[] bucketPrefix = bucketAndCoord.bucketType.getUniqueId(); 
            if(bucketPrefix.length != dimension.getBucketPrefixSize()) {
                throw new RuntimeException("Bucket prefix length was wrong. For dimension " + 
                        dimension + ", expected bucket prefix of length " + dimension.getBucketPrefixSize() +
                        " but the bucket prefix was " + Arrays.toString(bucketPrefix) +
                        " which had length" + bucketPrefix.length);
            }
            
            bb.put(bucketPrefix);
            bb.put(bucketAndCoord.coord);
        }
        return bb.array();
    }
    
}
