package com.urbanairship.datacube;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * You can use this bucketer to avoid writing your own, in the case where:
 *  - You have a cube coordinate that's a String
 *  - You want the bucketer to pass through the String unchanged as the bucket
 */
public class StringToBytesBucketer implements Bucketer<String> {
    private static final StringToBytesBucketer instance = new StringToBytesBucketer();
    
    @Override
    public CSerializable bucketForWrite(String coordinateField, BucketType bucketType) {
        return bucket(coordinateField, bucketType);
    }

    @Override
    public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
        return bucket((String)coordinateField, bucketType);
    }

    private CSerializable bucket(String coordinateField, BucketType bucketType) {
        return new StringSerializable(coordinateField);
    }
    
    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    public static final StringToBytesBucketer getInstance() {
        return instance;
    }
}
