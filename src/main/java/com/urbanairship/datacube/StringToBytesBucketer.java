package com.urbanairship.datacube;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.google.common.collect.ImmutableList;

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
        try {
            return new BytesSerializable(coordinateField.getBytes("UTF8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }        
    }
    
    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    public static final StringToBytesBucketer getInstance() {
        return instance;
    }
}
