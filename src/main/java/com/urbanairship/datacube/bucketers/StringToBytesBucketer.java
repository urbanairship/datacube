/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import java.util.List;


import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.StringSerializable;

/**
 * You can use this bucketer to avoid writing your own, in the case where:
 *  - You have a cube coordinate that's a String
 *  - You want the bucketer to pass through the String unchanged as the bucket
 */
public class StringToBytesBucketer extends AbstractIdentityBucketer<String> {
    private static final StringToBytesBucketer instance = new StringToBytesBucketer();
    
//    @Override
//    public CSerializable bucketForWrite(String coordinateField, BucketType bucketType) {
//        return bucket(coordinateField, bucketType);
//    }
//
//    @Override
//    public CSerializable bucketForRead(Object coordinateField, BucketType bucketType) {
//        return bucket((String)coordinateField, bucketType);
//    }
//
//    private CSerializable bucket(String coordinateField, BucketType bucketType) {
//        return new StringSerializable(coordinateField);
//    }
//    
//    @Override
//    public List<BucketType> getBucketTypes() {
//        return ImmutableList.of(BucketType.IDENTITY);
//    }

    public static final StringToBytesBucketer getInstance() {
        return instance;
    }

    @Override
    public CSerializable makeSerializable(String coord) {
        return new StringSerializable(coord);
    }
}
