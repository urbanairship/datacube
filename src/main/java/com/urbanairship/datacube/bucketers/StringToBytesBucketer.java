/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.StringSerializable;

import java.util.List;

/**
 * You can use this bucketer to avoid writing your own, in the case where:
 *  - You have a cube coordinate that's a String
 *  - You want the bucketer to pass through the String unchanged as the bucket
 */
public class StringToBytesBucketer implements Bucketer<String> {
    private static final StringToBytesBucketer instance = new StringToBytesBucketer();
    private static final StringSerializable STRING_SERIALIZABLE = new StringSerializable("");

    @Override
    public CSerializable<String> bucketForWrite(String coordinateField, BucketType bucketType) {
        return bucket(coordinateField, bucketType);
    }

    @Override
    public CSerializable<String> bucketForRead(Object coordinateField, BucketType bucketType) {
        return bucket((String)coordinateField, bucketType);
    }

    private CSerializable<String> bucket(String coordinateField, BucketType bucketType) {
        return new StringSerializable(coordinateField);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return ImmutableList.of(BucketType.IDENTITY);
    }

    @Override
    public String readBucket(BoxedByteArray key, BucketType btype) {
        return STRING_SERIALIZABLE.deserialize(key.bytes);
    }

    public static final StringToBytesBucketer getInstance() {
        return instance;
    }
}
