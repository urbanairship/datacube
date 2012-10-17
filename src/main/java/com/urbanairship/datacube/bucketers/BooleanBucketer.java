/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.BooleanSerializable;

import java.util.List;

/**
 *  BooleanBucketer
 *  You can use this bucketer for cases where:
 *  - You have a cube coordinate that is boolean
 *  - You want to store that boolean as a byte[0] for false or a byte[1] for true.
 */

public class BooleanBucketer extends AbstractIdentityBucketer<Boolean> {
    private static final BooleanBucketer instance = new BooleanBucketer();

    @Override
    public CSerializable makeSerializable(Boolean coordinateField) {
        return new BooleanSerializable(coordinateField);
    }

    /**
     * One instance of this class can be reused by multiple cubes/dimensions/etc.
     * This method returns the static instance to make it easy to reuse.
     */
    public static final BooleanBucketer getInstance() {
        return instance;
    }
}

