/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.serializables.IntSerializable;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Integer.
 */
public class BigEndianIntBucketer extends AbstractIdentityBucketer<Integer> {
    @Override
    public CSerializable makeSerializable(Integer coord) {
        return new IntSerializable(coord);
    }
}
