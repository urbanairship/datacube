/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.List;

/**
 * Use this to specify the location of a cell to read from a datacube.
 */
public class ReadBuilder {
    private final Address address;

    public ReadBuilder(DataCube<?> cube) {
        address = Address.create(cube);
    }

    public ReadBuilder(boolean usePartitionByte, List<Dimension<?, ?>> dimensions) {
        address = new Address(usePartitionByte, dimensions);
    }

    public <O> ReadBuilder at(Dimension<?, O> dimension, O coordinate) {
        if (dimension.isBucketed()) {
            throw new IllegalArgumentException("This dimension requires you to specify a bucketType");
        }
        this.at(dimension, BucketType.IDENTITY, coordinate);
        return this;
    }

    public <O> ReadBuilder at(Dimension<?, O> dimension, BucketType bucketType, O coord) {
        Bucketer<?, O> bucketer = dimension.getBucketer();
        byte[] bucket = bucketer.bucketForRead(coord, bucketType).serialize();
        address.at(dimension, bucketType, bucket);
        return this;
    }

    public Address build() {
        return address;
    }
}
