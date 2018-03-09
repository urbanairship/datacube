/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

/**
 * Use this to specify the location of a cell to read from a datacube.
 */
public class ReadBuilder {
    private final Address address;
    boolean built = false;

    public ReadBuilder(DataCube<?> cube) {
        address = Address.create(cube);
    }

    public <O> ReadBuilder at(Dimension<?> dimension, O coordinate) {
        if (dimension.isBucketed()) {
            throw new IllegalArgumentException("This dimension requires you to specify a bucketType");
        }
        this.at(dimension, BucketType.IDENTITY, coordinate);
        return this;
    }

    public <O> ReadBuilder at(Dimension<?> dimension, BucketType bucketType, O coord) {
        Bucketer<O> bucketer = (Bucketer<O>) dimension.getBucketer();
        byte[] bucket = bucketer.bucketForRead(coord, bucketType).serialize();
        address.at(dimension, bucketType, bucket);
        return this;
    }

    public Address build() {
        if (built) {
            throw new RuntimeException("Already built");
        }
        built = true;
        return address;
    }
}
