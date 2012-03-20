package com.urbanairship.datacube;

/**
 * Use this to specify the location of a cell to read from a datacube.
 */
public class ReadAddressBuilder {
    private final Address address;
    boolean built = false;
    
    public ReadAddressBuilder(DataCube<?> cube) {
        address = new Address(cube);
    }

    public <O> ReadAddressBuilder at(Dimension<?> dimension, O coordinate) {
        this.at(dimension, BucketType.IDENTITY, coordinate);
        return this;
    }

    public <O> ReadAddressBuilder at(Dimension<?> dimension, BucketType bucketType, O coord) {
        Bucketer<?> bucketer = dimension.getBucketer();
        byte[] bucket = bucketer.bucketForRead(coord, bucketType).serialize();
        address.at(dimension, bucketType, bucket);
        return this;
    }
    
    public Address build() {
        if(built) {
            throw new RuntimeException("Already built");
        }
        built = true;
        return address;
    }
}
