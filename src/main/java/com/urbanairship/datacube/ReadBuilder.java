package com.urbanairship.datacube;

public class ReadBuilder {
    private final ReadAddress address = new ReadAddress();
    boolean built = false;
    
    public final ReadBuilder at(Dimension dimension, byte[] coordinate) {
        address.at(dimension, coordinate);
        return this;
    }

    public final ReadBuilder at(Dimension dimension, BucketType bucketType, byte[] coordinate) {
        address.at(dimension, bucketType, coordinate);
        return this;
    }
    
    public ReadAddress build() {
        if(built) {
            throw new RuntimeException("Already built");
        }
        built = true;
        return address;
    }
}
