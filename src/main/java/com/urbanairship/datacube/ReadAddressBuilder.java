package com.urbanairship.datacube;

public class ReadAddressBuilder {
    private final ReadAddress address = new ReadAddress();
    boolean built = false;

    public <O> ReadAddressBuilder at(Dimension<?> dimension, O coordinate) {
        address.at(dimension, coordinate);
        return this;
        
    }

    public <O> ReadAddressBuilder at(Dimension<?> dimension, BucketType bucketType, O coordinate) {
        address.at(dimension, bucketType, coordinate);
        return this;
    }
    
//    public final ReadAddressBuilder atWildcard(Dimension dimension) {
//        address.atWildcard(dimension);
//        return this;
//    }
    
    public ReadAddress build() {
        if(built) {
            throw new RuntimeException("Already built");
        }
        built = true;
        return address;
    }
}
