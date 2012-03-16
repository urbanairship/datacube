package com.urbanairship.datacube;

public class ReadAddress extends Address {
    public <O> void at(Dimension<?> dimension, BucketType bucketType, O coord) {
        super.at(dimension, bucketType, 
                dimension.getBucketer().bucketForRead(coord, bucketType).serialize());
    }
    
    public <O> void at(Dimension<?> dimension, O coord) {
        this.at(dimension, BucketType.IDENTITY, coord);
    }
}
