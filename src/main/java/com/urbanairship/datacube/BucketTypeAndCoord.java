package com.urbanairship.datacube;

import java.util.Arrays;

class BucketTypeAndCoord {
    public final BucketType bucketType;
    public final byte[] coord;
    
    public BucketTypeAndCoord(BucketType bucketType, byte[] coord) {
        this.bucketType = bucketType;
        this.coord = coord;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(bucketType.toString());
        sb.append(": ");
        sb.append(Arrays.toString(coord));
        sb.append(")");
        return sb.toString();
    }
}