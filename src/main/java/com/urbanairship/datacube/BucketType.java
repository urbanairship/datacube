package com.urbanairship.datacube;

public class BucketType {
    public static final BucketType IDENTITY = new BucketType("unbucketed_dimension", new byte[] {});
    
    private final String name;
    private final byte[] uniqueId;
    
    public BucketType(String name, byte[] uniqueId) {
        this.name = name;
        this.uniqueId = uniqueId;
    }
    
    public byte[] getUniqueId() {
        return uniqueId;
    }
}
