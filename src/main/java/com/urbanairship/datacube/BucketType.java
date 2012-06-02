/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

public class BucketType {
    public static final BucketType IDENTITY = new BucketType("nobucket", new byte[] {});
    public static final BucketType WILDCARD = new BucketType("wildcard", new byte[] {});
    
    private final String nameInErrMsgs;
    private final byte[] uniqueId;
    
    public BucketType(String nameInErrMsgs, byte[] uniqueId) {
        this.nameInErrMsgs = nameInErrMsgs;
        this.uniqueId = uniqueId;
    }
    
    public byte[] getUniqueId() {
        return uniqueId;
    }
    
    public String toString() {
        return nameInErrMsgs;
    }
}
