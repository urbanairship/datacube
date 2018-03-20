/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.Arrays;
import java.util.Objects;

public class BucketType {
    public static final BucketType IDENTITY = new BucketType("nobucket", new byte[]{});
    public static final BucketType WILDCARD = new BucketType("wildcard", new byte[]{});

    private final String nameInErrMsgs;
    private final byte[] uniqueId;

    public BucketType(String nameInErrMsgs, byte[] uniqueId) {
        this.nameInErrMsgs = nameInErrMsgs;
        this.uniqueId = uniqueId;
    }

    public byte[] getUniqueId() {
        return uniqueId;
    }

    public String getNameInErrMsgs() {
        return nameInErrMsgs;
    }

    public String toString() {
        return nameInErrMsgs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BucketType)) return false;
        BucketType that = (BucketType) o;
        return Objects.equals(nameInErrMsgs, that.nameInErrMsgs) &&
                Arrays.equals(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(nameInErrMsgs);
        result = 31 * result + Arrays.hashCode(uniqueId);
        return result;
    }
}
