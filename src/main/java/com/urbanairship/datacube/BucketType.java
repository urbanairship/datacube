/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

public class BucketType {
    /**
     * These are provided for convenience.
     *
     * The identity bucket type means we write exactly what you supply to the underlying data store. No transformation
     * needs to happen to translate from the value passed in at the dimension
     *
     * It is permissible to specify rollups that omit dimensions. IF you do so, then the value of that dimension is
     * "WILDCARD".
     *
     * For example, if you have a counter for mobile apps and days, you might have a rollup:
     * for {@code (app, day) -> count}. All time count for an app would be {@code ("<app id>", *) -> count}
     */
    public static final BucketType
            IDENTITY = new BucketType("nobucket", new byte[]{}),
            WILDCARD = new BucketType("wildcard", new byte[]{});

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
