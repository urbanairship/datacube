/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

/**
 * Cube oplog mutation type for storing a long counter.
 */
public class LongOp implements Op<Long> {
    private final long val;
    public static final LongOpDeserializer DESERIALIZER = new LongOpDeserializer();

    public LongOp(long val) {
        this.val = val;
    }

    @Override
    public Op add(Op otherOp) {
        if (!(otherOp instanceof LongOp)) {
            throw new RuntimeException();
        }
        return new LongOp(val + ((LongOp) otherOp).val);
    }

    @Override
    public Op subtract(Op otherOp) {
        return new LongOp(this.val - ((LongOp) otherOp).val);
    }

    /**
     * Eclipse auto-generated
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (val ^ (val >>> 32));
        return result;
    }

    /**
     * Eclipse auto-generated
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LongOp other = (LongOp) obj;
        if (val != other.val)
            return false;
        return true;
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(val);
    }

    @Override
    public Long get() {
        return val;
    }

    public static class LongOpDeserializer implements Deserializer<LongOp> {
        @Override
        public LongOp fromBytes(byte[] bytes) {
            return new LongOp(Util.bytesToLong(bytes));
        }
    }

    public String toString() {
        return Long.toString(val);
    }

    public long getLong() {
        return val;
    }
}
