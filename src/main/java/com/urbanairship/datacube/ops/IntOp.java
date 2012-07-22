/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

/**
 * Cube oplog mutation type for storing a int counter.
 */
public class IntOp implements Op {
    private final int val;
    public static final IntOpDeserializer DESERIALIZER = new IntOpDeserializer(); 
    
    public IntOp(int val) {
        this.val = val;
    }

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof IntOp)) {
            throw new RuntimeException();
        }
        return new IntOp(val + ((IntOp)otherOp).val);
    }
    
    @Override
    public Op subtract(Op otherOp) {
        return new IntOp(this.val - ((IntOp)otherOp).val);
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
        IntOp other = (IntOp) obj;
        if (val != other.val)
            return false;
        return true;
    }

    @Override
    public byte[] serialize() {
        return Util.intToBytes(val);
    }

    public static class IntOpDeserializer implements Deserializer<IntOp> {
        @Override
        public IntOp fromBytes(byte[] bytes) {
            return new IntOp(Util.bytesToInt(bytes));
        }
    }
    
    public String toString() {
        return Integer.toString(val);
    }
    
    public int getInt() {
        return val;
    }
}
