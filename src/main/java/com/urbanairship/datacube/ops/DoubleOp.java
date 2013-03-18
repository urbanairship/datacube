/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Cube oplog mutation type for storing a double value.
 */
public class DoubleOp implements Op {
    private final double val;
    public static final DoubleOpDeserializer DESERIALIZER = new DoubleOpDeserializer();

    public DoubleOp(double val) {
        this.val = val;
    }

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof DoubleOp)) {
            throw new RuntimeException();
        }
        return new DoubleOp(val + ((DoubleOp)otherOp).val);
    }

    @Override
    public Op subtract(Op otherOp) {
        return new DoubleOp(this.val - ((DoubleOp)otherOp).val);
    }

    @Override
    public int hashCode() {
        long temp = val != +0.0d ? Double.doubleToLongBits(val) : 0L;
        return (int) (temp ^ (temp >>> 32));
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
        DoubleOp other = (DoubleOp) obj;
        if (val != other.val)
            return false;
        return true;
    }

    @Override
    public byte[] serialize() {
        return Bytes.toBytes(val);
    }

    public static class DoubleOpDeserializer implements Deserializer<DoubleOp> {
        @Override
        public DoubleOp fromBytes(byte[] bytes) {
            return new DoubleOp(Bytes.toDouble(bytes));
        }
    }

    public String toString() {
        return Double.toString(val);
    }

    public Double getDouble() {
        return val;
    }
}
